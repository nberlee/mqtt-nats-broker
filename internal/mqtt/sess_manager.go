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

// The account session manager: its state, construction and stream provisioning, the
// subscription barrier, the phase gate, flapper jailing, the session-record CAS, the orphaned
// consumer sweep, session locking and registration, and create-or-restore of a session.

package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server/stree"
	"github.com/nats-io/nuid"
)

// mqttPhaseGateWaitFor is how long a delivery waits on a reconciliation phase
// gate: the connection's effective AckWait, capped at mqttPhaseGateWaitMax.
// Tying it to AckWait is what keeps the wait from changing the outcome — at
// that deadline JetStream redelivers regardless, and the skipped attempt is
// recorded so the redelivery is still the client's first transmission.
func mqttPhaseGateWaitFor(c *client) time.Duration {
	wait := mqttDefaultAckWait
	if c != nil && c.srv != nil {
		if aw := c.srv.getOpts().MQTT.AckWait; aw > 0 {
			wait = aw
		}
	}
	return min(wait, mqttPhaseGateWaitMax.Get())
}

// How often to re-run the session-expiry sweep for an account.
//
// Atomic rather than a plain var: every account manager's sweep goroutine reads
// this, and those goroutines outlive the test that created them, so a test
// shortening the interval would otherwise race every sweeper still draining from an
// earlier test. Only the ticker construction reads it, so a change takes effect for
// account managers created afterwards.
var mqttSessionExpirySweepIntervalNS = func() *atomic.Int64 {
	v := &atomic.Int64{}
	v.Store(int64(15 * time.Minute))
	return v
}()

// mqttSubscribeClaimAttempts is the SUBSCRIBE source-consumer name claim's re-attempt budget, its
// own (consult 104): the claim runs under the generation and tenure pinned at its first read, so a
// takeover still defeats every attempt, while a same-session burst of authority writes is outlasted.
const mqttSubscribeClaimAttempts = 8

func mqttSessionExpirySweepInterval() time.Duration {
	return time.Duration(mqttSessionExpirySweepIntervalNS.Load())
}

type mqttSessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*mqttAccountSessionManager // key is account name

	// testSessCreateGate / testSessCreateObserve are instance-scoped test seams,
	// nil in production. The gate is invoked after the $MQTT_sess lookup reports the
	// stream absent and BEFORE createStream, so a test can barrier two racing
	// managers at the exact lookup->create boundary; the observer receives the
	// createStream outcome (created, err) so a test can prove exactly one manager
	// won the create and the other saw JSStreamNameExistErr. See
	// mqttCreateAccountSessionManager.
	testSessCreateGate    func()
	testSessCreateObserve func(created bool, err error)

	// testRetainedApplyGate is an instance-scoped test seam, nil in production,
	// invoked by processRetainedMsg with the decoded record's logical subject and
	// whether it is a tombstone, immediately BEFORE the record is applied to the
	// index. Holding one application there lets a test run the other index writer
	// — mqttHandlePubRetain's synchronous local seed — ahead of it, which is the
	// reordering F-059 describes and the window that made the removed compaction
	// delete destructive. See processRetainedMsg.
	testRetainedApplyGate func(subject string, tombstone bool)

	// The retained index's other instance-scoped test seams, nil (zero) in production.
	// See retained_index.go and retained_index_test.go.
	//
	// testRetainedDeliveryDrop runs FIRST in processRetainedMsg, before any bookkeeping,
	// and drops the message when it returns true: a delivery nats.go never handed over.
	// testRetainedSeedGate runs in mqttHandlePubRetain after storeMsg, before the seed
	// applies. testRetainedLoadGate runs in loadRetainedMessages after the stream read,
	// before its results apply. testRetainedObserverHold suppresses the observer loop's
	// work while it returns true. testRetainedResetDeleteAmbiguous makes a reset's whole
	// definite delete come back ambiguous. testRetainedCreateHook runs inside a reset's
	// create, after the server answered and before the reset sees the outcome, and may
	// replace that outcome. testRetainedInitHook runs inside
	// mqttProvisionEphemeralConsumers after the observer's create, with the manager being
	// initialized. testRetainedCommitObserve runs after every observer commit returns: the
	// barrier a test needs to assert that a commit changed NOTHING.
	// testRetainedObserverHeartbeat overrides the observer's idle heartbeat.
	testRetainedDeliveryDrop         func(subject string) bool
	testRetainedSeedGate             func(subject string)
	testRetainedLoadGate             func(subjects []string)
	testRetainedObserverHold         func() bool
	testRetainedResetDeleteAmbiguous func() bool
	testRetainedCreateHook           func(err error) error
	testRetainedInitHook             func(as *mqttAccountSessionManager)
	testRetainedCommitObserve        func(subject string)
	testRetainedObserverHeartbeat    time.Duration
}

type mqttAccountSessionManager struct {
	mu         sync.RWMutex
	sessions   map[string]*mqttSession // key is MQTT client ID
	sessByHash map[string]*mqttSession // key is MQTT client ID hash
	sessLocked map[string]struct{}     // key is MQTT client ID and indicate that a session can not be taken by a new client at this time
	flappers   map[string]time.Time    // When connection connects with client ID already in use
	flapTimer  *time.Timer             // Timer to perform some cleanup of the flappers map
	// Delayed Wills are no longer tracked in memory here: they live on the durable
	// $MQTT_willq outbox and are driven by the claim loop. See will_outbox.go.
	// Pending session-expiry timers, keyed by MQTT client ID hash, that clean up a
	// session once its MQTT 5.0 Session Expiry Interval elapses after the
	// connection closes. A local reconnect cancels them, and so does an observed
	// remote record write — which shows the record moved, not that a peer holds the
	// session (F-084); cancelling is correct either way, because this broker may no
	// longer write that record and so its timer must not fire against it.
	// Spec5 [3.1.2.11.2]. Guarded by mu.
	pendingExpiries map[string]mqttPendingExpiry
	// expiryTokens mints mqttPendingExpiry install tokens under as.mu.
	expiryTokens uint64
	// recoveryShardCursor is where the next outbound-recovery pass starts its shard walk,
	// retained across passes and guarded by mu (F-105, row 6).
	//
	// A per-pass shard budget with a walk that always began at shard 0 would never reach the
	// later shards at all — starvation by construction rather than by load — so the position
	// advances and wraps round the fixed shard count.
	recoveryShardCursor uint64
	// Per-client reconciliation ordering state, keyed by MQTT client ID hash and
	// guarded by mu. A remote session-record write observed by the session-record watch
	// hands the expiry reconciliation to a spawned worker; because each worker
	// loads the record before taking mu, worker completion order can reverse the
	// (ordered) delivery order. latest is the highest observed PubAck sequence for
	// the client, inFlight counts admitted-but-not-yet-completed workers. A worker
	// applies its decision only if it is still the latest observation, and the
	// entry is pruned only once inFlight drops to zero so a worker parked before mu
	// cannot resume into a pruned-away watermark. See reconcileRemoteSessionExpiry.
	reconcileWM map[string]*sessionReconcileState
	// refusedWillRecords is the set of $MQTT_willq subjects whose record has
	// already been reported unsupported, so the 5s claim scan warns once per
	// record rather than every pass. Guarded by mu; bounded by the number of
	// distinct refused subjects, and never cleared because a refused record is
	// permanent until an operator removes it. See refuseWillOutboxRecord.
	refusedWillRecords map[string]struct{}
	// refusedAcceptFacts is the same set for CONNECT acceptance facts this broker
	// cannot interpret. Same reasons and the same bound, and one more of its own:
	// an acceptance fact is never rewritten (its subject is unique) and never
	// deleted (facts are permanent), so nothing but an operator ever clears one.
	// Guarded by mu. See refuseAcceptFact.
	refusedAcceptFacts map[string]struct{}
	// willAheadCursor is where the next early-cancellation phase resumes in this
	// account's not-yet-due pending Wills. The phase is budgeted, so without a cursor
	// a large enough backlog would re-examine the same first records every pass and
	// never reach the rest. Guarded by mu. See cancelSupersededAhead.
	willAheadCursor int
	// testClaimLease overrides the delayed-Will publish lease for THIS account
	// manager only (instance-scoped, 0 = the package default). Set/read under mu.
	// It exists because the package variable is read by every live claim loop on its
	// own goroutine: a test that assigned it would be racing them, not configuring
	// itself. The renewal cadence and the scan budget are derived from whatever this
	// resolves to. Zero in production. See claimLease.
	testClaimLease time.Duration

	// testClaimStop, when non-nil, replaces the stop channels a claimant's fact scan
	// watches, for THIS account manager only (instance-scoped). Set/read under mu. A
	// test that quiets this account's background claim loop by closing the server's
	// willClaimStopCh supplies an EMPTY set here, so the claimant it drives directly
	// still runs. Nil in production. See claimStopChans.
	testClaimStop *[]<-chan struct{}
	// forkedGrants holds, per MQTT client ID, the CANDIDATE grant delta a RECOGNISED fork
	// minted — this broker's acknowledged body against its retained baseline, computed
	// WITHOUT reading the survivor — waiting for that client's own reconnect to filter it
	// against the surviving record at merge and re-assert what survives that comparison
	// through the ordinary accept save. Guarded by mu, bounded by mqttForkedGrantMax, and memory-only: a restart
	// forfeits a repair and breaks nothing. Entries are removed when a reconnect's anchor
	// refuses them, superseded by a later verdict for the same client, or evicted by the
	// bound. See sess_fork_repair.go.
	forkedGrants map[string]*mqttForkedGrant
	// forkedGrantStores counts SUCCESSFUL capsule stores on this manager. Read only by
	// tests, and per-manager rather than package-global so parallel tests cannot observe
	// each other's mints. See storeForkedGrantLocked for why the count is the only
	// observable that can tell one mint from two.
	forkedGrantStores atomic.Int64
	retmsgs           *stree.SubjectTree[mqttRetainedMsgRef] // retained message metadata
	rmsCache          *sync.Map                              // map[subject]mqttRetainedMsg
	// rmObs is the retained observer's ordering and loss-detection state, guarded by mu
	// with the index. See retained_index.go.
	rmObs    mqttRetainedObserverState
	jsa      mqttJSA
	domainTk string // Domain (with trailing "."), or possibly empty. This is added to session subject.

	// sessStreamEpoch is the $MQTT_sess incarnation identity resolved once at
	// account-manager init from the stream's mqtt_sess_epoch metadata (form
	// "nuid:<NUID>"). It stamps every acceptance fact (SessEpoch), every retirement
	// watermark and every native Will record (OriginSessEpoch); a sequence is only
	// comparable across them when the epochs are equal. Immutable after init (read
	// without lock). If a valid epoch cannot be resolved, the account manager is not
	// published. See validateSessStream, sess_accept_fact.go.
	sessStreamEpoch string
	// sharedVerdictSeen is the previous verdict sweep's reading of each session record's
	// release verdicts, keyed by record subject: a verdict is executed by the sweep only once
	// the same (sequence, verdicts) has been observed on two consecutive passes. Guarded by mu.
	sharedVerdictSeen map[string]mqttSharedVerdictObservation

	// testPublishWill, when non-nil, replaces the delayed-Will publication for
	// THIS account manager only (instance-scoped, so parallel tests stay
	// isolated). Set/read under mu. Used to inject publish failures and verify
	// cross-replica recovery. Nil in production. See publishWillRecord.
	testPublishWill func(rec *mqttWillOutbox) error

	// testEnqueueStore, when non-nil, replaces the durable enqueue store for THIS
	// account manager only (instance-scoped). Set/read under mu. Used to force the
	// enqueue CAS to fail so the immediate-publish fail-safe is exercised. Nil in
	// production. See storeWillEnqueueCAS / enqueueDelayedWill.
	testEnqueueStore func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error)

	// testClaimStore, when non-nil, replaces the claim / renewal / licensing CAS for
	// THIS account manager only (instance-scoped). Set/read under mu. It is the seam
	// the renewal and licensing tests need in both directions: to fail a renewal
	// mid-scan, and to COUNT licensing attempts — "the claimant did not publish" is
	// also what a stale final CAS produces, so only the attempt count discriminates.
	// Nil in production. See storeWillClaimCAS.
	testClaimStore func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error)

	// testFinalizeStore, when non-nil, replaces the terminal publishing->done CAS
	// store for THIS account manager only (instance-scoped). Set/read under mu.
	// Used to fail or ambiguate the terminal commit and verify the finalizer's
	// re-read/reconcile path. Nil in production. See finalizeWillPublish.
	testFinalizeStore func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error)

	// testSessionLoad, when non-nil, replaces the point session-record load used by
	// the orphan QoS 2 sweep and the shared-affine record refusal for THIS account
	// manager only (instance-scoped). Set/read under mu. Used to drive a decision on
	// a synthesized record and to inject a load error so the caller defers on an
	// indeterminate read. Nil in production. See loadFenceSession.
	testSessionLoad func(cidHash string) (*mqttPersistedSession, uint64, error)

	// testFactLoad, when non-nil, replaces the acceptance-fact walk's point read for
	// THIS account manager only (instance-scoped). Set/read under mu. It is both a
	// gate and a COUNTER, and the counting is the load-bearing half: a claimant that
	// reused a persisted decision and one that rescanned and found nothing produce
	// the same publication, so the only discriminating assertion is that no fact was
	// loaded at all. Nil in production. See loadNextAcceptFact.
	testFactLoad func(filter string, seq uint64) (*StoredMsg, error)

	// testSessLastSeq, when non-nil, replaces the $MQTT_sess last-sequence sample
	// that becomes a decision's cutoff, for THIS account manager only
	// (instance-scoped). Set/read under mu. Used to stage a claimant scanning under
	// an older cutoff than a peer's. Nil in production. See sessLastSeq.
	testSessLastSeq func() (uint64, error)

	// testBeforeTeardownLock, when non-nil, is invoked by mqttHandleClosedClient for
	// THIS account manager only (instance-scoped) after the closing connection has
	// recorded what its Will is owed, but BEFORE it takes the session gate. Set/read
	// under mu. Lets a test park the teardown there and drive a same-client-ID
	// CONNECT to completion, so "the close was observed first, then the session was
	// rebound" is an ordering rather than a probability. Nil in production.
	testBeforeTeardownLock func(clientID string)

	// testAfterParkedJailDecision, when non-nil, is invoked by mqttDeferDisconnectEx
	// once it has decided what to do about the flappers jail, before it returns.
	//
	// Stored in an atomic rather than under mu like its siblings, and that is
	// load-bearing rather than a style choice: a test uses this seam to hold the
	// closing goroutine still while another goroutine reads the jail, and the whole
	// point of that read is that it must NOT be ordered against the removal. Reading
	// the hook under mu would acquire and release the very lock the removal is
	// supposed to be holding, which is a happens-before edge that hides exactly the
	// race the seam exists to expose.
	testAfterParkedJailDecision atomic.Pointer[func(clientID string)]

	// testBeforeTakeoverDiscard, when non-nil, is invoked by mqttProcessConnect for
	// THIS account manager only (instance-scoped) after a takeover has rebound the
	// session to the new connection and before it decides the displaced
	// connection's Will. Set/read under mu. That gap is the window in which the
	// displaced connection can classify its own Will, so a test needs to hold it
	// open to pin which interval that classification uses. Nil in production.
	testBeforeTakeoverDiscard func(clientID string)

	// testBeforeBoundDisconnectHonor, when non-nil, is invoked by mqttProcessConnect for
	// THIS account manager only (instance-scoped) once the takeover has found that the
	// connection still bound to the session sent a DISCONNECT ending it, and before it
	// opens the record transaction that persists that ending. Set/read under mu. That is
	// the interval in which an off-readLoop writer can arm uncertainty after the CONNECT's
	// entry check. Nil in production.
	testBeforeBoundDisconnectHonor func(clientID string)

	// testBeforeUngatedPark, when non-nil, is invoked by mqttDeferDisconnectEx for
	// THIS account manager only, on the UNGATED park alone, after the non-Will action
	// fields are built and before the park's commit (parkClose). Set/read under mu.
	// That is the interval in which a takeover, a disposal or a resolution can move the
	// session under a close that holds no gate. Nil in production.
	testBeforeUngatedPark func(clientID string)

	// testAfterTeardownOwnerLost, when non-nil, is invoked by mqttHandleClosedClient for
	// THIS account manager only when the teardown finds the session no longer bound to
	// its connection, after mqttPublishOwedWill and immediately before it returns.
	// Set/read under mu. It is how a test proves the displaced teardown has passed its
	// last publisher before a takeover decides the Will. Nil in production.
	testAfterTeardownOwnerLost func(clientID string)

	// testBeforeResumeNaks, when non-nil, is invoked on the resume path for THIS
	// account manager only (instance-scoped) after subscriptions have been restored
	// and before resendPendingRedelivery runs. Set/read under mu. Lets a test hold
	// the resume open until a fresh delivery has certainly landed in the pending
	// map, which is the window in which a NAK would duplicate it. Nil in production.
	testBeforeResumeNaks func(clientID string)

	// testResumeReconcileLookup, when non-nil, replaces the resume reconciliation
	// phase's consumer lookup for THIS account manager only (instance-scoped).
	// Set/read under mu. It is what lets a test fail (or delay) a chosen batch of
	// a REAL CONNECT deterministically — the batching helper's function parameter
	// alone would leave the CONNECT path hardwired. Nil in production. See
	// reconcileResumedConsumers.
	testResumeReconcileLookup func(durables []string) ([]*ConsumerInfo, []bool, error)

	// testResumeReconcilePreCreate, when non-nil, is invoked by the resume
	// reconciliation phase for THIS account manager only (instance-scoped)
	// immediately before it creates a replacement durable, with the deliver
	// subject the flushed subscription was installed on. Returning a non-nil
	// error rejects the create WITHOUT issuing it — the same failure surface as
	// a JetStream rejection. Set/read under mu. It is the seam that pins
	// subscribe-BEFORE-create as an ordering assertion rather than a race
	// against the reaper, and the lever for a forced repair failure. Nil in
	// production.
	testResumeReconcilePreCreate func(sess *mqttSession, deliver string) error

	// testSourcePreCreate, when non-nil, answers a SUBSCRIBE's source CONSUMER.CREATE in place of
	// issuing it (F-105 7c-lc): a non-nil error is the create failing after its name was claimed.
	// Set/read under mu. Nil in production.
	testSourcePreCreate func(sess *mqttSession, durable string) error

	// testReconcilePause, when non-nil, is invoked by a remote-expiry reconciliation
	// worker for THIS account manager only (instance-scoped) AFTER it has loaded the
	// record but just BEFORE it takes mu to apply its decision. Set/read under mu.
	// Lets the worker-ordering tests park a stale worker while a newer worker (or a
	// local reconnect/re-disconnect) races ahead, so the ordering/ownership guards
	// are exercised deterministically. Nil in production. See
	// reconcileRemoteSessionExpiry.
	testReconcilePause func(idHash string)

	// testEvictPauseBeforeLoad / testEvictPauseAfterLoad, when non-nil, are invoked
	// by an eviction worker for THIS account manager only (instance-scoped) around
	// its per-iteration record load: BEFORE the load (after the trigger read) and
	// AFTER the load/decode, both outside every lock, so a handler may block. They
	// are the levers for the replacement-object, stale-save, pair-freshness and
	// coalescing races, and pauseBeforeLoad is where test 14(a)'s Goexit exercises
	// the pre-fence abnormal cleanup. Set/read under mu. Nil in production. See
	// evictDisplacedOnRemotePersist.
	testEvictPauseBeforeLoad func(idHash string)
	testEvictPauseAfterLoad  func(idHash string)

	// testEvictPostFence, when non-nil, is invoked by an eviction worker's decision
	// section for THIS account manager only, with as.mu HELD and the record fence
	// OPEN — the window an abnormal exit must clean up from. A handler may panic or
	// Goexit (the deferred guard releases the fence handoff-free and the deferred
	// unlock releases as.mu, in that LIFO order) but must NOT block: it pins as.mu.
	// Read under the held mu. Nil in production. See evictDecideOnce.
	testEvictPostFence func(idHash string)

	// testEvictCleanupEntry, when non-nil, is invoked by an eviction worker at the
	// ENTRY of its terminal cleanup, outside every lock — before the as.mu section
	// that re-checks the trigger watermark and clears the coalescing flag. It is
	// what makes the cleanup's atomic re-check causally testable: a handler can
	// release a gate entry and deliver a newer observation here, and the same
	// worker must loop instead of stranding the trigger. Set/read under mu. Nil in
	// production. See evictDisplacedOnRemotePersist.
	testEvictCleanupEntry func(idHash string)

	// testEvictComplete, when non-nil, is invoked by an eviction worker AFTER its
	// cleanup on both the normal and abnormal exit paths, outside every lock,
	// carrying whether an eviction was applied. Tests awaiting "no eviction"
	// outcomes assert after this seam, so the negative is the worker's DECISION
	// rather than its absence. Set/read under mu. Nil in production. See
	// evictDisplacedOnRemotePersist.
	testEvictComplete func(idHash string, applied bool)

	// testSessPersistDrop, when non-nil, drops the remote persist observation for the
	// client ID hashes it returns true for, on THIS account manager only. Set/read
	// under mu. That is the state a lost core-NATS notification leaves — the remote
	// store happened and this broker never learned of it — which is the condition the
	// record-rejection settlement exists for and which nothing else can produce
	// deterministically. Hash-keyed so a parallel test and every unrelated session on
	// this manager are unaffected. Nil in production. See processSessionRecordDelivery.
	testSessPersistDrop func(idHash string) bool
	// testSessRecordBackstopOff disables the periodic ownership backstop
	// (reconcileResidentOwnership) for this manager. It is the counterpart of
	// testSessPersistDrop, and the two exist so a test can isolate ONE convergence
	// channel: with both live, a test that means to prove the watch converged a
	// session would also pass if only the backstop did, and vice versa. Read under
	// mu; false in production.
	testSessRecordBackstopOff bool

	// testQoS2HoldStore / testQoS2FinalizeStore / testQoS2Load, when non-nil,
	// replace the inbound QoS 2 move protocol's create-only hold CAS, its terminal
	// held->delivered CAS, and its classified record load, for THIS account manager
	// only (instance-scoped). Set/read under mu. Each is routed separately so a test
	// can fail exactly one step of the move — the reconcile loops only make sense
	// when the OTHER steps still reach real JetStream. Nil in production. See
	// qos2_move.go.
	testQoS2HoldStore     func(subject string, expectSeq uint64, hdrLen int, msg []byte) (uint64, error)
	testQoS2FinalizeStore func(subject, gen string, expectSeq uint64) (uint64, error)
	testQoS2Load          func(subject string) (*mqttQoS2Record, uint64, error)

	// testQoS2FinalizeDone, when non-nil, is invoked with the RESOLVED status of every
	// terminal commit on THIS account manager (instance-scoped). Set/read under mu.
	// It is the only way to assert what a finalize decided when its client cannot be
	// asked: a cross-broker takeover closes the connection whatever the finalize
	// concluded, so the wire carries no evidence, and a stalled finalize is
	// indistinguishable from a displaced one to a reader. Nil in production. See
	// finalizeQoS2Move.
	testQoS2FinalizeDone func(subject, gen string, status mqttQoS2FinalizeStatus)

	// testQoS2PreFinalize, when non-nil, is invoked immediately BEFORE the terminal
	// held->delivered CAS, with the generation and sequence the PUBREL loaded. This
	// is the only seam that can reach into the load..commit window, which is where
	// every interesting race lives: a test uses it to purge the held record (driving
	// Gone) or to replace it with a NEW generation (driving Superseded). The
	// pre-existing deleteMsg hook cannot do either — after the reorder deleteMsg is
	// terminal cleanup that runs AFTER the commit. A returned error aborts the
	// finalize as indeterminate. Nil in production. See finalizeQoS2Move.
	testQoS2PreFinalize func(subject, gen string, seq uint64) error

	// Seams for the inbound QoS 2 RELEASE CLAIM (qos2_release_claim.go),
	// instance-scoped and set/read under mu. Routed separately from the move
	// protocol's seams because the claim is a different register on a different
	// subject: a test must be able to fail or observe the claim while the hold
	// store, the terminal commit and the record loads all still reach real
	// JetStream. Nil in production.
	testQoS2ClaimStore func(subject string, expectSeq uint64, c *mqttQoS2RelClaim) (uint64, error)
	testQoS2ClaimLoad  func(subject string) (*mqttQoS2RelClaim, *StoredMsg, error)

	// testQoS2Subjects, when non-nil, replaces the discard walk's subject
	// enumeration. Routed separately from the claim loads for the reason
	// testInflightEnumerate is: "the identifier space could not be read" and "one
	// claim could not be loaded" are different failures with the same remedy. It is
	// also the only way to stage the ONE response shape the walk must refuse — a
	// non-empty subject map with a zero last sequence, which the server can legitimately
	// return once because State and State.Subjects are separate reads, and which a
	// bounded purge cannot act on (F-062). Nil in production.
	testQoS2Subjects func(filter string) (map[string]uint64, uint64, error)

	// testQoS2ClaimDone, when non-nil, is invoked with the RESOLVED status of every
	// release claim on THIS account manager. Same reason as testQoS2FinalizeDone: a
	// releaser that correctly declines to deliver sends nothing at all, and on a
	// takeover its connection is already closed, so the wire carries no evidence of
	// what it decided. Nil in production. See claimQoS2Release.
	testQoS2ClaimDone func(holdSubject, gen string, status mqttQoS2ClaimStatus)

	// testQoS2PreClaim, when non-nil, is invoked immediately BEFORE the first
	// create-only claim CAS, with the generation and hold sequence the PUBREL loaded.
	// It is the only seam inside the load..claim window, which is where the race this
	// protocol arbitrates lives: a test uses it to discard the exchange from another
	// broker (driving Discarded), or to let a peer complete and reap the whole
	// exchange (driving the stalled-loader case that the post-claim re-read catches).
	// A returned error abandons the claim as indeterminate. Nil in production.
	testQoS2PreClaim func(holdSubject, gen string, holdSeq uint64) error

	// Seams for the shared-subscription QoS 2 session-affine move
	// (qos2_shared_affine.go), instance-scoped and set/read under mu. Each step is
	// routed separately because every interesting failure here is "this ONE
	// operation did not commit while the others did": a claim CAS that is rejected
	// or lost, a copy store that succeeds while its response is lost, or a load
	// that is indeterminate rather than absent. Nil in production.
	testQ2AffClaimStore func(subject string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error)
	testQ2AffClaimLoad  func(subject string) (*mqttQ2AffClaim, uint64, error)
	testQ2AffCopyStore  func(subject string, hdrLen int, msg []byte) (uint64, error)
	testQ2AffCopyLoad   func(subject string) (*StoredMsg, error)

	// testQ2AffMoveDone, when non-nil, reports the resolved disposition of every
	// shared QoS 2 move attempt on this account manager. Like testQoS2FinalizeDone,
	// it exists because the decision is often unobservable on the wire: a contender
	// that correctly declines to deliver sends nothing at all, and "declined" and
	// "still running" look identical to a reader. Nil in production.
	testQ2AffMoveDone func(claimSubject string, res mqttQ2AffMoveResult)

	// testQ2AffCopyAck, when non-nil, replaces the CONFIRMED acknowledgement of an
	// affine copy on the retired-source recovery route. That route's correctness is an
	// ORDER — record AffineDone, then confirm the ack, then forget the retirement — and
	// the order is only observable if a test can make each step succeed or fail
	// independently. A real confirmed ack needs a live JetStream delivery, which cannot
	// be staged around a seeded claim. Nil in production.
	testQ2AffCopyAck func(reply string) error

	// testSharedGroupLookupFail, when non-nil, is consulted for each group durable the
	// resume's batched verification classifies, and a non-nil return replaces that
	// durable's outcome with an INDETERMINATE one.
	//
	// It exists because absence and indeterminacy are the two outcomes this verification
	// must not confuse, and only one of them can be staged by deleting a consumer. Without
	// a seam for the other, the fixture for "the lookup established nothing" would have to
	// pre-seed a verdict — which tests the fixture rather than the classification, and
	// leaves the mutation that collapses the two classifications alive. Nil in production.
	testSharedGroupLookupFail func(durable string) error

	// testSharedGroupBetweenPasses, when non-nil, runs once after the resume's PRE-pass
	// classification and before its post-pass, which is the only window in which a test
	// can change what the second reading will see.
	//
	// That window is the post-pass's entire reason for existing — a durable can be
	// deleted, altered, or have a legitimately variable field moved by another member
	// between the two readings — and it is unreachable from outside without a seam.
	// Nil in production.
	testSharedGroupBetweenPasses func()

	// testSharedGroupLookupBatch, when non-nil, is called with the size of each batched
	// consumer lookup the resume verification issues.
	//
	// The bound it observes is not a tuning choice: a session's shared filter count is
	// client-controlled, one batch shares one timeout, and an indeterminate verdict
	// refuses — so an unbounded batch lets a large session fail closed on every reconnect.
	// Only a seam can see the size actually requested. Nil in production.
	testSharedGroupLookupBatch func(n int)

	// testSharedGroupLookupErr, when non-nil and returning non-nil, makes ONE batched
	// consumer lookup fail as a transport error would, so a test can prove the
	// verification stops at the first failure rather than paying a timeout per remaining
	// chunk. The counter argument is its own: the call index is passed in. Nil in
	// production.
	testSharedGroupLookupErr func(call int) error

	// testQ2AffPreMaterialize, when non-nil, runs immediately BEFORE the
	// claimed->materialized CAS, after the copy has been stored. It is the only
	// seam inside the window where the copy is visible to a live affine consumer
	// but the claim does not yet authorize delivery — the window the delivery gate
	// exists for. A returned error abandons the transition. Nil in production.
	testQ2AffPreMaterialize func(claimSubject string, seq uint64) error

	// testQ2AffConsumerLookup, when non-nil, replaces EVERY affine consumer
	// lookup: the contender's, before it restores a durable on behalf of an absent
	// winner, and the owner's, inside the recovery that follows a create already
	// refused for a storage-type conflict. It exists to prove a negative — the
	// owner's PROVISIONING path performs no lookup at all, so a lookup forced to
	// fail must leave every owner-side shape working exactly as it does today
	// (F-064). A lookup added outside lookupQ2AffineConsumer is invisible here,
	// which is why that helper is the single route. Nil in production.
	testQ2AffConsumerLookup func(durable string) (*ConsumerInfo, error)

	// testQ2AffConsumerCreate, when non-nil, replaces the create-or-update a
	// CONTENDER issues to restore an absent winner's affine durable. It carries the
	// request so a test can assert WHAT was sent — which storage type was adopted,
	// and, by counting, that a refusal happened with no create attempted at all.
	// That last one is not observable any other way: most of D1's refusals produce
	// the same final state as a create that was issued and then rejected. Nil in
	// production.
	testQ2AffConsumerCreate func(cfg *ConsumerConfig) (*JSApiConsumerCreateResponse, error)

	// testQ2AffOutStreamInfo, when non-nil, replaces the $MQTT_out lookup a
	// CONTENDER makes to read the live consumer limits before restoring a durable.
	// An unreadable stream must be indeterminate rather than "no limits", and there
	// is no non-destructive way to stage that against a real stream. Nil in
	// production.
	testQ2AffOutStreamInfo func() (*StreamInfo, error)

	// testQ2AffSourceProbe, when non-nil, is called at the top of q2AffSourceGone and
	// changes NOTHING about its answer. A pure observer, and the only way to prove the
	// ORDER of the sourceless rule's two reads: that rule must read the source first and
	// the copy LAST, so the reading its CAS consumes is as fresh as possible, and no
	// existing seam can witness the source read at all. Nil in production, where the
	// cost is one nil check.
	testQ2AffSourceProbe func(*mqttQ2AffClaim)

	// Seams for the outbound in-flight register (inflight_register.go),
	// instance-scoped and set/read under mu. Routed only through the register's own
	// store and load so a test can fail, stall or observe the write that must
	// PRECEDE a send while the source delivery, the PUBREL store and every unrelated
	// load still reach real JetStream. A store hook that reports success models
	// "landed, response lost", which is the ambiguous outcome the nonce re-read
	// resolves. Nil in production.
	testInflightStore func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error)
	testInflightLoad  func(subject string) (*mqttInflightRecord, *StoredMsg, error)
	// testRecoverCancellation observes recovery's cancellation classification (F-088 WP3 edit 4):
	// it fires for every decided, unretired register that carries a cancellation (its own
	// Cancelled bit, or a verdict naming its nonce) BEFORE the genuine-cancellation arm tests the
	// marker, with the marker evidence the walk established (markerSeq 0: authoritatively absent).
	// markerSeq is marker PRESENCE, kept as deletion debt even on a possibly-attempted register
	// (a recovered output token beside a standing marker, F-088): nonzero does not by itself mean
	// proven-unsent. Observation only. Nil in production.
	testRecoverCancellation func(pi uint16, nonce string, recCancelled, verdict bool, markerSeq uint64)
	// testRecoverDropCondemn observes recovery's RecoveryDrop over a consumed reservation (F-088
	// W6a): it fires at the apply loop's condemnSendLocked(..., mqttSendSettleRecoveryDrop) with
	// the entry's send nonce as it stood and whether the call PARKED the drop on an open section
	// (true) rather than leaving the entry to be deleted. Session lock held when it fires.
	// Observation only. Nil in production.
	testRecoverDropCondemn func(pi uint16, sendNonce string, parked bool)
	// testResumeAuthorityLoad replaces the resume's durable authority read (F-105 7b,
	// resumeEntitlementServes), for rigs that drive the whole store through these hooks.
	testResumeAuthorityLoad func(id string) mqttAuthorityLoad
	// testAckAdvance wraps the atomic acknowledgement advance (advanceExchangeAck), the write
	// that REPLACES the slot-only CAS for an ordinary obligation, so the store seam above no
	// longer sees it. It runs between the pre-batch read and the commit: a hook can refuse the
	// batch, report a landed batch as failed ("response lost"), or land a racing write before
	// calling write(). Keyed on req.To by the caller. Nil in production.
	testAckAdvance func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error)
	// testDrainComplete wraps the PUBREL holder's atomic drain completion
	// (completeExchangeDrain) exactly as testAckAdvance wraps the acknowledgement advance.
	// Nil in production.
	testDrainComplete func(req *mqttCompleteExchangeDrainRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error)
	// testWithdrawUnsent wraps the withdraw-unsent write (withdrawUnsent) exactly as
	// testAckAdvance wraps the acknowledgement advance: a hook can refuse it, report a landed
	// batch as lost, or land a racing write before calling write(). Nil in production.
	testWithdrawUnsent func(req *mqttWithdrawUnsentRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error)
	// testAbandonDelivery wraps the abandon-delivery write (abandonDeniedDelivery) exactly as
	// testWithdrawUnsent wraps the withdrawal. Nil in production.
	testAbandonDelivery func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error)
	// testCustodyResolve may override a custody attempt's settlement at one step: after the
	// first resolution (step "resolve", replacing its verdict), or in place of the abort and its
	// re-resolution (step "abort", returning the verdict directly); ok false leaves the step
	// alone. testCustodyDecodeFault reports a custody read of the named exchange subject as
	// undecodable. testCustodyCensus fails the custody census when it returns an error. Nil in
	// production.
	testCustodyResolve     func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool)
	testCustodyDecodeFault func(subject string) bool
	testCustodyCensus      func() error
	// testAdmitWrite wraps each attempt's authority write inside admitInflightPublish, so a
	// test can pause an attempt, fail it after it composed, or report a landed batch as lost.
	// Nil in production.
	testAdmitWrite func(sess *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error)
	// testAdmitReserved runs inside admitInflightPublish right after its custody reservation
	// opened, before anything is submitted. Nil in production.
	testAdmitReserved func(sess *mqttSession, nonce string)
	// testWithdrawAfterEvidence runs inside opWithdrawUnsent after its evidence reads and before
	// its commit, when the gated door withdraws: the read-to-commit window. Nil in production.
	testWithdrawAfterEvidence func()
	// testPubCompAck observes every JetStream acknowledgement of a canonical PUBREL that the
	// PUBCOMP path sends — inline (byWorker false) or carried by the completion worker (true) —
	// so a test can prove an ack was WITHHELD, not merely that a record disappeared (finalize
	// deletes records directly). Nil in production.
	testPubCompAck func(subject string, byWorker bool)
	// testPubCompEnrolled observes a completion-worker enrolment (a new record or a merge), so a
	// test can distinguish an inline completion from one handed off. Nil in production.
	testPubCompEnrolled func(pi uint16, created bool)
	// testPubCompDetached runs immediately after the completion worker's detach critical
	// section unlocks: the one point where the correct detach (snapshot and removal together)
	// and a split one (snapshot, unlock, then removal) differ. Nil in production.
	testPubCompDetached func(pi uint16)
	// testPubCompAttempt observes each completion-worker attempt and whether its record is still
	// the map's (member), so a test can prove a detached worker keeps its cleanup retries after
	// losing map membership. Nil in production.
	testPubCompAttempt func(rec *mqttPubCompOwed, member bool)
	// testOwnershipMetadataEntry observes an accepting CONNECT's executor ENTERING its fenced
	// metadata commit, so a test can prove the commit was never attempted for a proposal that is
	// not this acceptance's. Nil in production.
	testOwnershipMetadataEntry func()
	// testAbandonOwn intercepts the owed-abandonment worker (F-105 8a) before it acts: it
	// returns false to suppress the attempt, so a test can prove the next acceptance settles a
	// proposal the worker never reached. Nil in production.
	testAbandonOwn func(id string, target mqttOwnershipProposal) bool
	// testOwnershipStepEntry observes an accepting CONNECT's executor ENTERING a step, and may
	// fail it: an error returned here ends the step exactly as a process stopping there would —
	// nothing of the step lands and no in-place cleanup runs. Nil in production.
	testOwnershipStepEntry func(action mqttConnectOwnershipAction) error
	// testOwnershipBeforeMetadata runs after a reservation or preparation LANDED and before its
	// metadata half is committed; an error returned here ends the step as a process stopping
	// there would, leaving the proposal standing with an empty receipt slot. Nil in production.
	testOwnershipBeforeMetadata func(action mqttConnectOwnershipAction) error
	// testOwnershipBudgetTotal, when positive, replaces mqttConnectOwnershipBudget for the
	// CONNECTs of this account, so a test reaches an expiry in milliseconds. Zero in production.
	testOwnershipBudgetTotal time.Duration
	// testRestoreEntry runs as a CONNECT enters its authoritative restore (after the settlement
	// at the top of restoreSession). Nil in production.
	testRestoreEntry func(id string)
	// testBoundarySettle observes every discard boundary's settlement run (prepareClear), with
	// the number of steps it took. Nil in production.
	testBoundarySettle func(id string, steps int)
	// testCertified observes every connection this account's CONNECT path certifies closed on a
	// refusal, with the certificate. Nil in production.
	testCertified func(c *client, cert *mqttCloseCert)
	// testCertifiedClearSubmit observes every certified token clearance (F-105 8c) after its
	// licence is proven and before its write is submitted, with the sequence the write expects.
	// Nil in production.
	testCertifiedClearSubmit func(subject string, seq uint64, pre *mqttInflightRecord)
	// testResumeWalkEntry runs as a resume's register walk is about to start, after the
	// certified-clearance pre-pass (F-105 8c). Nil in production.
	testResumeWalkEntry func(id string)
	// testAuthorityContentionWait replaces the authority-contention wait (the successful-PUBREC
	// advance's and the SUBSCRIBE name claim's backoff sleep), receiving
	// the wait it would have slept, so a test observes and coordinates the spacing without
	// real time. Nil in production.
	testAuthorityContentionWait func(d time.Duration)
	// testInflightPubRelScan replaces the pre-resume PUBREL census. It exists to make a
	// partial/failed scan deterministic: that failure must reserve the identifier space,
	// and no record fixture can manufacture a failed enumeration through stored data.
	testInflightPubRelScan func(subject string) ([]mqttStoredPubRel, int, bool)
	// testInflightSourceState replaces both the singleton and batched source
	// classification. It lets recovery tests stage a source changing from live to complete
	// between its first snapshot and the retirement refresh without timing a JetStream
	// delete between two internal round trips.
	testInflightSourceState func(*mqttInflightRecord) mqttInflightSourceState

	// testInflightEnumerate, when non-nil, replaces the resume barrier's subject
	// enumeration. Routed separately from the loads because "the identifier space could
	// not be read at all" and "one register could not be loaded" are different failures
	// with the same remedy, and a test needs to reach each on its own. Nil in production.
	testInflightEnumerate func(filter string) (map[string]uint64, error)
	// testInflightLoadMulti, when non-nil, replaces the BATCHED register read used by
	// both resume and teardown. Separate from testInflightLoad because teardown's window
	// is the one BETWEEN the enumeration and the load — a register written after the
	// watermark was taken — and a fixture that served both from one seam could not stage
	// it. Resume has no destructive watermark but shares the exact batched transport.
	testInflightLoadMulti func(subjects []string) ([]*JSApiMsgGetResponse, error)
	// testInflightEnumerateBounded, when non-nil, replaces the teardown reap's
	// enumeration. Distinct from testInflightEnumerate because the reap needs the
	// stream WATERMARK the resume walk deliberately discards: the resume walk removes
	// nothing by filter, so it has no purge to bound, while the reap's entitlement IS
	// that watermark.
	testInflightEnumerateBounded func(filter string) (map[string]uint64, uint64, error)

	// testDrainBeforeRegisterRelease, when non-nil, runs in finalizeDrainDelivery between
	// the entry capture of (drain, seq) and the critical section that proves the drain is
	// still current and captures the incarnation identity. It exists for one hazard: a
	// concurrent clear() in exactly that window re-mints the generation and resets the
	// packet-identifier allocator, so a stale finalizer could address a FRESH incarnation's
	// register. No other seam sits in that window, and a fixture that cannot stage it
	// cannot distinguish the guard from its absence. Nil in production.
	testDrainBeforeRegisterRelease func()

	// testInflightSourceAck, when non-nil, replaces the CONFIRMED source acknowledgement
	// on the QoS 2 PUBREC path — the one that licenses deleting the in-flight register.
	// It exists because that failure is unobservable from outside: a confirmation that
	// does not land looks exactly like one that does, and what the broker must then do
	// (keep the register, and keep the source correlated so its redelivery is
	// suppressed) is visible only in what does NOT appear on the wire. Nil in
	// production.
	testInflightSourceAck func(ackSubject string) error

	// testSharedExchangeLoad, when non-nil, answers the exchange read of the recovery walk's
	// unsupported-shared-shape check (sharedRecordUnsupported), for a storeless fixture.
	testSharedExchangeLoad func(gen, lin string) (*mqttOutboundExchange, error)
	// testInflightDelete, when non-nil, replaces the exact-sequence register delete.
	// It exists because R5's failure mode is unobservable on the wire: a delete that
	// never lands leaves a stale record that only a later scan reveals, so a test
	// that wants to reach the "completed exchange, register survived" state has to
	// suppress the delete rather than race it. Nil in production.
	testInflightDelete func(seq uint64) error

	// testInflightStorePair, when non-nil, replaces the atomic PAIR store (register plus
	// unsent marker). A hook that reports success models "landed, response lost", which
	// is the ambiguous outcome the nonce re-read resolves — for the pair as for the single
	// register (testInflightStore). Nil in production.
	testInflightStorePair func(subject string, regExpect, markerExpect uint64, r *mqttInflightRecord) (uint64, error)
	// testInflightMarkerLoad, when non-nil, replaces the authoritative read of an unsent
	// marker's subject. It exists so a fixture can stage the three read outcomes the
	// marker boundary keeps apart — absent, present, indeterminate — on a chosen subject
	// while every other load reaches real JetStream. Nil in production.
	testInflightMarkerLoad func(subject string) (*StoredMsg, error)
	// testMarkerGhosts is the marker a hooked register store (testInflightStore) claims
	// to have landed beside each register subject, by nonce. See storeInflightPair.
	testMarkerGhosts map[string]mqttTestMarkerGhost

	// testRestoreFilterFail fails exactly ONE named exit of the CONNECT restore, for
	// exactly one sid. Per-exit rather than a blanket failure because a fixture has
	// to prove WHICH route it took: F-050 has five reachable exits and a remedy
	// demonstrated on one says nothing about the others. Consulted only when
	// fromSubProto is false, and only AFTER the real call has run, so the fixture
	// reaches the production path and then fails it rather than replacing it.
	testRestoreFilterFail func(exit, sid string) error

	// testSubFilterFail is testRestoreFilterFail for the LIVE SUBSCRIBE path, consulted
	// only when fromSubProto is true. Separate rather than one field consulted on both,
	// so a fixture that means to fail a live filter cannot silently also fail the
	// CONNECT restore that preceded it in the same test.
	//
	// One sid's exit is reached by EVERY occurrence of its filter in the packet — the
	// staging occurrence passes through it before the reviving one does — so a fixture
	// that fails on sid alone cancels the first downgrade before any revival exists. A
	// test that means to fail the revival counts its own calls.
	testSubFilterFail func(exit, sid string) error

	// qos2SweepCursor is the orphan QoS 2 sweep's position in $MQTT_qos2in, and
	// qos2SweepSeeded whether it has been seeded yet. Scheduling state only: every
	// candidate it discovers is authoritatively revalidated before any mutation, so
	// losing, wrapping or reseeding the cursor changes only which records a given
	// pass looks at. Guarded by mu. See orphan_qos2_sweep.go.
	qos2SweepCursor uint64
	qos2SweepSeeded bool
	// retirementMemo is the orphan sweep's POSITIVE-ONLY memo of proven retirements
	// (F-036 unit 2): client-ID hash -> the one generation a valid watermark was
	// observed naming. It holds one generation per client because the durable
	// watermark does; remembering more would claim a history the single slot never
	// had. Only the monotone matching-generation observation is admitted — losing it
	// costs a cleanup opportunity, never a licence — while absent,
	// different-generation, corrupt and indeterminate are never remembered, because
	// each can be invalidated by a retirement landing afterwards. Guarded by mu. See
	// retirementProvenFor.
	retirementMemo map[string]string
	// inflightSweepCursor / inflightSweepSeeded are the same scheduling state for the
	// $MQTT_inflight walk. Separate from the inbound pair because the two streams have
	// independent sequence spaces and independent budgets; sharing one cursor would
	// make each pass's coverage of one stream depend on the other's size. Guarded by
	// mu. See orphan_qos2_sweep.go.
	inflightSweepCursor uint64
	inflightSweepSeeded bool

	// testQoS2SweepSeed, when non-nil, chooses the sweep cursor's initial position
	// instead of the pseudo-random seed, so a test can make one pass deterministic.
	// Instance-scoped, set/read under mu, nil in production.
	testQoS2SweepSeed func(firstSeq, lastSeq uint64) uint64

	// Instance-scoped overrides for the orphan sweep's age policies and discovery
	// budget. Pointers, because zero is a value a test legitimately wants to set.
	// Set/read under mu, nil in production — package-level knobs cannot be used here
	// because the account manager's sweep goroutine reads them concurrently with any
	// test that would swap them.
	testOrphanGrace          *time.Duration
	testQoS2ReleaseGrace     *time.Duration
	testQoS2SweepRecordLimit *int
	testQoS2SweepByteLimit   *int64
	testPubRelAllowance      *int

	// sweepQuarantine records how long each orphan-sweep candidate has been
	// continuously observed as an orphan, on this process's monotonic clock. Created
	// on first use; see orphan_qos2_sweep.go for why no JetStream timestamp can serve
	// that purpose.
	sweepQuarantine *mqttSweepQuarantine

	// recoveryKick is the prompt drive of discarded generations' recovery roots (F-105 7d): a
	// bounded caller of the recovery pass's own visit, so a teardown's consumers are retired
	// promptly rather than at the next pass. See outbound_recovery_kick.go.
	recoveryKick mqttRecoveryKick
}

// sessionReconcileState orders remote-persist workers for a single client ID
// hash: the no-local expiry reconciliation worker and the resident-session
// eviction worker share the watermark, so a newer observation supersedes an
// older in-flight worker of either kind. Guarded by mqttAccountSessionManager.mu.
type sessionReconcileState struct {
	latest   uint64 // highest observed PubAck sequence for this client
	inFlight uint64 // admitted-but-not-yet-completed workers (both kinds)
	// evictLatest is the eviction worker's TRIGGER: the highest observation
	// delivered while a session was RESIDENT. It is deliberately not `latest` —
	// an observation that arrives with no resident session belongs to the
	// no-local reconcile branch exclusively (which in the remove→re-add window
	// deliberately does nothing), and a lingering eviction worker replaying it
	// against a subsequently re-added object would evict on evidence that was
	// never an eviction trigger (holistic round 3). Guarded by as.mu.
	evictLatest uint64
	// evictRunning coalesces resident-branch observations onto ONE eviction worker
	// per client: while set, an observation only advances evictLatest, and the
	// running worker's terminal cleanup re-checks it under as.mu before clearing
	// the flag, looping instead of exiting when a newer trigger arrived. Checked
	// on the resident branch only, so an observation with no resident session
	// always reaches the no-local reconcile regardless of the flag.
	evictRunning bool
	// evictDecided is the highest trigger a decision actually CONSUMED, and it is
	// what makes a refused eviction recoverable instead of lost. A refusal — a held
	// ownership gate, a busy record fence, a digest deferral, an exhausted retry
	// budget — ends the worker without deciding anything, and clearing the trigger
	// there is what used to discard the observation entirely (docs/CONFORMANCE.md
	// stated the eviction as losable with zero notifications). Only a real decision
	// advances this, so the record-state backstop can re-fire any entry left with
	// evictLatest > evictDecided. Guarded by as.mu.
	evictDecided uint64
}

// pruneReconcileEntryLocked removes a client's reconciliation entry, and it is the
// ONLY thing that may: the entry now outlives its workers whenever a refusal left a
// trigger undecided, so "no worker is running" stopped being a licence to delete it.
//
// Three conjuncts, each answering a way the delete goes wrong:
//
//   - the map must still hold THIS st. A cleanup arriving late holds a pointer that a
//     replacement may already have superseded, and deleting by hash alone would take
//     the replacement's trigger with it — the same identity argument
//     removeSessionIfCurrent exists for.
//   - no session may be resident for the hash. A resident session is exactly who an
//     undecided trigger is about, so pruning under one discards the eviction the
//     backstop was going to re-fire.
//   - inFlight must be zero. A live worker of either kind holds this pointer and
//     will write through it.
//
// as.mu must be held. Shutdown is the one exception and does not come through here:
// it nils the whole map, because nothing may install new state once the process is
// stopping.
func (as *mqttAccountSessionManager) pruneReconcileEntryLocked(cIDHash string, st *sessionReconcileState) {
	if as.reconcileWM == nil || st == nil {
		return
	}
	if cur := as.reconcileWM[cIDHash]; cur != st {
		return
	}
	if _, resident := as.sessByHash[cIDHash]; resident {
		return
	}
	if st.inFlight != 0 {
		return
	}
	delete(as.reconcileWM, cIDHash)
}

// mqttSubsBarrier prevents a PUBLISH from racing a concurrent SUBSCRIBE into
// the JetStream messages stream. $MQTT_msgs uses interest-based retention: a
// message stored while a subscription's JetStream consumer create is still in
// flight finds no interest and is discarded outright by the stream (the
// sequence is burned, nothing is stored), leaving it permanently invisible to
// the new subscription. SUBSCRIBE processing holds an entry in the barrier for
// its duration; a QoS 1/2 store calls wait() first, so any SUBSCRIBE the server
// began processing before the publish has its consumer — and therefore stream
// interest — registered by the time the message is stored. MQTT does not
// require delivery to a subscription whose SUBSCRIBE is still in flight, but
// brokers that register subscriptions synchronously do deliver in this
// window, and the Eclipse Paho interop suite relies on it
// (test_request_response, test_subscribe_options). The zero value is ready to
// use.
//
// Each entrant is its own entry with its own completion channel, and a waiter
// waits only for the entries live when it sampled. The barrier used to be one
// count and one channel closed when the count reached zero, so every later
// entrant EXTENDED a generation an earlier waiter had already sampled, and
// continuously overlapping SUBSCRIBEs could hold a QoS 1/2 store — and a Will
// phase with its gate — indefinitely. A wait is now bounded by the slowest
// SUBSCRIBE in flight at its sample, which is a function of that SUBSCRIBE's
// own work, never of traffic that arrived after the publish.
type mqttSubsBarrier struct {
	mu   sync.Mutex
	live map[*mqttSubsBarrierEntry]struct{}
}

// mqttSubsBarrierEntry is one SUBSCRIBE's (or CONNECT restore's) hold on the
// barrier. done closes when that processing leaves.
type mqttSubsBarrierEntry struct {
	done chan struct{}
}

func (b *mqttSubsBarrier) enter() *mqttSubsBarrierEntry {
	e := &mqttSubsBarrierEntry{done: make(chan struct{})}
	b.mu.Lock()
	if b.live == nil {
		b.live = make(map[*mqttSubsBarrierEntry]struct{})
	}
	b.live[e] = struct{}{}
	b.mu.Unlock()
	return e
}

// leave releases e. Leaving one entry twice is a programming error and panics
// on the second close, rather than silently releasing another entrant the way
// an unbalanced decrement of a shared count would.
func (b *mqttSubsBarrier) leave(e *mqttSubsBarrierEntry) {
	b.mu.Lock()
	delete(b.live, e)
	close(e.done)
	b.mu.Unlock()
}

// wait blocks until every SUBSCRIBE in flight when wait sampled the barrier
// has finished processing. SUBSCRIBEs entering afterwards are not waited for:
// the publish preceded them, so it owes them nothing, and waiting for them is
// what let overlapping SUBSCRIBEs hold a store forever.
//
// abort (when non-nil) unblocks the wait early on shutdown: a delayed-Will
// publisher worker passes willClaimStopCh (closed in shutdown Phase 2) so it
// cannot stay blocked here past Phase 3's wait. Skipping the barrier only risks a
// racing SUBSCRIBE missing this message, and that subscriber is being drained
// anyway. Normal operation passes an open channel, so behavior is unchanged.
//
// connAbort (when non-nil) is a client connection's abort latch (inbound_pipeline.go), fired
// when its transport closes. It is a different answer, and wait reports it: a publication whose
// connection is gone must not be stored after the wait (nothing will acknowledge it, and the
// client re-sends on a resumed session), whereas a Will skipping the barrier still stores.
// Shutdown Phase 1 closes every socket, so a readLoop or pipeline issuance blocked here is
// released before Phase 1's clientWG wait, which precedes the Phase 2 abort above.
func (b *mqttSubsBarrier) wait(abort, connAbort <-chan struct{}) (connAborted bool) {
	b.mu.Lock()
	sampled := make([]chan struct{}, 0, len(b.live))
	for e := range b.live {
		sampled = append(sampled, e.done)
	}
	b.mu.Unlock()
	if h := mqttTestSubsBarrierSampledHookV.Load(); h != nil {
		(*h)(b, len(sampled))
	}
	for _, done := range sampled {
		select {
		case <-done:
		case <-abort:
			return false
		case <-connAbort:
			return true
		}
	}
	return false
}

// Returns the MQTT sessions manager for a given account.
// If new, creates the required JetStream streams/consumers
// for handling of sessions and messages.
func (s *Server) getOrCreateMQTTAccountSessionManager(c *client) (*mqttAccountSessionManager, error) {
	sm := &s.mqtt.sessmgr

	c.mu.Lock()
	acc := c.acc
	c.mu.Unlock()
	accName := acc.GetName()

	sm.mu.RLock()
	asm, ok := sm.sessions[accName]
	sm.mu.RUnlock()

	if ok {
		return asm, nil
	}

	// We will pass the quitCh to the account session manager if we happen to create it.
	s.mu.Lock()
	quitCh := s.quitCh
	s.mu.Unlock()

	// Not found, now take the write lock and check again
	sm.mu.Lock()
	defer sm.mu.Unlock()
	asm, ok = sm.sessions[accName]
	if ok {
		return asm, nil
	}
	// A shutting-down server does not CREATE a manager, only hand back one that
	// already exists. Start opens the listener before it initializes eagerly, so a
	// CONNECT can be queued on this very mutex while that initialization fails; without
	// this check it would then build a fresh manager — two ephemeral consumers included
	// — for a server that is on its way out, and the success path has no rollback. The
	// residual window this leaves (a CONNECT that passed here in the instant before
	// BeginShutdown flipped the flag) is stated in docs/CONFORMANCE.md rather than
	// closed with a second mechanism: its cost is bounded by InactiveThreshold.
	if s.isShuttingDown() {
		return nil, fmt.Errorf("MQTT server is shutting down")
	}
	// Need to create one here.
	asm, err := s.mqttCreateAccountSessionManager(acc, quitCh)
	if err != nil {
		return nil, err
	}
	sm.sessions[accName] = asm
	return asm, nil
}

// Creates JS streams/consumers for handling of sessions and messages for this account.
//
// Global session manager lock is held on entry.
func (s *Server) mqttCreateAccountSessionManager(acc *Account, quitCh chan struct{}) (*mqttAccountSessionManager, error) {
	accName := acc.GetName()

	opts := s.getOpts()
	c := s.createInternalAccountClient()
	c.acc = acc

	id := s.NodeName()

	mqttJSAPITimeout := opts.MQTT.JSAPITimeout
	if mqttJSAPITimeout == 0 {
		mqttJSAPITimeout = mqttDefaultJSAPITimeout
	}
	// The delayed-Will claim protocol depends on no single request being able to consume
	// a whole ownership window: the publish worker deliberately does not renew, so a
	// request timeout at or above the lease lets a licensing ack arrive after a peer
	// has already reclaimed the expired record and published it — a duplicate Will
	// with no crash anywhere. It does NOT follow that a request fits the lease REMAINING
	// once licensing has been acknowledged; that overrun is a disclosed residual, and
	// mqttWillTimingInvariant says so. Checked against the DEFAULT lease, not the effective
	// one: tests shorten the lease per-manager, and a two-second lease against the
	// five-second default timeout is their intent rather than a misconfiguration.
	// Enforced here rather than asserted in a test, because the timeout is an option
	// and an operator can set it.
	// Every input but the timeout is a DEFAULT: the timeout is the only one an
	// operator can set, and the defaults are known consistent among themselves (a
	// unit test asserts that). Mixing a default lease with the LIVE interval would
	// compare two worlds — tests retune the interval and lease together, and a
	// 30s interval under a 60s lease is safe on its own terms while failing against
	// the 30s default.
	if verr := mqttWillTimingInvariant(mqttWillDefaultClaimLease, mqttWillDefaultClaimInterval, mqttJSAPITimeout); verr != nil {
		return nil, fmt.Errorf("MQTT delayed-Will timing is unsafe for account %q: %w", accName, verr)
	}

	replicas := opts.MQTT.StreamReplicas
	if replicas <= 0 {
		replicas = s.mqttDetermineReplicas()
	}
	qname := fmt.Sprintf("[ACC:%s] MQTT ", accName)
	as := &mqttAccountSessionManager{
		sessions:        make(map[string]*mqttSession),
		sessByHash:      make(map[string]*mqttSession),
		sessLocked:      make(map[string]struct{}),
		flappers:        make(map[string]time.Time),
		pendingExpiries: make(map[string]mqttPendingExpiry),
		reconcileWM:     make(map[string]*sessionReconcileState),
		jsa: mqttJSA{mqttJSACore: &mqttJSACore{
			id:      id,
			c:       c,
			rplyr:   mqttJSARepliesPrefix + id + ".",
			sendq:   newIPQueue[*mqttJSPubMsg](s, qname+"send"),
			nuid:    nuid.New(),
			quitCh:  quitCh,
			timeout: mqttJSAPITimeout,
		}},
		rmsCache: &sync.Map{},
		rmObs:    mqttRetainedObserverState{wake: make(chan struct{}, 1)},
	}
	// JetStream domain. Unlike the NATS Server this code came from, the broker is
	// a plain client of exactly one external JetStream, so there is a single
	// source of truth: -js-domain (MQTTNB_JS_DOMAIN). There are no leafnode or
	// per-account topologies to resolve between. Empty means the domain of the
	// server we are connected to, which is the common case.
	//
	// The value is validated at startup (config.Validate) as a single subject
	// token, because it is interpolated both into the JS API subjects
	// ($JS.<domain>.API.*, see prefixDomain) and into the session subject prefix
	// used to store sessions in the $MQTT_sess stream.
	as.jsa.domain = opts.MQTT.JsDomain
	if as.jsa.domain != _EMPTY_ {
		as.domainTk = as.jsa.domain + "."
		s.Noticef("Creating MQTT streams/consumers with replicas %v for account %q in domain %q", replicas, accName, as.jsa.domain)
	} else {
		s.Noticef("Creating MQTT streams/consumers with replicas %v for account %q", replicas, accName)
	}

	var subs []*subscription
	var success bool
	closeCh := make(chan struct{})

	defer func() {
		if success {
			return
		}
		for _, sub := range subs {
			if uerr := c.processUnsub(sub.sid); uerr != nil {
				c.Warnf("MQTT: unable to remove subscription %q: %v", sub.sid, uerr)
			}
		}
		close(closeCh)
	}()

	// We create all subscriptions before starting the go routine that will do
	// sends otherwise we could get races.
	// Note that using two different clients (one for the subs, one for the
	// sends) would cause other issues such as registration of recent subs in
	// the "sub" client would be invisible to the check for GW routed replies
	// (shouldMapReplyForGatewaySend) since the client there would be the "sender".

	jsa := &as.jsa
	sid := int64(1)
	// This is a subscription that will process all JS API replies. We could split to
	// individual subscriptions if needed, but since there is a bit of common code,
	// that seemed like a good idea to be all in one place.
	if err := as.createSubscription(jsa.rplyr+">",
		as.processJSAPIReplies, &sid, &subs); err != nil {
		return nil, err
	}

	// We create the subscription on "$MQTT.sub.<ephemeralID>.rmsgs" to limit the
	// subjects that a user would allow permissions on. DERIVED, not freshly minted:
	// the retained observer this feeds is named from the same token, so an
	// initialization retry asks for the same consumer instead of stranding the
	// previous attempt's under a name nothing can compute again. See
	// ephemeral_identity.go.
	//
	// The subject is per GENERATION: a reset re-creates the observer on a fresh one, and
	// the subscription is what tells a current delivery from a stale one. The generation is
	// installed here, before the consumer exists, because it may deliver before its create
	// returns. See retained_index.go.
	ephemeralID, ephemeralOwner := s.ephemeralIdentity()
	rmsubj := mqttRetainedObserverDeliverSubject(ephemeralID, s.mqtt.rmObsGen.Add(1))
	if err := as.createSubscription(rmsubj, as.processRetainedMsg, &sid, &subs); err != nil {
		return nil, err
	}
	as.rmObs.sub = subs[len(subs)-1]

	// The session-record watch's delivery subject. Every session-record write in the
	// account arrives here, which is how this broker learns that one of its sessions'
	// records MOVED — not who moved it, which no observation shows (F-084). The
	// at-most-once `$MQTT.JSA.*.SP.*.*` notification this replaces could simply be
	// lost, stranding a displaced client indefinitely.
	// Under "$MQTT.sub." and on the SERVICE connection, like the retained consumer,
	// so the recommended client ACL denying "$MQTT.>" stays correct.
	// Derived from the same per-run token as the retained observer's, for the same
	// reason. See sess_record_watch.go and ephemeral_identity.go.
	srwsubj := mqttSessRecordWatchDeliverSubject(ephemeralID)
	if err := as.createSubscription(srwsubj, as.processSessionRecordDelivery, &sid, &subs); err != nil {
		return nil, err
	}

	// No more creation of subscriptions past this point otherwise RACEs may happen.

	// Start the go routine that will send JS API requests.
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		as.sendJSAPIrequests(s, c, accName, closeCh)
	})

	// Start the go routine that will clean up cached retained messages that expired.
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		as.cleanupRetainedMessageCache(s, closeCh)
	})

	// Provision the ten broker-owned $MQTT_* streams through the one
	// convergence shape (ensureBrokerStream): create-or-lose, an UNCONDITIONAL
	// authoritative re-read, then per-owner validation over that re-read — a
	// nonconforming stream is a startup failure with recreate guidance, never
	// something to mutate in place. The ordered spec table is the only route: a
	// stream cannot be provisioned outside the helper without deleting its entry,
	// which the table-coverage test refuses. Per-stream specials (the recovery
	// epoch's generation, gate hooks and adoption, and the per-stream stamp that
	// couples the ten) live in the entries, not around this loop.
	wantReplicas := 0
	if opts.MQTT.StreamReplicas != 0 {
		wantReplicas = replicas
	}
	maxAckPending := int(opts.MQTT.MaxAckPending)
	if maxAckPending == 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}
	// The ordered provisioning table, built ONCE and used by both the preflight and
	// the loop. One construction is the point: a preflight looking up a
	// hand-maintained second list would drift from the table it is meant to police,
	// and the table-coverage test proves the table's contents, not that some other
	// list matches it.
	specs := as.brokerStreamSpecs(s, replicas, wantReplicas, maxAckPending)

	// Two lookup-only preflights, before anything is created, because the loop's own
	// shape creates an absent stream before it validates one.
	//
	// First: an account still holding the retired $MQTT_origin register is refused
	// outright. Second: an account holding a PROPER SUBSET of the ten broker-owned
	// streams is refused rather than silently completed — those ten are one
	// recreation unit, and recreating a subset leaves records referencing identities
	// the other streams have since reissued (F-066). Both take
	// brokerStreamLookupAPI, so neither can create what it is deciding about.
	if err := mqttRefuseRetiredOriginStream(jsa); err != nil {
		return nil, fmt.Errorf("MQTT stream prerequisites for account %q: %w", accName, err)
	}
	if err := mqttPreflightBrokerStreamBundle(jsa, specs, s.bundleSettleBudget, s.quitCh); err != nil {
		return nil, fmt.Errorf("MQTT stream prerequisites for account %q: %w", accName, err)
	}
	// Which purposes this attempt CONFIRMABLY created, which is the rollback's only
	// licence: an errored create — of any shape, definite or not — licenses nothing,
	// because a structured error can be returned before the server even looks the
	// name up. See mqttEphemeralCreated in ephemeral_identity.go.
	//
	// Nothing in the loop below confirms into it any more: the retained observer
	// moved to mqttProvisionEphemeralConsumers, so the ledger is necessarily empty
	// until that call and a rollback from inside the loop would have nothing to
	// remove.
	created := mqttEphemeralCreated{}
	for _, spec := range specs {
		si, err := ensureBrokerStream(jsa, spec.cfg, spec.validate, spec.preCreate, spec.observe)
		if err != nil {
			return nil, fmt.Errorf("MQTT %s stream for account %q: %w", spec.txt, accName, err)
		}
		if spec.post != nil {
			if err := spec.post(si); err != nil {
				return nil, err
			}
		}
	}

	// Both per-run ephemeral consumers — the retained-messages observer and the
	// session-record watch — created only once EVERY stream has provisioned and
	// validated, and rolling back whatever this attempt confirmably created if
	// either fails.
	//
	// Every connection retries this whole function UNTIL one attempt succeeds, or
	// until Start's own attempt fails and tears the server down: the listener opens
	// in startMQTT before Start initializes the manager, so a CONNECT arriving in
	// that window drives initialization itself. What a retry must not do is mint
	// another consumer, which is why both names are derived. See
	// mqttProvisionEphemeralConsumers and ephemeral_identity.go.
	if err := as.mqttProvisionEphemeralConsumers(accName, srwsubj, rmsubj, ephemeralID, ephemeralOwner, created); err != nil {
		return nil, err
	}

	// Re-arm session-expiry timers, without delaying the connecting client. Then
	// re-sweep periodically: expiries catch deadlines that never got a timer (e.g.
	// records orphaned when their owner crashed while the client was connected),
	// and the orphaned-consumer sweep reconciles durables leaked in crash windows
	// (re-run so it also catches orphans skipped by the grace window at startup).
	// reconcileResidentRecordState rides along here but is deliberately NOT part of
	// sweepSessionExpiries: that walks persisted records and skips ExpiryInterval 0
	// or never-expire, which are exactly the two cases that park a deferred clear()
	// or a deferred Will. See sess_uncertain.go.
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		as.sweepSessionExpiries(s)
		as.sweepOrphanedConsumers(s)
		as.sweepSharedGroupRetirements(s)
		as.sweepAcqReleaseVerdicts(s)
		as.sweepOrphanedQoS2State(s)
		tt := time.NewTicker(mqttSessionExpirySweepInterval())
		defer tt.Stop()
		for {
			select {
			case <-tt.C:
				as.reconcileResidentRecordState(s)
				as.sweepSessionExpiries(s)
				as.sweepOrphanedConsumers(s)
				// The shared-group arms are passes OF THEIR OWN, never behind the consumer
				// sweep's "no candidates" exit: a retiring entry whose consumer is already gone
				// and a release verdict on a record whose consumer is still young are both work
				// that sweep would never reach.
				as.sweepSharedGroupRetirements(s)
				as.sweepAcqReleaseVerdicts(s)
				as.sweepOrphanedQoS2State(s)
				// The outbound recovery pass rides the same cadence (F-105, row 6). It is
				// deliberately NOT run at startup beside the three above: those reconcile state
				// this broker may have left behind, while this one walks a durable directory
				// that any broker may walk at any time, so there is nothing about a fresh start
				// that makes it urgent. It checks shutdown between store operations, so it adds
				// at most one operation to the shutdown latency of this goroutine — except
				// inside its account-wide readiness walk, where the granularity is one bounded
				// RECORD UNIT rather than one operation, plus the pass's own bounded progress
				// bookkeeping. See performAffineReadiness for why that walk cannot be charged.
				as.sweepOutboundRecovery(s, func() bool {
					select {
					case <-closeCh:
						return true
					case <-s.quitCh:
						return true
					default:
						return false
					}
				})
			case <-closeCh:
				return
			case <-s.quitCh:
				return
			}
		}
	})

	// The retained observer's loss detector and resume, started only now: until
	// provisioning succeeded, initialization owned the consumer, its rollback and its
	// subscription cleanup, and a failed initialization never reaches this line. See
	// retained_index.go.
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		as.retainedObserverLoop(s, closeCh)
	})

	// Run the delayed-Will claim loop on its own cadence. Tracked by willClaimWG
	// (not grWG) so shutdown Phase 2 drains it before waiting on publisher workers.
	s.willClaimWG.Add(1)
	go func() {
		as.willClaimLoop(s, closeCh)
	}()

	// Set this so that on defer we don't cleanup.
	success = true

	return as, nil
}

// mqttDetermineReplicas chooses the count for new streams when -stream-replicas is 0.
// cmd/mqtt-nats-broker never sets Options.Routes and its StreamReplicas 0 makes
// JetStreamIsClustered false, so for this binary the answer is always 1; the route
// count below applies only to a caller that sets both.
func (s *Server) mqttDetermineReplicas() int {
	// If not clustered, then replica will be 1.
	if !s.JetStreamIsClustered() {
		return 1
	}
	opts := s.getOpts()
	replicas := 0
	for _, u := range opts.Routes {
		host := u.Hostname()
		// If this is an IP just add one.
		if net.ParseIP(host) != nil {
			replicas++
		} else {
			addrs, _ := net.LookupHost(host)
			replicas += len(addrs)
		}
	}
	if replicas < 1 {
		replicas = 1
	} else if replicas > 3 {
		replicas = 3
	}
	return replicas
}

// Adds this client ID to the flappers map, and if needed start the timer
// for map cleanup.
//
// Lock held on entry.
func (as *mqttAccountSessionManager) addSessToFlappersLocked(clientID string) {
	as.flappers[clientID] = time.Now()
	if as.flapTimer == nil {
		as.flapTimer = time.AfterFunc(mqttFlapCleanItvl.Get(), func() {
			as.mu.Lock()
			defer as.mu.Unlock()
			// In case of shutdown, this will be nil
			if as.flapTimer == nil {
				return
			}
			now := time.Now()
			for cID, tm := range as.flappers {
				if now.Sub(tm) > mqttSessJailDur.Get() {
					delete(as.flappers, cID)
				}
			}
			as.flapTimer.Reset(mqttFlapCleanItvl.Get())
		})
	}
}

// Remove this client ID from the flappers map.
//
// Lock held on entry.
func (as *mqttAccountSessionManager) removeSessFromFlappersLocked(clientID string) {
	delete(as.flappers, clientID)
	// Do not stop/set timer to nil here. Better leave the timer run at its
	// regular interval and detect that there is nothing to do. The timer
	// will be stopped on shutdown.
}

// storeSessionRecordCAS rewrites a session record expecting the given stream
// sequence, so a concurrent writer (e.g. a reconnect elsewhere) wins and this
// rewrite is dropped.
func (as *mqttAccountSessionManager) storeSessionRecordCAS(ps *mqttPersistedSession, idHash string, seq uint64) error {
	_, err := mqttStoreSessionRecordCAS(&as.jsa, as.domainTk, ps, idHash, seq)
	return err
}

// mqttStoreSessionRecordCAS is storeSessionRecordCAS's body, on the JSA rather
// than the account manager and returning the new sequence. The teardown claim
// needs both: it runs from sess.clear(), which is handed no account context, and
// it must know the sequence its marker landed at in order to release it. See
// sess_teardown.go.
func mqttStoreSessionRecordCAS(jsa *mqttJSA, domainTk string, ps *mqttPersistedSession, idHash string, seq uint64) (uint64, error) {
	b, _ := json.Marshal(ps)
	return mqttStoreSessionRecordCASBody(jsa, domainTk, b, idHash, seq)
}

// mqttStoreSessionRecordCASBody is the same CAS store given the record body
// verbatim, for callers that already hold the exact bytes.
func mqttStoreSessionRecordCASBody(jsa *mqttJSA, domainTk string, b []byte, idHash string, seq uint64) (uint64, error) {
	bb := bytes.Buffer{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":")
	bb.WriteString(strconv.FormatUint(seq, 10))
	bb.WriteString(CR_LF)
	bb.WriteString(CR_LF)
	hdr := bb.Len()
	bb.Write(b)
	resp, err := jsa.storeSessionMsg(domainTk, idHash, hdr, bb.Bytes())
	if err != nil {
		return 0, err
	}
	return resp.Sequence, nil
}

// sweepOrphanedConsumers deletes MQTT-owned durables on the messages stream
// that no session references anymore: a crash between a confirmed record
// save/delete and the consumer teardown (UNSUBSCRIBE, session end, 3.1.1
// forced leave) leaks the durable forever. Consumers are listed BEFORE the
// record walk: a shared join persists its record before (re)creating the
// group durable — and repairs a lost race with a delete by re-issuing the
// idempotent create — so a listed consumer the walk does not reference was
// orphaned at list time. Plain session durables are created before the record
// save that references them, so only consumers older than
// mqttOrphanSweepGrace are eligible; consumers held by local in-memory
// sessions count as referenced too. Any indeterminate scan aborts the sweep
// (fail closed: never delete on a partial view). Runs from the account
// session manager's sweep go routine. Spec5 [4.8.2].
func (as *mqttAccountSessionManager) sweepOrphanedConsumers(s *Server) {
	as.sweepOrphanedConsumersBefore(s, time.Now().Add(-mqttOrphanSweepGrace.Get()))
}

// sweepOrphanedConsumersBefore is sweepOrphanedConsumers with the eligibility cutoff as a
// parameter. The grace window is real time, so a test that could not move it would have no
// eligible candidates and every "nothing was deleted" assertion would hold vacuously.
func (as *mqttAccountSessionManager) sweepOrphanedConsumersBefore(s *Server, cutoff time.Time) {
	consumers, err := as.jsa.consumerList(mqttStreamName)
	if err != nil {
		s.Debugf("MQTT orphaned consumer sweep ended: %v", err)
		return
	}
	// Candidates: MQTT-shaped (a shared group durable, or a session durable
	// delivering to "$MQTT.sub.") and older than the grace window. Anything
	// else on the stream is not ours to judge. A consumer the clustered LIST
	// could not report (Missing) is not a candidate until a later sweep sees it.
	candidates := make(map[string]struct{})
	var sharedNames []string
	for _, ci := range consumers {
		if ci == nil || ci.Config == nil || !ci.Created.Before(cutoff) {
			continue
		}
		// A SHARED GROUP CONSUMER is not judged by the record walk below at all: whether it
		// may go is its GROUP RECORD's arbitrated state (live, retiring, or derived retired),
		// read from a record that exists — never the absence of a reference across the
		// session records. It is dispositioned by sweepSharedConsumers after this pass.
		if strings.HasPrefix(ci.Name, mqttSharedConsumerDurablePrefix) {
			sharedNames = append(sharedNames, ci.Name)
			continue
		}
		if !strings.HasPrefix(ci.Config.DeliverSubject, mqttSubPrefix) {
			continue
		}
		candidates[ci.Name] = struct{}{}
	}
	if len(sharedNames) > 0 {
		as.sweepSharedConsumers(s, sharedNames)
	}
	if len(candidates) == 0 {
		return
	}
	// A local session's in-memory state may be newer than its persisted record
	// (a save may be in flight); count its consumers as referenced.
	as.mu.RLock()
	sesss := make([]*mqttSession, 0, len(as.sessions))
	for _, sess := range as.sessions {
		sesss = append(sesss, sess)
	}
	as.mu.RUnlock()
	for _, sess := range sesss {
		sess.mu.Lock()
		for _, cc := range sess.cons {
			delete(candidates, cc.Durable)
		}
		// A draining durable is referenced: it exists precisely because started
		// deliveries are still owed on it, and reaping it would delete them with it
		// (the messages stream is interest-retained). See unsub_drain.go.
		for _, dur := range sess.drainDurablesLocked() {
			delete(candidates, dur)
		}
		sess.mu.Unlock()
	}
	// Walk every session record; any referenced durable is not an orphan.
	filter := mqttSessStreamSubjectPrefix + as.domainTk + ">"
	for seq := uint64(1); len(candidates) > 0; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				s.Debugf("MQTT orphaned consumer sweep ended: %v", err)
				return
			}
			break
		}
		seq = smsg.Sequence + 1
		ps := &mqttPersistedSession{}
		if err := json.Unmarshal(smsg.Data, ps); err != nil {
			// An undecodable record may still reference candidates: abort.
			s.Warnf("MQTT orphaned consumer sweep: unable to decode session record at sequence %v: %v", smsg.Sequence, err)
			return
		}
		// A record whose shape this broker does not write is treated exactly like an
		// undecodable one: ABORT the whole sweep, never skip the record. This walk decides
		// what to DELETE by absence of a reference, so a record it cannot account for may
		// well be referencing candidates — and a partial view must never be reported as a
		// complete one. See mqttSharedConsInRecord.
		if sid, found := mqttSharedConsInRecord(ps); found {
			s.Warnf("MQTT orphaned consumer sweep: aborting, %v", mqttSharedConsRecordErr(ps.ID, sid))
			return
		}
		for _, cc := range ps.Cons {
			delete(candidates, cc.Durable)
		}
		for _, pd := range ps.Drains {
			if pd != nil && pd.Cons != nil {
				delete(candidates, pd.Cons.Durable)
			}
		}
	}
	for dur := range candidates {
		// A physical name's DELETE is authorized by its OWNERSHIP record, never by the mere
		// absence of a session reference (F-105 §5.1 point 5). This sweep's original defect was
		// exactly that: a source consumer LIVE with an outstanding outbound obligation, aged past
		// the grace window and no longer referenced by a local session or a record, was deleted
		// out from under the obligation. Consult the name's ownership on $MQTT_msgs before deleting:
		//   - RETIRED authorizes disposal — the name is spent and a physical consumer found under
		//     it is an orphan, INCLUDING a retired name resurrected by a delayed CREATE after its
		//     cleanup duty completed, which must be revisited and reaped (point 6);
		//   - LIVE means an obligation may still depend on the name, so it is WITHHELD;
		//   - a MISSING record is WITHHELD too, never read as permission (the name is claimed
		//     BEFORE its CREATE, so absence means no claim landed, not a free name);
		//   - an INDETERMINATE read SKIPS this candidate, never deletes it. Unlike the session
		//     record walk (whose undecodable record may reference OTHER candidates, so it aborts
		//     the whole view), a failed ownership read concerns only this one name, so skipping it
		//     is the fail-closed choice AND lets the rest of the pass make progress (Codex consult
		//     14): a persistent read error on one name must not obstruct unrelated cleanup.
		own, _, oerr := as.jsa.loadNameOwnershipPhysical(as.sessStreamEpoch, mqttOwnedStreamMsgs, dur)
		if oerr != nil {
			s.Warnf("MQTT orphaned consumer sweep: skipping consumer %q: unable to read its name ownership: %v", dur, oerr)
			continue
		}
		if own == nil || own.State != mqttNameOwnRetired {
			// Missing (withheld) or LIVE (an obligation may still depend on it): not deletable.
			continue
		}
		if _, err := as.jsa.deleteConsumer(mqttStreamName, dur, false); isErrorOtherThan(err, JSConsumerNotFoundErr) {
			s.Warnf("MQTT orphaned consumer sweep: unable to delete consumer %q: %v", dur, err)
			continue
		}
		s.Debugf("MQTT orphaned consumer sweep deleted consumer %q under its RETIRED name ownership", dur)
	}
}

// Helper to create a subscription. It updates the sid and array of subscriptions.
func (as *mqttAccountSessionManager) createSubscription(subject string, cb msgHandler, sid *int64, subs *[]*subscription) error {
	// An internal deliver-subject subscription: it carries no retained frames, so the
	// displaced identifiers are always empty here.
	sub, _, err := as.jsa.c.processSub([]byte(subject), nil, []byte(strconv.FormatInt(*sid, 10)), cb, false)
	if err != nil {
		return err
	}
	*sid++
	*subs = append(*subs, sub)
	return nil
}

// First check if this session's client ID is already in the "locked" map,
// which if it is the case means that another client is now bound to this
// session and this should return an error.
// If not in the "locked" map, but the client is not bound with this session,
// then same error is returned.
// Finally, if all checks ok, then the session's ID is added to the "locked" map.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) lockSession(sess *mqttSession, c *client) error {
	as.mu.Lock()
	defer as.mu.Unlock()
	var fail bool
	if _, fail = as.sessLocked[sess.id]; !fail {
		sess.mu.Lock()
		fail = sess.c != c
		sess.mu.Unlock()
	}
	if fail {
		return fmt.Errorf("another session is in use with client ID %q", sess.id)
	}
	as.sessLocked[sess.id] = struct{}{}
	return nil
}

// lockSessionForVoidHandoff takes the session gate as NOBODY for a release-triggered
// void-verdict attempt and, when it cannot, gives up the handoff loop's ownership IN THE
// SAME as.mu CRITICAL SECTION.
//
// That atomicity is the whole point, and it closes a lost wakeup the two-step form has: a
// gate owner deletes its sessLocked entry under as.mu and only then asks whether a handoff
// loop is running. Clearing the flag after as.mu was released lets that owner observe a loop
// that is about to exit, suppress its own trigger, and leave the staged revision with no
// successor at all — on an unbound, never-expiring session, indefinitely. Done here instead,
// the owner either released BEFORE this section — in which case this attempt finds the gate
// free and wins it — or reads the flag AFTER it, already clear, and fires its own loop.
//
// Lock order is the established as.mu → sess.mu, same as lockSession.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) lockSessionForVoidHandoff(sess *mqttSession) error {
	as.mu.Lock()
	defer as.mu.Unlock()
	// RESIDENCY by identity, in the same critical section as the gate itself, which is what
	// makes it a claim rather than a stale sample: a trigger's session pointer can be
	// evicted before its turn — a remote persist, an accept rollback, an expiry, a foreign
	// disposition — and that object may no longer write its record. Claiming the
	// shared client-ID gate for it would hold a replacement CONNECT off across a JetStream
	// store for a session this broker no longer has. Identity, not client ID: a CONNECT may
	// already have installed a restored replacement under the same ID.
	fail := as.sessions[sess.id] != sess
	if !fail {
		if _, fail = as.sessLocked[sess.id]; !fail {
			sess.mu.Lock()
			fail = sess.c != nil
			sess.mu.Unlock()
		}
	}
	if fail {
		sess.mu.Lock()
		sess.voidHandoffActive = false
		sess.mu.Unlock()
		return fmt.Errorf("session %q is in use; its void-verdict handoff yields", sess.id)
	}
	as.sessLocked[sess.id] = struct{}{}
	return nil
}

// lockSessionForRecordRejection takes the session gate as NOBODY for a periodic
// rejection probe, and it is the SOLE unboundness check on that path.
//
// Residency BY IDENTITY, in the same as.mu critical section as the claim, is what makes
// the probe's foreign classification true: the periodic pass snapshots session
// pointers, and plain lockSession checks only sessLocked and sess.c — so a stale object
// could take the gate after a same-broker replacement had been installed and saved, and
// then read that locally authored record as another owner's and dispose of the live
// session. Identity, not client ID: a CONNECT may already have installed a restored
// replacement under the same ID.
//
// Unbound-only, and that is what makes THIS settlement unbound-only: claiming the gate as
// nobody for a healthy bound session would fail a concurrent SUBSCRIBE's own lockSession,
// and that packet error closes a live connection — a disruption the uncertainty pass only
// inflicts on sessions that are already unusable. So a bound session's marker simply has
// its claim refused here. It is not left unconverged: the bound session settles the marker
// on its OWN readLoop before the next packet is dispatched, claiming the gate as itself
// (mqttGateDispatchOnRejection), and this pass settles whatever survives the unbind. The
// check lives here and nowhere else: a duplicate check in the caller would mask a mutation
// of this one. See sess_rejected_write.go.
//
// Lock order is the established as.mu → sess.mu, same as lockSession.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) lockSessionForRecordRejection(sess *mqttSession) error {
	as.mu.Lock()
	defer as.mu.Unlock()
	fail := as.sessions[sess.id] != sess
	if !fail {
		if _, fail = as.sessLocked[sess.id]; !fail {
			sess.mu.Lock()
			fail = sess.c != nil
			sess.mu.Unlock()
		}
	}
	if fail {
		return fmt.Errorf("session %q is in use; its record-rejection probe yields", sess.id)
	}
	as.sessLocked[sess.id] = struct{}{}
	return nil
}

// lockSessionForTeardown takes the session gate on behalf of a connection that is
// about to end — a close, or a bound record-rejection settlement that has already
// decided not to serve — retrying for as long as that connection is still the
// session's owner.
//
// lockSession fails for two very different reasons: the session has been rebound
// to another connection (a takeover — nothing below is this connection's to do
// any more), or the gate is merely held for a moment by another ownership-gated
// operation, such as an uncertain-record resolution pass. Only the first is a
// reason to give up. Giving up on the second would silently drop this
// connection's whole teardown — including the Will it owes — over contention
// that was about to clear.
//
// Bounded, and mirrors the CONNECT side's own spin (mqttProcessConnect). It
// cannot deadlock: nothing that holds the gate waits on this teardown, and a
// CONNECT registers the gate before it rebinds the session, so a teardown
// waiting on a CONNECT observes the rebind on its next attempt. The settlement
// caller keeps that true by spinning here BEFORE it takes the record fence — a
// waiter holding the fence a gate holder needs would close the cycle.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) lockSessionForTeardown(sess *mqttSession, c *client) error {
	var err error
	for attempt := 0; attempt < mqttTeardownLockAttempts; attempt++ {
		if err = as.lockSession(sess, c); err == nil {
			return nil
		}
		sess.mu.Lock()
		rebound := sess.c != c
		sess.mu.Unlock()
		if rebound {
			return err
		}
		time.Sleep(mqttTeardownLockItvl)
	}
	return err
}

// beforeUngatedPark runs the ungated-park test seam, if one is installed.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) beforeUngatedPark(clientID string) {
	as.mu.RLock()
	hook := as.testBeforeUngatedPark
	as.mu.RUnlock()
	if hook != nil {
		hook(clientID)
	}
}

// afterTeardownOwnerLost runs the lost-owner teardown test seam, if one is installed.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) afterTeardownOwnerLost(clientID string) {
	as.mu.RLock()
	hook := as.testAfterTeardownOwnerLost
	as.mu.RUnlock()
	if hook != nil {
		hook(clientID)
	}
}

// beforeBoundDisconnectHonor runs the bound-DISCONNECT takeover test seam, if one is
// installed.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) beforeBoundDisconnectHonor(clientID string) {
	as.mu.RLock()
	hook := as.testBeforeBoundDisconnectHonor
	as.mu.RUnlock()
	if hook != nil {
		hook(clientID)
	}
}

// beforeTeardownLock runs the teardown-ordering test seam, if one is installed.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) beforeTeardownLock(clientID string) {
	as.mu.RLock()
	hook := as.testBeforeTeardownLock
	as.mu.RUnlock()
	if hook != nil {
		hook(clientID)
	}
}

// afterParkedJailDecision runs the parked-close jail seam, if one is installed.
//
// Deliberately takes NO lock: see the field's comment. In production the hook is nil
// and this is one atomic load.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) afterParkedJailDecision(clientID string) {
	if hook := as.testAfterParkedJailDecision.Load(); hook != nil {
		(*hook)(clientID)
	}
}

// beforeTakeoverDiscard runs the takeover-ordering test seam, if one is installed.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) beforeTakeoverDiscard(clientID string) {
	as.mu.RLock()
	hook := as.testBeforeTakeoverDiscard
	as.mu.RUnlock()
	if hook != nil {
		hook(clientID)
	}
}

// beforeResumeNaks runs the resume-ordering test seam, if one is installed.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) beforeResumeNaks(clientID string) {
	as.mu.RLock()
	hook := as.testBeforeResumeNaks
	as.mu.RUnlock()
	if hook != nil {
		hook(clientID)
	}
}

// Remove the session from the "locked" map.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) unlockSession(sess *mqttSession) {
	as.mu.Lock()
	delete(as.sessLocked, sess.id)
	as.mu.Unlock()
	as.handoffVoidVerdictsOnGateRelease(sess)
}

// handoffVoidVerdictsOnGateRelease is the ownership-gate half of the release handoff: a
// verdict staged while this gate was held could not be persisted by its stager — a
// resolver's fence and the gate itself both refuse it — and an UNBOUND session has no
// packet tail that would come back to it. See voidVerdictHandoffDueLocked.
//
// Both gate-release forms call it, and that is not redundancy: CONNECT releases through its
// own inline delete rather than through unlockSession, so naming only unlockSession would
// leave every CONNECT that ends with the session unbound uncovered.
//
// It must be called with as.mu NOT held. The attempt takes the ownership gate, and its own
// fence release re-enters this decision; doing that under as.mu would deadlock against
// lockSession.
//
// Nonresident objects are skipped, by the attempt's own gate acquisition: a session that has
// been evicted — a foreign disposition, a rollback, an expiry — has no record of ours to
// write, because the subject has moved past the base it can speak for, and a verdict about
// a register of a generation nobody resumes is not ours to persist.
func (as *mqttAccountSessionManager) handoffVoidVerdictsOnGateRelease(sess *mqttSession) {
	if sess == nil {
		return
	}
	sess.mu.Lock()
	due := sess.voidVerdictHandoffDueLocked()
	sess.mu.Unlock()
	if !due {
		return
	}
	// Residency is not checked here: the attempt's gate acquisition checks it by identity,
	// in the same critical section that claims the gate, which is the only place a
	// pre-check could not go stale. See lockSessionForVoidHandoff.
	sess.attemptVoidVerdictHandoff(nil)
}

// Simply adds the session to the various sessions maps.
// The boolean `lock` indicates if this function should acquire the lock
// prior to adding to the maps.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) addSession(sess *mqttSession, lock bool) {
	if lock {
		as.mu.Lock()
	}
	as.sessions[sess.id] = sess
	as.sessByHash[sess.idHash] = sess
	// RE-ENROL THE RECLAMATION CONTINUATION, because nonresidency is not terminal. This
	// same object comes back here from the expiry fire's drain timeout, from the
	// clean-disconnect drain timeout, and from refused-discard recovery — and a pass that
	// observed the removal has already disarmed itself and returned. A restoration installs
	// no exclusion of its own, so without this the standing exclusions would be resident
	// again with no timer and their identifiers would never be reclaimed.
	//
	// HERE rather than at each of those call sites: this is the one function all of them
	// funnel through, which is the same reason the install-side arm lives inside
	// excludeIdentifierLocked rather than at its 27 callers. Arming is timer-only, so
	// taking sess.mu under as.mu introduces no synchronous callback and no reverse
	// acquisition; as.mu -> sess.mu is the established order.
	sess.mu.Lock()
	sess.armExclusionReclaimIfOwedLocked()
	sess.mu.Unlock()
	if lock {
		as.mu.Unlock()
	}
}

// mqttTestSessionRemovedV observes every session object leaving the account maps, after
// the maps are updated and possibly with as.mu held, so it must neither block nor take a
// lock. The resource soak captures each removed object weakly and requires it to become
// unreachable once its work has completed. Nil in production.
var mqttTestSessionRemovedV atomic.Pointer[func(*mqttSession)]

// Simply removes the session from the various sessions maps.
// The boolean `lock` indicates if this function should acquire the lock
// prior to removing from the maps.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) removeSession(sess *mqttSession, lock bool) {
	if lock {
		as.mu.Lock()
	}
	delete(as.sessions, sess.id)
	delete(as.sessByHash, sess.idHash)
	if h := mqttTestSessionRemovedV.Load(); h != nil {
		(*h)(sess)
	}
	// The session is gone, so an entry kept alive only because it was resident has
	// nothing left to be about. Pruning HERE is what bounds the map: an undecided
	// trigger now outlives its worker, so the worker's own cleanup can no longer be
	// the last word. See pruneReconcileEntryLocked, which refuses when a worker is
	// still in flight or when a replacement installed its own entry.
	as.pruneReconcileEntryLocked(sess.idHash, as.reconcileWM[sess.idHash])
	if lock {
		as.mu.Unlock()
	}
}

// removeSessionIfCurrent removes the session from the maps ONLY while they still
// point at this object.
//
// removeSession deletes by client ID and hash unconditionally, which is wrong for
// any late or racing disposition: a CONNECT that has already evicted this object and
// installed a restored replacement would have that replacement deleted instead,
// leaving a live connection with a session no map knows about. Identity is the only
// safe predicate.
//
// No lock held on entry.
// It also cancels the session's expiry timer, in the SAME critical section: the
// timer is keyed by client hash, so cancelling it before knowing whether this object
// is still the resident one would silently disarm a replacement session's
// finite-expiry deadline — which the periodic sweep would eventually re-arm, but not
// before that session had outlived the interval it was promised. Removal and
// cancellation are one decision about one identity, so they belong under one lock.
func (as *mqttAccountSessionManager) removeSessionIfCurrent(sess *mqttSession) bool {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.sessions[sess.id] != sess {
		return false
	}
	delete(as.sessions, sess.id)
	if as.sessByHash[sess.idHash] == sess {
		delete(as.sessByHash, sess.idHash)
	}
	if h := mqttTestSessionRemovedV.Load(); h != nil {
		(*h)(sess)
	}
	as.cancelSessionExpiryLocked(sess.idHash)
	// Same reason as removeSession's prune, and the helper's own residency check is
	// what keeps this correct when the hash map still points at a replacement.
	as.pruneReconcileEntryLocked(sess.idHash, as.reconcileWM[sess.idHash])
	return true
}

// mqttReservedMembershipInRecord returns the first subscription key in the
// record that lies in the reserved "$MQTT.>" namespace, looking in Subs,
// SharedSubs, Cons and Drains. Such a key can only have been written by a
// development broker predating the namespace deny (processSubs has rejected
// those filters ever since, and buildPersistedSessionLocked persists only what
// the session holds), so the record's schema is UNSUPPORTED and restoration
// fails closed before any name is derived from it — no migration, no
// half-honored record. CONTRIBUTING.md forbids migration code before the first
// release; the operator recreates the broker-owned state instead.
//
// Such a record is almost certainly also generationless, since the deny predates
// generation-scoped naming — which is why the caller runs this check BEFORE the
// generation one. A reserved key alongside a VALID generation is a shape no writer
// could produce (a broker new enough to mint generations already denied the filter);
// it is refused all the same, because a fail-closed schema boundary is not owed a
// provenance story.
//
// Read-only, and deliberately narrow: it checks the key SHAPES a pre-deny
// writer could emit, not the record's schema in general. A shared key that does
// not parse is skipped at restore anyway, and mqttPersistedDrain.Subject is not
// independently checked — no writer, pre- or post-deny, can produce a drain
// whose key is harmless while its subject is reserved (drains postdate the deny
// entirely), and defending against a hand-tampered record is general schema
// validation rather than this boundary.
func mqttReservedMembershipInRecord(ps *mqttPersistedSession) (string, bool) {
	// A plain filter and a level-up sid both start with the prefix; a shared key
	// ("$share/g/$MQTT.msgs.>") never does, so its underlying subject is tested.
	isReserved := func(key string) bool {
		if strings.HasPrefix(key, mqttPrefix) {
			return true
		}
		if _, subject, ok := mqttParseSharedKey(key); ok {
			return strings.HasPrefix(subject, mqttPrefix)
		}
		return false
	}
	// Cons and Drains are keyed by sid, so a record can carry a reserved consumer
	// with no membership left to name it: the old in-place migration was driven by
	// the memberships alone and would leave those behind.
	for key := range ps.Subs {
		if isReserved(key) {
			return key, true
		}
	}
	for key := range ps.SharedSubs {
		if isReserved(key) {
			return key, true
		}
	}
	for key := range ps.Cons {
		if isReserved(key) {
			return key, true
		}
	}
	for key := range ps.Drains {
		if isReserved(key) {
			return key, true
		}
	}
	return _EMPTY_, false
}

// createOrRestoreSessionWithin creates this client ID's session record if none exists,
// or recovers the single record to rebuild the session; a record whose session is not
// registered in this server's runtime makes the owner close its client, since
// [MQTT-3.1.4-2] requires a Client already connected with that ClientId to be
// disconnected. It runs under a CALLER'S re-drive budget, so a CONNECT that re-enters
// the restore cannot silently start a fresh one.
//
// The loop is R1's entry point (sess_takeover_redrive.go): the two outcomes reachable
// from here that mean "the base I was going to act on is not current" — a superseded
// discard of an overdue record, and a superseded release inside an adopted teardown —
// re-read the subject and restart the whole restore decision rather than returning an
// object derived from what they already know is stale.
//
// This is transition T0: pre-bind and NOT resident. The object was built from the
// record moments ago, is in no map and has no client, so there is nothing to dispose —
// on exhaustion it is simply garbage nobody references.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (as *mqttAccountSessionManager) createOrRestoreSessionWithin(clientID string, opts *Options,
	rd *mqttRecordReDrive) (*mqttSession, bool, error) {

	jsa := &as.jsa
	hash := getHash(clientID)
RESTORE:
	smsg, err := jsa.loadSessionMsg(as.domainTk, hash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, false, fmt.Errorf("loading session record: %w", err)
		}
		// Message not found, so create the session and indicate it did not exist.
		//
		// A create is PARENTLESS. It used to inherit the identity of the record whose
		// removal emptied this subject, so that a sibling create stayed recognisable;
		// the write-lineage audit deleted every such source, because each documented
		// route through which that inheritance produced a verdict was a FALSE fork —
		// the inherited identity outlived the interval it named. A create carrying no
		// PWID concedes at classifyRecordLineage's second conjunct from either
		// orientation. That unrecognisability is the stated residual; what it concedes TO
		// is the UNSETTLED vocabulary, not a takeover claim (F-084). See
		// sess_fork_lineage.go and docs/CONFORMANCE.md.
		//
		// The watermark is still READ here, and its unreadable arm is a refusal in its
		// own right rather than the discarded half of an inheritance: admitting a
		// session over a watermark this broker cannot read leaves its OWN later teardown
		// unable to record a retirement (recordGenRetirement refuses to replace a corrupt
		// standing watermark), so the record would stay MARKED for every future finisher
		// — new state admitted into a store whose teardown path is already known to be
		// unusable.
		if _, _, st := jsa.loadGenRetirement(as.domainTk, hash); st == mqttRetirementCorrupt {
			return nil, false, fmt.Errorf("creating session %q: the delayed-teardown "+
				"watermark for this client is unreadable, so a teardown of this session "+
				"could never record its retirement; stop this account's brokers and "+
				"recreate the broker-owned JetStream state (the $MQTT_* streams)", clientID)
		}
		sess := mqttSessionCreate(jsa, clientID, hash, mqttNewSessGen(), 0, _EMPTY_, _EMPTY_, opts)
		sess.asm = as
		sess.domainTk = as.domainTk
		return sess, false, nil
	}
	// We need to recover the existing record now.
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		return nil, false, fmt.Errorf("unmarshal of session record at sequence %v: %w", smsg.Sequence, err)
	}
	if ps.ID != clientID {
		return nil, false, errMQTTSessionCollision
	}

	// A record carrying a subscription in the reserved "$MQTT.>" namespace has an
	// unsupported schema and is REFUSED here, ahead of every other branch: the
	// teardown adoption below copies durable names straight out of Cons / Drains and
	// derives shared durables from every SharedSubs key (mqttTeardownTargetFromRecord,
	// which this path does not validate), and the overdue discard runs clear(), which
	// does the same while deleting. Refusing first is what keeps the CONNECT path from
	// ever deriving a name from such a record or migrating it in place.
	//
	// It also precedes the GENERATION check below, which is what makes the diagnostic
	// reach the record shape that actually exists: the namespace deny (d222539)
	// predates generation-scoped naming, so a broker old enough to have written a
	// reserved membership wrote no `gen` either, and the generation refusal would
	// otherwise answer every real case with the less specific message. Both refusals
	// are read-only and both prescribe recreating the streams, so the order decides
	// only which one an operator is told about — and naming the offending key is the
	// more actionable of the two. Nothing here reads ps.Gen, and the generation is
	// still validated before anything derives a name from it.
	// See mqttReservedMembershipInRecord.
	if key, found := mqttReservedMembershipInRecord(ps); found {
		return nil, false, fmt.Errorf("restoring session %q: the record contains a "+
			"subscription in the reserved %q namespace (%q), written by a pre-release "+
			"development broker; this format is not supported: stop this account's "+
			"brokers and recreate the broker-owned JetStream state (the $MQTT_* "+
			"streams)", clientID, mqttPrefix, key)
	}

	// A shared group consumer config in Cons is likewise an unsupported shape, refused
	// here for the same reasons and in the same place: the teardown adoption and the
	// overdue discard below both copy durable names straight out of Cons, and the resume
	// would take it for a resident join it never made. See mqttSharedConsInRecord.
	//
	// This one refusal covers the overdue/retired branch further down too, which builds a
	// transient session from the same ps.
	if sid, found := mqttSharedConsInRecord(ps); found {
		return nil, false, mqttSharedConsRecordErr(clientID, sid)
	}

	// A CLAIMED teardown must be FINISHED, never resumed and never overwritten: some
	// broker is (or was) part way through deleting this incarnation's resources, so
	// the record's consumers may already be gone. Finish the deletions, release the
	// record, and start fresh — with a fresh generation, so nothing this session
	// creates can collide with what the previous owner may still be deleting — and
	// Session Present false. See sess_teardown.go.
	// The generation is validated BEFORE anything derives a name from it — the adopted
	// teardown below deletes by generation-scoped name, and it WALKS by generation-scoped
	// filter (the inbound QoS 2 holds; $MQTT_inflight's own filter purge is gone since
	// F-034, replaced by a bounded reap whose enumeration uses the same kind of filter).
	// A malformed stored value would widen those walks across other incarnations, and
	// what each pass then destroys is bounded per F-062 and F-034 rather than blind — but
	// a walk that names the wrong incarnation is still the wrong inventory. Fails closed.
	// See mqttSessGenFromRecord.
	recGen, err := mqttSessGenFromRecord(ps.Gen)
	if err != nil {
		return nil, false, fmt.Errorf("restoring session %q: %w", clientID, err)
	}
	ps.Gen = recGen

	// A shared QoS 2 membership with no affine generation predates the affine path,
	// and is REFUSED here rather than resumed. It has to be caught before the
	// branches below: the teardown adoption derives shared durable names from every
	// SharedSubs key, the overdue discard runs clear() over the same set, and an
	// ordinary resume would REWRITE the record — after which materializeQ2AffOnBehalf
	// reads the empty generation as proof the winner is gone and discards an
	// assignment owed to a delivery already started. The sweep and both expiry paths
	// enforce the same rule, so the record is uninterpretable everywhere rather than
	// only where it is convenient. See mqttValidateRecordAffinePair.
	if aerr := mqttValidateRecordAffinePair(ps, hash); aerr != nil {
		return nil, false, fmt.Errorf("restoring session %q: %w; this record was written by "+
			"a pre-release development broker and its format is not supported: stop this "+
			"account's brokers and recreate the broker-owned JetStream state (the $MQTT_* "+
			"streams)", clientID, aerr)
	}

	if ps.Teardown != nil {
		if err := as.finishAdoptedTeardown(ps, hash, smsg.Sequence, opts); err != nil {
			if !errors.Is(err, errMQTTTeardownAdoptionSuperseded) {
				return nil, false, fmt.Errorf("finishing the claimed teardown of session %q: %w", clientID, err)
			}
			// R1. A newer record holds the subject, and the marker this pass was going
			// to succeed is gone. Re-read rather than returning a base-0 object built
			// from it: that object would create against a record we have just been told
			// exists, carrying lineage derived from a marker that is nobody's base.
			if aerr := rd.admitCycle(); aerr != nil {
				return nil, false, fmt.Errorf("restoring session %q: %w", clientID, aerr)
			}
			goto RESTORE
		}
		sess := mqttSessionCreate(jsa, clientID, hash, mqttNewSessGen(), 0, _EMPTY_, _EMPTY_, opts)
		sess.asm = as
		sess.domainTk = as.domainTk
		// Parentless, as every create now is: this session's first write no longer
		// descends from the record the ADOPTED teardown claimed. The marker's own PWID
		// still names that record, and finishAdoptedTeardown passes it to
		// recordGenRetirement as the PRECONDITION that the record being retired has an
		// identity at all — the watermark no longer stores it. See the create arm above.
		return sess, false, nil
	}

	// THE RETIREMENT FENCE. An unmarked record whose generation is durably RETIRED is
	// the hollow survivor of the claim-won deletion window: some teardown won its
	// claim, recorded the retirement and deleted this generation's consumers, durables,
	// QoS 2 holds and in-flight registers, and an unconditional foreign write then
	// evicted the marker off this subject (the same-base boundary, docs/CONFORMANCE.md:
	// a CAS cannot exclude a write that carries no expectation). Resuming it would answer Session Present 1 for state
	// that no longer exists, so it is discarded instead. This is the only prompt reader
	// for a winner that crashed after the watermark: without it every reconnect
	// re-resumes the same hollow record until the sweep runs.
	//
	// One exact-subject read per record-restoring CONNECT (a fresh create and a
	// marked-record adoption never reach here). A watermark that cannot be read — or
	// that is malformed in any field — REFUSES the CONNECT rather than being ignored:
	// the same fail-closed rule the record load itself follows, and the same rule the
	// writer and the sweep apply. See sess_teardown.go.
	rt, _, rstatus := jsa.loadGenRetirement(as.domainTk, hash)
	switch rstatus {
	case mqttRetirementCorrupt:
		return nil, false, fmt.Errorf("restoring session %q: its durable retirement watermark is malformed, "+
			"so whether this generation was torn down cannot be decided: stop this account's brokers and "+
			"recreate the broker-owned JetStream state (the $MQTT_* streams)", clientID)
	case mqttRetirementIndeterminate:
		return nil, false, fmt.Errorf("restoring session %q: unable to read its durable retirement watermark", clientID)
	}
	genRetired := rstatus == mqttRetirementValid && rt.Gen == ps.Gen
	if genRetired {
		// The discard below is destructive — clear() deletes every durable this record
		// names — and `genRetired` is a NEW route into it, so it does not inherit the
		// overdue arm's pre-existing validation level. A record naming another session's
		// consumer is refused here rather than discarded, exactly as readers 1 and 3
		// refuse to condemn one. See mqttValidateTeardownTargets.
		if verr := mqttValidateTeardownTargets(ps, hash); verr != nil {
			return nil, false, fmt.Errorf("restoring session %q: its generation is durably retired, but the "+
				"record names resources that are not this session's: %w", clientID, verr)
		}
	}

	// An overdue session must not be resumed, even if this reconnect beats the
	// startup sweep to it: discard its state and start fresh. Spec5 [3.1.2.11.2].
	// A retired generation takes the SAME route rather than a second discard shape of
	// its own: build the transient session, clear() it (which re-marks the record,
	// finds the retirement already recorded, repeats the deletions — the inbound QoS 2 hold
	// walk bounded per walk, F-062; the `$MQTT_inflight` reap beside it bounded by its own
	// enumeration watermark, F-034 — and releases the record) and hand back the reset
	// object as a fresh session, with
	// Session Present false — which is the truthful answer in both cases.
	overdue := ps.DisconnectedAt > 0 && ps.ExpiryInterval > 0 && ps.ExpiryInterval != mqttSessionNeverExpire &&
		time.Now().Unix() >= ps.DisconnectedAt+int64(ps.ExpiryInterval)
	if overdue || genRetired {
		sess := mqttSessionCreate(jsa, clientID, hash, ps.Gen, smsg.Sequence, ps.WID, ps.PWID, opts)
		sess.asm = as
		sess.domainTk = as.domainTk
		sess.cons = ps.Cons
		// Already durable: they came out of the record. See voidedRegsRev.
		sess.voidedRegNonces = mqttNonceSet(ps.VoidedRegs)
		sess.acqReleases = mqttAcqReleaseSet(ps.AcqReleases)
		sess.acqReleases = mqttAcqReleaseSet(ps.AcqReleases)
		sess.voidedRegsRev, sess.voidedRegsSavedRev, sess.voidedRegsHandoffRev = 0, 0, 0
		sess.restoreDrainsFromRecord(ps)
		sess.sharedSubs = ps.SharedSubs
		sess.pubRelConsumer = ps.PubRel
		sess.q2AffConsumer = ps.Q2Aff
		sess.q2AffGen = ps.Q2AffGen
		// Wait for the deletes (like the clean-start discard): a queued no-wait
		// delete of the deterministic PUBREL durable could otherwise race the new
		// session recreating it.
		// As in expireSessionFromRecord, the late-uncertainty refusal is UNREACHABLE on
		// this object and gets no recovery: it was just built from the record, this
		// function never returns it resident (only the non-overdue branch below calls
		// addSession) and nothing writes through it before this call, so no save can have
		// armed uncertainty. The refusal is retained as a fail-closed invariant — the
		// CONNECT is refused rather than the session destroyed unclassified.
		if err := sess.clear(false); err != nil {
			if !errors.Is(err, errMQTTClearSuperseded) {
				return nil, false, fmt.Errorf("discarding the unresumable session record of %q: %w", clientID, err)
			}
			// R1, at transition T0. The base this discard claimed is not current, so the
			// object in hand describes a record that is gone; returning it would create
			// at base 0 against whatever superseded it. Nothing to dispose — it is in no
			// map and has no client — so the whole disposition is to read again.
			if aerr := rd.admitCycle(); aerr != nil {
				return nil, false, fmt.Errorf("restoring session %q: %w", clientID, aerr)
			}
			goto RESTORE
		}
		// clear() reset the state (seq included), so sess is now a fresh session.
		return sess, false, nil
	}

	// Restore this session (even if we don't own it), the caller will do the right
	// thing. The incarnation generation comes from the record and is PRESERVED: a
	// resumed session must still find its own PUBRELs and QoS 2 holds, which are
	// named after it. Only a discard re-mints (clear()).
	sess := mqttSessionCreate(jsa, clientID, hash, ps.Gen, smsg.Sequence, ps.WID, ps.PWID, opts)
	sess.asm = as
	sess.domainTk = as.domainTk
	sess.clean = ps.Clean
	sess.subs = ps.Subs
	sess.noLocalSubs = mqttFilterSetFromList(ps.NoLocal)
	sess.rapSubs = mqttFilterSetFromList(ps.RetainAsPublished)
	sess.subIDs = ps.SubIDs
	sess.sharedSubs = ps.SharedSubs
	sess.cons = ps.Cons
	// The restored lists came OUT of the record, so they are already durable there; only
	// a later change reopens the gap.
	sess.voidedRegNonces = mqttNonceSet(ps.VoidedRegs)
	sess.acqReleases = mqttAcqReleaseSet(ps.AcqReleases)
	sess.voidedRegsRev, sess.voidedRegsSavedRev, sess.voidedRegsHandoffRev = 0, 0, 0
	sess.restoreDrainsFromRecord(ps)
	sess.pubRelConsumer = ps.PubRel
	// A persisted affine consumer proves only that one once existed: it may since
	// have been deleted or have drifted from the persisted config. q2AffReady
	// therefore stays FALSE here and is set only by
	// ensureQ2AffineConsumerSubscription, after the delivery subscription is
	// installed and the real consumer is create-or-update verified.
	sess.q2AffConsumer = ps.Q2Aff
	sess.q2AffGen = ps.Q2AffGen
	sess.expiryInterval = ps.ExpiryInterval
	sess.disconnectedAt = ps.DisconnectedAt
	// The BYTES of the record just restored. The parent is nil because this broker did not
	// author this record and holds no copy of its parent's content, which is exactly the case
	// where nothing is repairable. Retaining the bytes rather than a built grant also removes
	// the ordering trap the earlier design had: there is nothing here that could be captured
	// before the hydration above filled Subs, the option sets, Cons and the drains.
	// See sess_fork_repair.go.
	sess.setRestoredGrantBodyLocked(smsg.Data)
	as.addSession(sess, true)
	return sess, true, nil
}
