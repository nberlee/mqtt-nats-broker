// Observing every session-record write, rather than hoping to be told about one.
//
// A session's ownership changes only by a write to its record subject on
// $MQTT_sess, so observing that stream IS observing every ownership change — and a
// session nobody writes about needs no revalidation at all, which is what lets this
// cost nothing per quiet session.
//
// What an observation establishes is that the record MOVED, which makes the resident
// object no longer ours. It does not establish who wrote it or that anyone holds the
// session now, and nothing downstream of here may claim that it does (F-084): the
// displaced client is told the ownership-unsettled 0x89 unless a route that HOLDS the
// successor disposes of it.
//
// WHAT THIS REPLACES, AND WHY IT HAD TO BE REPLACED.
//
// The old channel was a core-NATS subscription on the JetStream reply subject of
// every other broker's session store ($MQTT.JSA.*.SP.*.*). That is an AT-MOST-ONCE
// notification with no retry and nothing durable behind it: when it was lost, this
// broker never learned that the record had moved out from under it. A never-expiring
// session — every MQTT 3.1.1 persistent session — has no expiry timer, the expiry
// sweep skips it, and nothing else re-validates a resident object. So the displaced
// client kept being served indefinitely against a session another broker owned,
// which is the split-brain [MQTT-3.1.4-3] forbids. ("Owned" there is the HAZARD, which
// is real; it is not what a single observation proves.)
//
// THIS CONSUMER IS NOT A DURABLE CHANNEL EITHER, and the design does not pretend
// otherwise. It is an ephemeral AckNone push consumer: its delivery state is not
// quorum-replicated and it runs without flow control, so a leader change, a slow or
// briefly disconnected service connection, or an in-flight socket loss can DUPLICATE
// or DROP a delivery. What the pair of channels guarantees is:
//
//   - duplicates are inert — every watermark advances by max and evictRunning
//     coalesces observations onto one worker per client;
//   - a dropped delivery is recovered by the committed-read backstop in
//     reconcileResidentRecordState, which reads each resident session's record on
//     the periodic pass and feeds the same triggers.
//
// So the watch owns LATENCY and the backstop owns CONVERGENCE. Nothing recreates
// this consumer if it is deleted or inactivity-reaped under a live manager: the
// watch then stays absent and the backstop alone converges, until the next manager
// start provisions a new one. That is an operational property to know, not a
// self-healing one to rely on.
//
// WHAT THE OLD CHANNEL DID FOR FREE, AND HOW IT IS PAID FOR HERE.
//
// The SP subscription never delivered this broker's OWN writes: the reply subject
// carries the writing JSA's id, and the callback filtered on it. A stream watch has
// no such property — it delivers everything, including our own records — and one
// own-write shape is dangerous. A store that LANDED with its response lost arms
// seqUncertain and leaves the acknowledged lineage pair stale; a delivery of that
// very write is strictly past sess.seq, so the eviction worker would classify our
// own bytes against our own stale pair, find them unrelated, and evict the rightful
// client over its own write.
//
// Two things pay for it, and both live in the worker rather than here:
//
//   - before the write settles or arms, the writer's OPEN record transaction refuses
//     the worker's fence (tryBeginRecordTx), so no decision is made mid-write;
//   - after it arms, the digest arm (evictOwnLanding) recognises the record as the
//     attempt's own bytes — the resolver's identity test, reused verbatim, because
//     the digest is the only thing that may say "ours" (sess_fork_lineage.go).
//
// The Origin check below is therefore a SPAWN filter, not a correctness gate: it
// avoids waking a worker for records this broker plainly wrote, and NodeName
// uniqueness is documented rather than enforced, so nothing rests on it.
//
// See docs/ARCHITECTURE.md, "The session-record watch".

package mqtt

import (
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/nats-io/nuid"
)

// mqttTestWatchConsumerCreateFailV forces the session-record watch's consumer create
// to fail, so a test can drive the initialization-failure path repeatedly and assert
// that a failed attempt leaves no ephemeral consumer behind. Package-level because
// the failure has to happen before any account manager exists to hang a seam on.
// Nil in production.
var mqttTestWatchConsumerCreateFailV atomic.Pointer[bool]

// mqttTestConsumerCreateResultHookV lets a test turn a SUCCESSFUL ephemeral-consumer
// create into a lost response: the consumer exists on the server and the caller sees
// an error it cannot classify. That is the window a rollback keyed on "confirmed
// creates only" would miss. Nil in production.
var mqttTestConsumerCreateResultHookV atomic.Pointer[func(stream, name string, err error) error]

// setMQTTTestConsumerCreateResultHook arms that seam and returns its restore function.
func setMQTTTestConsumerCreateResultHook(h func(stream, name string, err error) error) func() {
	prev := mqttTestConsumerCreateResultHookV.Load()
	mqttTestConsumerCreateResultHookV.Store(&h)
	return func() { mqttTestConsumerCreateResultHookV.Store(prev) }
}

// setMQTTTestWatchConsumerCreateFail arms the seam and returns its restore function.
func setMQTTTestWatchConsumerCreateFail(fail bool) func() {
	prev := mqttTestWatchConsumerCreateFailV.Load()
	mqttTestWatchConsumerCreateFailV.Store(&fail)
	return func() { mqttTestWatchConsumerCreateFailV.Store(prev) }
}

// mqttSessRecordWatchInactiveThreshold is how long the watch consumer survives with
// no interest, i.e. after the broker that created it dies. Same value and same
// reasoning as the retained-messages consumer: long enough to ride out a reconnect,
// short enough that a dead broker's consumer does not accumulate on the stream.
const mqttSessRecordWatchInactiveThreshold = 5 * time.Minute

// mqttSessRecordWatchConsumerConfig builds this broker's watch consumer.
//
// DeliverNew, unlike the retained-messages consumer's DeliverAll: a resident session
// can only exist on this broker after the account manager (and therefore this
// consumer) exists, so no historical record can concern a resident, and replaying the
// whole stream at every manager start would drive one no-local expiry reconcile per
// record ever written. Records that predate the manager are the expiry sweep's
// business, exactly as before.
//
// The filter names the record space exactly: domainTk carries its trailing dot when
// a domain is configured, a domain is one token and a client-ID hash is one token, so
// a single trailing wildcard matches every record subject and nothing else.
//
// AckNone because nothing here acknowledges work — the observation's only job is to
// trigger a decision that re-reads authoritative state for itself.
func mqttSessRecordWatchConsumerConfig(domainTk, deliverSubject string) *CreateConsumerRequest {
	return &CreateConsumerRequest{
		Stream: mqttSessStreamName,
		Config: ConsumerConfig{
			Name:              mqttSessStreamName + "_watch_" + nuid.Next(),
			FilterSubject:     mqttSessStreamSubjectPrefix + domainTk + "*",
			DeliverSubject:    deliverSubject,
			ReplayPolicy:      ReplayInstant,
			DeliverPolicy:     DeliverNew,
			AckPolicy:         AckNone,
			InactiveThreshold: mqttSessRecordWatchInactiveThreshold,
		},
	}
}

// processSessionRecordDelivery is the watch's delivery callback: one session record,
// as written by any broker in the account (this one included).
//
// It runs on the account's subscription delivery goroutine, which every later
// delivery shares, so it performs NO JetStream request — the decode below is CPU-only
// and every read the decision needs happens on a spawned worker. That is the same
// head-of-line constraint the old SP callback had, for the same reason.
//
// Runs on the JSA delivery goroutine. No lock held on entry.
func (as *mqttAccountSessionManager) processSessionRecordDelivery(_ *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	cIDHash := mqttSessRecordHashFromSubject(as.domainTk, subject)
	if cIDHash == _EMPTY_ {
		return
	}
	// Checked before anything else, so a dropped observation touches neither
	// watermark: what it reproduces is a LOST delivery, which this channel really can
	// suffer (AckNone, no flow control) and which the backstop is what recovers.
	as.mu.RLock()
	drop := as.testSessPersistDrop
	as.mu.RUnlock()
	if drop != nil && drop(cIDHash) {
		return
	}
	// The stream sequence comes from the delivery metadata, never from the record
	// body: the body is attacker-influenced in the sense that any broker writes it,
	// while the sequence is the stream's own statement about this message.
	seq, _, _, _, _ := ackReplyInfo(reply)
	if seq == 0 {
		return
	}
	_, msg := pc.msgParts(rmsg)
	// A record we cannot decode still counts as an observation. Returning here would
	// discard the trigger for exactly the records most likely to matter, and the
	// worker already has an unusable-record arm that decides at the observation level
	// with the conservative vocabulary. What a failed decode costs is only the
	// own-write spawn filter below.
	var ps *mqttPersistedSession
	if len(msg) > 0 {
		decoded := &mqttPersistedSession{}
		if err := json.Unmarshal(msg, decoded); err == nil {
			ps = decoded
		}
	}
	as.deliverSessionRecordObservation(cIDHash, seq, ps)
}

// mqttSessRecordHashFromSubject returns the client-ID hash a session-record subject
// names, or empty when the subject is not one.
//
// Written against the subject the record is STORED on rather than parsed by token
// position, because the domain token is optional and carries its own separator: with
// a domain the subject is "$MQTT.sess.<domain>.<hash>", without one it is
// "$MQTT.sess.<hash>", and the prefix comparison covers both without counting tokens
// (a count is what a future wildcard-depth change would silently invalidate).
func mqttSessRecordHashFromSubject(domainTk, subject string) string {
	prefix := mqttSessStreamSubjectPrefix + domainTk
	if len(subject) <= len(prefix) || subject[:len(prefix)] != prefix {
		return _EMPTY_
	}
	hash := subject[len(prefix):]
	// Exactly one trailing token: a hash contains no separator, and admitting one
	// would let a deeper subject be attributed to a client it does not name.
	for i := 0; i < len(hash); i++ {
		if hash[i] == btsep {
			return _EMPTY_
		}
	}
	return hash
}

// deliverSessionRecordObservation is the observation half of the old
// processSessionPersist, kept as its own function so the backstop can feed the very
// same triggers a delivery does — one code path decides what an observation means,
// whichever channel carried it.
//
// ps is the decoded record when it decoded, nil otherwise; it is used only for the
// own-write spawn filter.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) deliverSessionRecordObservation(cIDHash string, seq uint64, ps *mqttPersistedSession) {
	as.mu.Lock()
	defer as.mu.Unlock()
	// The delivery client has no server ref, so use the account JSA client's.
	s := as.jsa.c.srv
	if s == nil {
		return
	}
	// Advance the per-client reconciliation watermark for this observation before
	// choosing the branch, so a newer observation supersedes an older in-flight
	// expiry-reconciliation worker. The EVICTION trigger is deliberately not advanced
	// here: it moves only on the resident branch below, so an observation that arrives
	// with no session resident can never become a lingering eviction worker's license
	// against a re-added object. Deliveries are serialized on one goroutine but need
	// not arrive in stream-sequence order, so advance with a max.
	if st := as.reconcileWM[cIDHash]; st != nil && seq > st.latest {
		st.latest = seq
	}
	sess, resident := as.sessByHash[cIDHash]
	if !resident {
		// No local session: reconcile this session's expiry deadline from the
		// authoritative persisted record on a spawned, ADMITTED worker. Re-reading the
		// record is a synchronous JSA request that must not run on the delivery
		// goroutine. Admission through the Will dispatch gate keeps the worker tracked
		// and drained before shutdown tears down the JSA; refusal means shutdown Phase
		// 2 closed the gate, in which case we skip. Spec5 [3.1.2.11.2].
		if as.reconcileWM == nil || !s.admitWillWorker() {
			return
		}
		st := as.reconcileWM[cIDHash]
		if st == nil {
			st = &sessionReconcileState{}
			as.reconcileWM[cIDHash] = st
		}
		if seq > st.latest {
			st.latest = seq
		}
		// Incremented synchronously, in delivery order, before the go: a newer
		// observation must not complete and prune this entry before the worker runs.
		st.inFlight++
		go func() {
			defer s.willWorkerWG.Done()
			as.reconcileRemoteSessionExpiry(s, cIDHash, seq, st)
		}()
		return
	}
	// Cheap synchronous filter only — strictly below, NOT at, sess.seq: the worker
	// refuses equality under the fence (its own strict `Sequence > sess.seq`), and an
	// observation AT sess.seq must still spawn it so tests can assert that the refusal
	// was DECIDED through the completion seam rather than skipped here.
	sess.mu.Lock()
	ignore := seq < sess.seq
	sess.mu.Unlock()
	// The own-write SPAWN filter. A stream watch delivers this broker's own records,
	// which the JSA-reply channel never did, and a delivery routinely arrives before
	// the writer's own transaction has adopted the sequence — so without this every
	// local save would wake a worker that can only conclude "stale" once the fence
	// frees. It is keyed on Origin ALONE, deliberately: the sequence cannot
	// distinguish "our write, not yet adopted" from "a peer's write", which is the
	// case that has to be fast.
	//
	// NodeName uniqueness is documented rather than enforced, so a collision would
	// make this skip a genuine peer's write. That costs nothing durable because the
	// filter is NOT on the convergence path: reconcileResidentOwnership passes no
	// record at all, so the backstop's own committed read re-triggers whatever this
	// skipped. Latency is the only thing at stake here.
	if ignore || (ps != nil && ps.Origin == as.jsa.id) {
		return
	}
	if as.reconcileWM == nil {
		// Shutdown nilled the map: dropping the observation is the shutdown contract.
		return
	}
	if st := as.reconcileWM[cIDHash]; st != nil {
		// The EVICTION trigger advances only here, on the resident branch.
		if seq > st.evictLatest {
			st.evictLatest = seq
		}
		if st.evictRunning {
			return
		}
	}
	// Admission refused means shutdown Phase 2 closed the Will dispatch gate; skip.
	// Checked before any state is created so refusal needs no undo.
	if !s.admitWillWorker() {
		return
	}
	st := as.reconcileWM[cIDHash]
	if st == nil {
		st = &sessionReconcileState{}
		as.reconcileWM[cIDHash] = st
	}
	if seq > st.latest {
		st.latest = seq
	}
	if seq > st.evictLatest {
		st.evictLatest = seq
	}
	// Flag and increment set synchronously, in delivery order, before the go: the flag
	// coalesces the next observation onto this worker, and the increment keeps a
	// concurrent no-local worker's prune from deleting the entry before it runs. The
	// worker clears both on every exit path.
	st.evictRunning = true
	st.inFlight++
	go func() {
		defer s.willWorkerWG.Done()
		as.evictDisplacedOnRemotePersist(s, cIDHash, st)
	}()
}
