# Architecture

```text
        MQTT clients (3.1.1 / 5.0)
     │        │        │        │
     ▼        ▼        ▼        ▼
┌──────────┐      ┌──────────┐        (stateless frontends,
│ bridge #1│ ...  │ bridge #N│         scale horizontally)
└────┬─────┘      └────┬─────┘
     │  nats.go (service conn + per-client auth-passthrough conns)
     ▼                 ▼
┌─────────────────────────────────────┐
│      NATS / JetStream cluster        │   (replicated → HA state:
│  sessions · retained · QoS1/2 · pub  │    sessions, retained msgs,
└─────────────────────────────────────┘    QoS state, message routing)
```

## Why standalone

NATS server ships MQTT **3.1.1** embedded. The **MQTT 5.0** work this project is
built on will not be merged upstream, so instead of maintaining a fork of the
whole server, the MQTT layer runs as a separate service that uses an external
NATS/JetStream cluster as its backend.

## Compatibility boundary

The bridge is **pre-release** and is being designed for its first public
release. Its internal persisted MQTT state and `$MQTT_*` JetStream stream
schemas do not have a backward-compatibility contract yet. During this phase,
schema changes may require development streams to be recreated; designs should
not carry migration, legacy-format, or mixed-version rolling-upgrade machinery
unless a concrete released contract is identified first.

The backend support target is the **latest JetStream** represented by the
current `nats-server/v2` dependency in `go.mod` and exercised in CI. The bridge
does not maintain alternate code paths for older JetStream APIs or semantics.
When the dependency advances, plans may adopt the new current behavior directly.

The floor is **nats-server 2.14**. Older releases are unsupported outright: there
are no fallbacks, and where a behavior below is load-bearing it is validated at
startup and failed closed rather than degraded.

This boundary applies only to bridge persistence and its JetStream backend. MQTT
3.1.1 and 5.0 wire-protocol compatibility remains a product requirement and is
covered by the conformance suites.

### JetStream behaviors this design depends on

Correctness rests on these specific semantics, each stated as the contract the
code assumes. This is the list a version bump has to re-verify — it is what makes
the "no fallbacks" stance reviewable rather than implicit.

- **Per-subject CAS** (`Nats-Expected-Last-Subject-Sequence`). A store carrying it
  is accepted only if the subject's last sequence matches, giving every state
  machine its linearization point — **within a leader term**. The check is made
  before the proposal and never re-made at apply (`canConsistencyCheck :=
  !isClustered`, `server/stream.go`), so a transition that clears the in-process
  map while a replicated proposal survives can admit a second same-base write and
  apply both. Every arbitration in this broker rests on that contract, so the
  window belongs to all of them, not to any one writer; `docs/CONFORMANCE.md`
  states it as a residual. The window itself cannot be closed here — production
  NATS is external, and the only apply-time-rechecked precondition
  (`Nats-Expected-Last-Msg-Id`) compares a stream-wide value — so `$MQTT_sess`
  carries a **parent pointer** instead, which makes the outcome *recognisable*
  rather than preventable (see "Recognising a forked session record" below). The
  other arbitrated streams have no such identity yet; each has its own roadmap
  bullet. A rejection is reported structurally
  (`JSStreamWrongLastSequenceErrF` 10071, or 10164 under clustered inflight
  contention). 10164 means *transient contention*, not "the subject moved", so
  every rejection is resolved by re-reading rather than classified directly.
- **`MaxMsgsPer: 1`** turns a subject into a single-slot register: a new record
  evicts the old one, so a stream sequence names a record, never a subject.
- **Interest retention.** A store with no matching consumer interest **succeeds and
  keeps nothing** — the server skips the message, still records its dedupe entry,
  and returns an *ordinary* success acknowledgment carrying a sequence that holds
  nothing. A publish response therefore never proves a record exists; only delivery
  or a presence check at the returned sequence does. Correspondingly, deleting a
  consumer deletes the messages it still held, so "absent" never proves
  "delivered".
- **Deduplication entries outlive their messages.** `deleteMsg` does not clear a
  `Nats-Msg-Id` entry, so re-storing under the same id inside the window can be
  accepted as a duplicate that stores nothing. Anything that must exist is verified
  by presence, and identities that need to be re-attempted carry an attempt
  counter.
- **`PubAck.Duplicate`** flags a deduplication hit. Useful diagnostically; it does
  not remove the need for the presence check, because a no-interest skip is an
  unflagged success.
- **Per-message TTL** and **JetStream double ack** (`AckPolicy` explicit with a
  confirmed acknowledgment), used where a cleanup must be known to have landed.
- **Consumer inactivity limits.** `InactiveThreshold` gives a durable a delete
  timer; a stream-level `ConsumerLimits.InactiveThreshold` imposes one that a
  consumer cannot opt out of. Where a durable must survive an offline session, the
  stream is checked and the feature is refused rather than silently reaped.

### Stream prerequisites

Every broker-owned `$MQTT_*` stream is provisioned through one convergence shape
(`ensureBrokerStream`, `internal/mqtt/stream_prereqs.go`): create-or-lose, then an
**unconditional authoritative re-read**, then a per-stream validator over that
re-read — so a create-race loser (and equally the winner, whose create response is
not authoritative for what now exists) validates the stream that is actually
there, never the config it asked for. The nine streams are entries in one ordered
provisioning table iterated by a single loop, so none can be provisioned outside
the shape. A stream that fails its validator **fails account-manager
initialization** with recreate guidance; nothing is repaired or updated in place,
because a store another broker may still be serving must not be mutated under it
(see "Compatibility boundary").

Each validator's list is **derived from what that stream's owner actually does**
— the stores, CAS writes, deletes, purges, consumers and scans in its feature
file — never copied from another stream's list. A setting absent from a list is
an audited *non-prerequisite* with a written reason, not an oversight: rejecting
a setting no owner operation depends on would refuse legitimate deployments.
`TestStreamValidatorFieldCoverage` enforces the audit — every `StreamConfig`
field carries exactly one disposition (rejected, waived, or dominated — the last
meaning its only enabling configuration is already rejected by a named check) for
every validator.

A shared shape covers what is a prerequisite of *every* owner because it is a
prerequisite of storing and reading a record at a named subject at all: the exact
subject space, file storage, no mirror, the configured replica count, not sealed,
no subject transform, no republish, not a counter stream, acknowledgements on,
no rollup headers admitted (`AllowRollup` lets any writer with publish
permission purge up to the whole stream in one `Nats-Rollup` message — the same
evict-live-state hazard class as the global limits every owner rejects, but
attacker-triggered), and the default persistence mode. Everything else is
per-owner:

| Stream | Owner does | Prerequisites beyond the shared shape | Audited non-prerequisites (reason) |
|---|---|---|---|
| `$MQTT_sess` | per-subject record CAS, `loadLastMsgFor`, filtered scans, exact-sequence confirmed deletes (teardown release, expiry, rollback); no purge; **one ephemeral `AckNone` session-record watch consumer per broker** (see "The session-record watch"). Every record carries `WID`/`PWID`, the write's own identity and its parent's | `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; **`DenyDelete` fatal** (the teardown delete is a record's only exit; nobody resumes a marked record, so a denied delete locks the client ID out permanently); no `Sources`; no `SubjectDeleteMarkerTTL`; a **valid incarnation epoch** in `Metadata` (validated on the same snapshot; unrelated keys accepted); **`MaxConsumers` unlimited** (one watch consumer per live broker, so a finite cap eventually rejects a legitimate broker start — note a configured `0` normalizes to unlimited) | `DenyPurge` (nothing purges); `ConsumerLimits.*` (a stream `InactiveThreshold` below the watch's requested 5 minutes, or any positive `MaxAckPending` against `AckNone`, already fails the consumer create at initialization, naming the consumer — as on `$MQTT_rmsgs`); `AllowMsgTTL` (no `Nats-TTL` header sent) |
| `$MQTT_willq` | create-only CAS enqueue per WillID, state-advance CAS, filtered scans, exact-sequence deletes (confirmed terminal, no-wait scan cleanup); no purge, no consumers | as `$MQTT_sess` minus the epoch; **`DenyDelete` fatal** with the accumulation rationale: per-WillID subjects mean deletion is the only removal, so denying it grows the outbox monotonically and every scan re-walks every Will ever fired (`done` precedes the delete, so nothing re-fires) | `DenyPurge`; `MaxConsumers`/`ConsumerLimits`; `AllowMsgTTL` |
| `$MQTT_willcancel` | per-client watermark CAS and reads — **nothing else, ever** | `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; no `Sources` | **`DenyDelete`/`DenyPurge`** (no operation deletes or purges — markers are permanent by design); `SubjectDeleteMarkerTTL` (no delete means no marker can ever be created); `MaxConsumers`/`ConsumerLimits` |
| `$MQTT_msgs` | plain stores with per-message TTL; durable, drained and shared queue consumers, each with an explicit `MaxAckPending`; consumer deletes; `loadMsg` by sequence; **no message delete, no purge** | **`InterestPolicy`**; `MaxMsgsPer` unlimited; no global limits; no `MaxMsgSize` cap (a review round proposed a threshold at the broker's max payload, since the PUBLISH admission preflight bounds the complete stored form; disproven — QoS 1/2 Will stores bypass that preflight, so no positive cap is provably inert); **`AllowMsgTTL` required when MQTT 5.0 is accepted** (the default; a `max_protocol_version: 4` broker never emits `Nats-TTL`, so it waives the field); no `Sources`; no `SubjectDeleteMarkerTTL`; `MaxConsumers` unlimited (one durable per subscription/group/drain); `ConsumerLimits.MaxAckPending` fatal when non-zero and **below** the broker's effective cap | `DenyDelete`/`DenyPurge` (no delete or purge exists); `Discard` (inert with every limit unlimited); **`ConsumerLimits.InactiveThreshold`** (runtime refusal by the drain and shared/affine guards — see below); `Duplicates` (best-effort narrowing only) |
| `$MQTT_rmsgs` | per-topic store with TTL incl. empty-body tombstones (the durable deletion authority); `loadLastMsgForMulti`/`loadNextMsgFor`; **no explicit message-delete or purge API calls**; one ephemeral `AckNone` consumer per broker | `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; **`AllowMsgTTL` required when MQTT 5.0 is accepted** (as on `$MQTT_msgs`); no `SubjectDeleteMarkerTTL`; `MaxConsumers` unlimited (one ephemeral consumer per broker) | **`DenyDelete`** (the owner issues no delete API request at all; the tombstone plus `MaxMsgsPer: 1` store-time replacement carry correctness, and both that eviction and TTL expiry are store-internal reaping `DenyDelete` does not gate); `DenyPurge`; **`Sources`** (supported cross-account retained topology); `ConsumerLimits.*` (a hostile limit already fails the consumer create at initialization, naming the consumer) |
| `$MQTT_qos2in` | hold/claim CAS at `<idHash>.<gen>.<PI>` (+ `.rel` sibling), exact-sequence deletes, confirmed exact-subject hold purge (discard + orphan sweep), bounded walk | `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; `DenyDelete` fatal; **`DenyPurge` fatal** (blocks the confirmed exact-subject hold purge); no `Sources`; no `SubjectDeleteMarkerTTL` | `MaxConsumers`/`ConsumerLimits` (no consumers); `AllowMsgTTL`; `Duplicates` |
| `$MQTT_qos2assign` | create-only claim CAS, transition CAS, exact-sequence reap; no purge, no consumers | as `$MQTT_qos2in` but `DenyPurge` waived | `DenyPurge` (nothing purges by subject); `MaxConsumers`/`ConsumerLimits`; `AllowMsgTTL` |
| `$MQTT_inflight` | create-only register CAS, exact-sequence deletes, per-incarnation filter purge (teardown), bounded walk; no consumers | as `$MQTT_qos2in` (`DenyDelete` and `DenyPurge` both fatal) | `MaxConsumers`/`ConsumerLimits`; `AllowMsgTTL` |
| `$MQTT_out` | append-only stores (one a create-only CAS), per-session PUBREL + affine durables, interest-based drop; **no message delete, no purge** | **`InterestPolicy`**; `MaxMsgsPer` unlimited; no global limits; no `MaxMsgSize` cap; no `Sources`; no `SubjectDeleteMarkerTTL`; `MaxConsumers` unlimited (two durables per session); **`ConsumerLimits.InactiveThreshold` fatal** (it would reap a disconnected session's affine consumer); `ConsumerLimits.MaxAckPending` fatal when non-zero and below the effective cap | `DenyDelete`/`DenyPurge` (interest retention removes messages); `Discard` (inert); `AllowMsgTTL`; `Duplicates` (best-effort narrowing only) |

Three asymmetries in that table are deliberate, and each is pinned by an
acceptance test so an over-strict "fix" fails:

- **`DenyDelete` is fatal on `$MQTT_sess`/`$MQTT_willq` and waived on
  `$MQTT_willcancel`/`$MQTT_rmsgs`** — the former have deletion as a record's
  only exit; on the latter the owner issues no explicit delete API request for
  correctness. That is not the same as "nothing is deleted": on `$MQTT_rmsgs`
  the `MaxMsgsPer: 1` store-time eviction IS the replacement semantics, and TTL
  reaping drops expired records — both store-internal, neither gated by
  `DenyDelete`.
- **`ConsumerLimits.InactiveThreshold` is fatal on `$MQTT_out` but waived on
  `$MQTT_msgs`** — plain subscriptions already run under the broker's own
  inactive-threshold option there and are verified (or recreated) on every
  resume (see "Plain QoS 1/2 consumer reconciliation on resume"), and the
  paths that cannot tolerate a stream-forced limit (the UNSUBSCRIBE drain,
  shared/affine consumers) refuse it at **runtime**, which also covers a limit
  imposed after startup — a startup check could not (see
  `docs/CONFORMANCE.md`).
- **`Sources` is waived on `$MQTT_rmsgs` only** — sourcing is additive, so it is
  fatal exactly where a foreign record could occupy a protocol CAS slot, and
  cross-account retained sourcing is a supported topology.

Startup validation is a **snapshot**: a stream config mutated after
initialization is not re-checked until the next process start or manager
recreation. The runtime guards above and JetStream's own consumer-create
rejection (for `MaxAckPending`) are the only live enforcement; the residual is
recorded in `docs/CONFORMANCE.md`.

## High availability

- The bridge is **stateless**: all durable state (sessions, retained messages,
  QoS1/2 in-flight, subscriptions' persistence) lives in JetStream.
- Run **N** bridge instances behind a TCP load balancer. A client can reconnect
  to any instance; **session takeover** works across instances because it is
  mediated by a NATS-message mechanism, not in-memory state.
- HA/durability guarantees come from JetStream **replicas** (`R3`/`R5`). Size the
  NATS cluster, not the bridge.

### Two distinct handoff mechanisms

These are independent and easy to conflate:

- **Bridge-replica handoff** — a *bridge* instance dies; another bridge picks up
  the work. Any bridge can serve any client and claim any durable task because all
  state is in JetStream. This works even against a single-node JetStream.
- **JetStream R3 leader failover** — a *NATS* node dies. The stream's Raft group
  elects a new leader among the surviving replicas and continues committing, as
  long as quorum survives. This is a property of the NATS cluster, not the bridge.

The **delayed-Will outbox** (`$MQTT_willq`, MQTT 5.0 Will Delay with a surviving
session) exercises both: a Will enqueued on one bridge is fired by a surviving
bridge (bridge-replica handoff), and its claim/publish/delete lifecycle keeps
committing across a NATS-node loss (R3 leader failover). The guarantee is
**at-least-once** *once the outbox record is durably acknowledged and while quorum
and storage remain available*: per-subject JetStream CAS serialises each state
transition, so a recovered duplicate is possible. It is not unconditional no-loss —
QoS 0 is best-effort, a sustained JetStream outage can defeat the immediate-publish
fallback, and a forced process kill can interrupt persistence before the record is
acknowledged. Both mechanisms are
regression-tested under the `mqttport_fulltests` tag — the shared-store simulation
in `will_outbox_test.go` and the real 3-node cluster in
`jetstream_cluster_harness_test.go` (see [CONTRIBUTING.md](../CONTRIBUTING.md)).

Two refinements keep the guarantee tight:

- **Durable termination.** A successful publish is committed `publishing -> done`
  (retried against the record's latest sequence) *before* any cleanup; only then is
  the record deleted (at the `done` sequence). Committing `done` first means a failed
  delete cannot leave a re-fireable `publishing` record — the scanner reaps the
  `done` record instead.
- **Takeover fence (durable cancellation watermark).** The primary suppression is a
  permanent per-client marker on the dedicated `$MQTT_willcancel` stream
  (`MaxMsgsPer:1`, no age/size limits). Every accepted CONNECT advances the client's
  marker to its session save sequence — a *gating* write: no success CONNACK is sent
  until it lands (and a failed advance runs a rollback transaction that rejects the
  CONNECT). Sequences are comparable only within one `$MQTT_sess` incarnation,
  identified by a generated epoch (`nuid:<NUID>`) stored in the session-stream metadata
  and carried on both the marker (`SessEpoch`) and each Will (`OriginSessionSeq`,
  `OriginSessEpoch`). The claimant fence suppresses a Will when a current-epoch marker
  is *strictly beyond* its origin sequence (`SessionSeq > OriginSessionSeq` — the
  connection's own delayed Will, at an equal or higher origin, still fires), or when the
  marker is from a different incarnation. Because the marker is permanent, this holds
  even after the newer connection finite-disconnects or its session record is deleted —
  the residual a session-record fence alone could not close. When no marker is present
  (administrative loss), the fence falls back to the numeric session-record rule
  (suppress iff a newer record with `DisconnectedAt == 0`), and a marker-stream outage
  defers pending Wills (never lost) until it recovers. Because that fallback reads
  `DisconnectedAt == 0` as *a live newer connection*, a disconnected session's record
  has to say it is disconnected whatever its expiry class. The finite close persists the
  stamp with its disconnect save; a **never-expiring** session makes no such save, so
  its close stamps the disconnect time inside a RECORD TRANSACTION of its own — acquired
  NON-BLOCKING, and a close whose acquisition is REFUSED stamps anyway, unordered, because
  the rejection probe would otherwise persist current memory with `DisconnectedAt = 0`
  at a newer sequence — before the detach and therefore before the shared purge's own
  record write. The fence, not `sess.mu`, is what makes that effective: an off-readLoop
  writer takes the fence without the ownership gate, so a body it had already marshalled
  with `DisconnectedAt = 0` could otherwise land after the Will's origin sequence is
  captured. Holding the fence means every earlier marshal has completed and advanced the
  sequence that capture reads, and every later write on that session (the purge, a
  void-verdict release handoff, a rejection probe) therefore publishes a record newer
  than an owed Will's origin sequence that still reads as disconnected, instead of
  cancelling a Will the client is owed. The field stays inert for every expiry reader,
  each of which pre-filters on never-expire before looking at it. Refusal has two causes
  and the same cost. An OPEN (or pre-admitted) transaction leaves the residual above: the
  one body already marshalled inside it, which no non-blocking form can order against.
  Waiter priority refuses on a FREE fence reserved for an announced blocking writer, where
  nothing is open yet — but that writer can then take `sess.mu`, open its transaction and
  marshal `DisconnectedAt = 0` before this stamp is taken, which is the same one-body
  exposure reached from the other side. This is the one non-blocking site that does not
  retry, so waiter priority gives its degraded arm more occasions of that residual, not a
  cost-free arm; `docs/CONFORMANCE.md` residual 13 carries it.

Residual that keeps literal exactly-once impossible:

- *At-least-once duplicate* — a crash between the external publish and the `done`
  commit republishes after the lease expires (the sanctioned duplicate).

The reconnect-suppression miss is now closed by the durable watermark; it reopens
only under administrative loss of `$MQTT_willcancel` (a documented degradation, not a
supported migration). This is a first-release storage format: pre-Layer-2 in-record
Wills, migrated outbox records, and a `$MQTT_sess` stream without a valid generated
epoch are unsupported and rejected with recreate guidance rather than carried forward.

**Refusal of an unsupported outbox record.** That rejection is a mechanism, not a
policy statement. Every decoded `$MQTT_willq` record is validated
(`validateWillOutboxRecord`): a well-formed incarnation generation, a nonzero
`OriginSessionSeq`, a well-formed `OriginSessEpoch`, a known state, `WillID` and
`ClientIDHash` that are each a nonempty **single subject token**, and the binding
that those two identities name the subject the record is stored on. All three
decode boundaries validate — the point loader, the claim scan, and the cancellation
scan — so nothing can act on a record that does not satisfy them. The enqueue
validates before persisting and falls to the immediate-publish fail-safe rather
than write one; consequently the claimant fence is handed only validated records
and does not re-check their origin.

Two of those checks answer failure modes the obvious ones do not. Body validation
alone would admit a well-formed record carrying another client's identities stored
on this client's subject: it would be fenced with the *other* client's watermark and
session state, then claimed and published here, and `finalizeWillPublish`'s `WillID`
equality would not catch it (that detects a post-claim replacement, not a body that
never matched its subject). And subject equality alone would admit a dotted
identity: NATS parses these into subject tokens, so a `WillID` of `a.b` on its
exactly-matching subject is visible to the account scan (`<domainTk>>`) but
**invisible** to the reconnect-cancel filter (`<domainTk><cidHash>.*`, exactly one
trailing token), and could fire after a reconnect entitled to suppress it.

A record that fails is **refused**: never claimed, never published, never
cancelled, and never deleted. Deletion is withheld deliberately — it is a mutation
of state the bridge has just declared it cannot interpret, and unlike a
retained-message cache entry the outbox record is the only durable copy of the
Will. Refusal is read-only, so a refused record is **reclaimed by nothing**: no
sweep, fence or reaper will ever remove it. It is logged once per subject (Warn,
then Debug — the claim scan meets it every 5s) with the only remedy there is,
deleting that message from `$MQTT_willq` or recreating the stream. A refused record
does not make a cancellation pass unresolved — no fire path will act on it either —
so a reconnect still completes cleanly, and it never contends for a live Will's
subject slot, because every enqueue mints a fresh WillID.

**Operational residual: refused records are permanent, and permanence has a
running cost on two paths.** Both scans walk their filter serially with one
JetStream message-get per matching record, and neither can skip a record it refuses
— refusing it is what the walk is for.

- *The claim scan.* `scanWillOutbox` restarts from sequence 1 each interval, so `N`
  refused records add `O(N)` requests to every scan on every broker, on every
  interval, indefinitely. A large enough backlog can push a scan past
  `mqttWillClaimInterval` and **delay valid due Wills**.
- *The reconnect path, which is worse because a client waits on it.*
  `cancelWillOutboxOnce` walks one client's filter, and `mqttProcessConnect` calls
  `cancelWillOutbox` synchronously **before the CONNACK**. So `N` refused records
  for one client cost `O(N)` serial round trips on **every reconnect of that
  client, forever**. Returning `nil` on a refused record avoids the retry loop and
  the spurious error log; it does not avoid the latency. A large enough per-client
  backlog degrades that client's connect time and can make it effectively unable to
  connect — an availability cost, not merely a delayed Will.

This is the same accumulation hazard the `DenyDelete` rationale states for the
stream as a whole, arriving by the other route: there, deletion is denied; here, the
bridge declines to delete what it cannot read. The refusal is therefore not a stable
resting state to be left alone — the diagnostic asks for prompt cleanup, and an
operator who leaves refused records in place is accepting a growing scan cost and a
growing connect cost for the affected client. Deliberately no quarantine stream, no
bounded scan and no reaper: each would be a second mechanism deciding the fate of
records the bridge has already said it cannot interpret.

Operational constraints for the marker:

- **Back up, restore, delete, or recreate `$MQTT_sess` and `$MQTT_willcancel` as one
  logical unit.** Recreating only `$MQTT_willcancel` loses suppression evidence and
  reopens the residual until each affected client reconnects.
- **Recreating `$MQTT_sess` requires restarting every bridge serving the account
  before MQTT traffic resumes.** The incarnation epoch is captured once per account
  manager (`as.sessStreamEpoch`); a live manager that outlives stream recreation
  retains the stale epoch and stale session-CAS state. A process restart recreates the
  manager and adopts the new epoch.
- **Retention is intentionally unbounded**: one permanent marker per distinct client
  ID ever seen (no `MaxAge`). Monitor `$MQTT_willcancel` message count and storage;
  administrative pruning weakens the guarantee for the pruned client IDs.
- **The full marker-stream configuration is validated at startup** (see "Stream
  prerequisites"): `validateWillCancelStream` covers the write/CAS-affecting
  settings (`NoAck`, `Sealed`, `SubjectTransform`, `MaxMsgSize`, …) beyond the
  state shape. `DenyDelete`/`DenyPurge` remain deliberately unchecked — no
  operation ever deletes or purges a watermark, so they are audited
  non-prerequisites, not oversights.

### The session-record watch

Ownership of a session changes only by a write to its record subject on `$MQTT_sess`,
so **observing that stream is observing every ownership change** — and a session
nobody writes about needs no revalidation at all. That is what lets convergence cost
nothing per quiet session, which the rejected alternatives could not: a periodic READ
per resident session proves nothing when it finds the record unmoved (10164 is
returned while a write is in process upstream), and a periodic CAS would be a write
per session per interval.

What this replaced was an at-most-once core-NATS notification on the JetStream reply
subject of every other broker's session store (`$MQTT.JSA.*.SP.*.*`). Lost meant lost:
a never-expiring session — **every** MQTT 3.1.1 persistent session — has no expiry
timer, the expiry sweep skips it, and nothing else re-validated a resident object, so
a displaced client kept being served against a session another broker owned, which is
the split-brain [MQTT-3.1.4-3] forbids.

**The watch is not a durable channel either, and the design does not claim it is.** It
is one uniquely-named ephemeral push consumer per broker per account
(`AckNone`, `ReplayInstant`, `InactiveThreshold` 5 minutes, delivered to a
`$MQTT.sub.<nuid>` subject on the **service** connection so the recommended client ACL
still holds), created only once EVERY stream in the provisioning table has been
created and validated.

That ordering is a leak fix, not a preference. Both per-broker ephemeral consumers —
this one and the retained-messages consumer — are uniquely named, so nothing later
adopts or replaces one; manager initialization tears down only local subscriptions;
and every MQTT connection retries the whole initialization. A consumer created before
a step that then fails is therefore deleted by nothing, and a persistent
misconfiguration accumulates one per attempt until each reaches its inactivity
threshold. So the pair is provisioned as a unit
(`mqttProvisionEphemeralConsumers`): the watch is created last, and a failure there
rolls the retained-messages consumer back, leaving a failed attempt with nothing of
its own behind. Its delivery state is not
quorum-replicated and it runs without flow control, so a leader change, a slow service
connection or an in-flight socket loss can **duplicate or drop** a delivery. The
guarantee lives in the pair:

- **the watch owns latency** — duplicates are inert, because every watermark advances
  by `max` and `evictRunning` coalesces observations onto one worker per client;
- **the committed-read backstop owns convergence** — `reconcileResidentOwnership`, one
  read per resident session on the periodic pass.

`DeliverPolicy` is **`DeliverNew`**, unlike the retained-messages consumer: a resident
session can only exist on this broker after the manager (and therefore the watch)
exists, so no historical record concerns a resident, while `DeliverAll` would replay
every record ever written into the no-local expiry reconcile at every manager start.
The filter names the record space exactly — `domainTk` carries its trailing dot, a
domain is one token and a client-ID hash is one token, so a single trailing wildcard
matches every record subject and nothing else.

**A read can only ever be a trigger, never a verdict**, and the asymmetry is the
point: "the subject moved" is provable from committed state; "the subject did not
move" is not. Every classification still happens in the eviction worker, under the
record fence. The backstop is reached only after the periodic pass's own
`recordTxOpen`, `seqUncertain` and armed-rejection arms have each taken their
`continue`, which is what stops it from rediscovering a local ambiguity as if it were
a peer's write.

Two shapes trigger, and the second is the one no sequence comparison can see:

- **the record MOVED** past our acknowledged sequence — feed the eviction trigger,
  exactly as a delivery does;
- **the record is GONE** while we hold a non-zero sequence — there is no newer
  sequence to compare against, so the backstop **arms the ownership marker**
  (`markWriteRejectedLocked`) and the existing rejection settlement converges it: the
  probe re-attempts the save, a CAS against an empty subject is rejected, and
  *rejected with the record gone* is already classified foreign. This adds an arming
  SITE, not a mechanism or a verdict, and the marker's point-of-use predicate is
  unchanged. It also inherits that settlement's PACKET-DRIVEN property for a bound
  session — the unbound half refuses while a client is attached, the bound half runs
  pre-dispatch on that connection's readLoop — so unlike the moved-record shape, this
  one does not converge a bound client that never sends again (`docs/CONFORMANCE.md`,
  and a roadmap bullet of its own). Absence IS an ownership fact: production contains
  exactly two deleters of
  a session record — `releaseTeardown`, at a marker sequence a claim CAS won, and
  `rollbackDeleteSession`, at the failed CONNECT candidate's own sequence — so a
  record can only vanish under a party that first claimed it. The bound outcome is
  therefore **0x8E**, not 0x89; `docs/CONFORMANCE.md` records the one imprecision that
  leaves.

**Observing our own writes is new, and one shape of it is dangerous.** The old channel
never delivered this broker's own stores; a stream watch delivers everything. A save
that LANDED with its response lost arms `seqUncertain` and leaves the acknowledged
lineage pair stale, so the delivered record is strictly past `sess.seq` and lineage
would read our own bytes as unrelated — evicting the rightful client with 0x8E over
its own write. Two layers answer it: before the write settles, the writer's open
record transaction refuses the worker's fence; after it arms, the worker's **digest
arm** (`evictOwnLanding`) recognises the record as the attempt's own bytes, using the
resolver's identity test verbatim, and defers to the resolver without deciding
anything. The same refusal covers the case with NO bytes to compare: while an attempt
is unresolved, an unusable load (absent, unreadable, undecodable) may not concede to
the takeover vocabulary, because the trigger can be that very write — the backstop
reaches this path carrying no record at all, so nothing else can screen it. The record's
`Origin` is used only as a **spawn filter** on the delivery
path — `Origin` proves no authorship, so the backstop deliberately passes no record at
all and re-triggers whatever the fast path skipped. A record the callback cannot
decode still feeds the trigger; returning early there would discard the observation
for exactly the records most likely to matter.

**A refused eviction is no longer lost.** `evictDecided` records the highest trigger a
decision actually consumed; a refusal (held gate, busy fence, digest deferral,
exhausted retries) leaves `evictLatest` ahead of it, and the backstop re-fires. The
entry that carries that accounting therefore outlives its workers, so pruning it is
one rule in one helper — **the map must still hold this exact entry, no session may be
resident for the hash, and no worker may be in flight** — applied from both eviction
cleanups, the no-local expiry worker's cleanup (which previously deleted on "no worker
in flight" alone and could erase a resident's trigger, since the two kinds share one
entry), `removeSession`, `removeSessionIfCurrent`, and the backstop's own scan.
Shutdown stays the one exception: it nils the whole map.

**What is closed and what is not.** A quiet session converges within delivery latency
of any remote ownership write, bounded by the next backstop pass that reaches it, and
absent any write ownership provably never moved. What remains: the serving window
between an observation and the worker's decision; and, during a rolling upgrade, a
quiet resident on a not-yet-upgraded broker still has only the lossy path until that
broker upgrades. Nothing recreates a deleted or inactivity-reaped watch consumer under
a live manager — the watch then stays absent and the backstop alone converges, until
the next manager start.

### An ambiguous record write is settled by a CAS, never by a readback

A save whose outcome is UNKNOWN — a timeout, a lost response — leaves the session
UNRESOLVED with the identity of the write it attempted: the CAS base sequence, a digest
of the exact bytes, and **those bytes as framed for the wire**. Settling it is a
classification of the record against that identity, and three of its four answers are
reads:

- **past the base with a matching digest** → it landed and only the response was lost;
  adopt the sequence it landed at;
- **moved otherwise** → that record is authoritative and not ours; detach and evict,
  never write;
- **unreadable** → nothing is known; write nothing, delete nothing, retry later.

The remaining two answers are not verdicts at all, and reading them as such was the
defect. The first is **the record still at the attempt's own base sequence**. JetStream
returns its contention rejection *while a write for the subject is in process upstream*,
so a proposal that has
not committed yet can commit a moment later; concluding "our write did not land" there
discarded the digest that would have identified that late landing as OURS. The session's
next save was then CAS-rejected, its rejection probe found a record whose bytes matched
nothing it knew, and `disposeForeignSessionLocked` evicted a session over its own write —
on the bound settlement, closing a live connection with 0x8E and discarding its Will, with
no takeover anywhere.

The second is **an absent subject**. It is tempting to read "gone" as proof that the
attempt is dead — its CAS can certainly not be ACCEPTED against an empty subject — but
acceptance is not the question. In a clustered stream the expected-sequence check runs
only BEFORE the proposal (`canConsistencyCheck := !isClustered`, nats-server v2.14.4
`server/stream.go:6247`), so a proposal already accepted into the log applies whatever
happened to the subject meanwhile, INCLUDING an exact-sequence delete — and this broker
issues those (`rollbackDeleteSession`, `clear()`). Disposing on absence therefore
reproduces the same late-landing false-foreign shape, one arm over.

**So both cases are settled by a CAS: the attempt's exact bytes are RE-SENT at the
same base sequence.** That is an arbitration, not a repair. A success proves the attempt's
CONTENT durable at the sequence it reports; a rejection
proves the subject moved, and one further read (through the same classification helper,
so the two cannot drift) says by whom — ours if the original landed in the window, foreign
if the record moved to other content, **unresolved** if it is at base again (contention)
or unreadable, and for an absent record whatever the REJECTION KIND says (below: 10164
defers, 10071 is fail-safe foreign). An ambiguous re-attempt keeps the identity EXACTLY
as it was: it names whichever of the two proposals lands. The re-send is byte-identical
rather than a re-marshal because the caller KEPT its mutation and memory may have moved
on — a
re-marshalled body's digest could no longer match what a later read compares against.

For the gone case the re-attempt can never be accepted, and that is not its purpose: what
it extracts is the REJECTION KIND. **10164** means a write for this subject is in process
upstream — possibly the original, accepted before the delete and still to apply — so
nothing is settled and nothing is disposed. **10071** means no write is in process, which
is the best evidence available that the original went with the record; it is classified
foreign FAIL-SAFE, not proven, because a leader transition clears the in-process map
(`jetstream_cluster.go:4720`) while a replicated proposal can survive it.

The pass is single-shot: the periodic pass is the retry cadence, and a retry loop would
hold the record fence across an unbounded number of round trips. It costs at most three
JetStream requests (read, re-attempt, classifying re-read) of record-fence occupation,
plus whatever the landed continuations do — which is the pre-existing landed-arm cost made
more frequent, not a new hold class.

**The invariant does not rest on "only one of them commits", and that matters.**
Ordinarily the shared CAS base makes the second proposal impossible. But a leader
transition clears the in-process map (`jetstream_cluster.go:4720`) while a replicated
proposal can survive it, and a clustered apply never re-checks the expected sequence — so
a surviving original and a re-attempt admitted by the new leader can BOTH land. Three
properties make that outcome indistinguishable from a single landing: the bytes are
identical; log order puts the re-attempt last, so the PubAck this broker adopts names the
surviving sequence; and `$MQTT_sess` is `MaxMsgsPer: 1` — a validated prerequisite — so
the earlier duplicate is discarded the moment the later one arrives. What is spent is one
stream sequence and one extra persist notification, and that notification is idempotent:
both deliveries trigger the same classifying eviction attempt on the remote broker (the
attempt itself may be refused by a held gate or fence — see "Recognising a forked
session record" — in which case convergence falls to the displaced session's own local
triggers, the rejection probe first among them).

**The invariant is that an ours-landed settlement always leaves a state reachable by the
original landing with its response lost.** Content identity is the digest, ordering
identity is the committed sequence, and no consumer of either asks *which* proposal
landed: a register's `ClaimSeq` sampled at the stale base is still older than every later
owner's claim, the delayed-Will fences compare captured baselines against actual
sequences, and a remote broker's persist observation triggers the same classifying
eviction attempt against its stale resident as a landed original would (refusable, per
the recognition section; a refused attempt leaves convergence to the local triggers).

Two consequences are stated rather than hidden. **"It did not land" ceases to be a
reachable verdict**, so a kept mutation is never rolled back and a caller registers only
what it withheld for want of PROOF (`retainOnLanded`); for operations the client was never
positively acknowledged for — a SUBSCRIBE with no SUBACK, the 3.1.1 UNSUBSCRIBE whose
error closes the connection, a refused CONNECT's lifetime grant — the ambiguity now
completes rather than aborts, which was always a legal outcome and is now the deterministic
one. The v5 UNSUBSCRIBE is the one ACKNOWLEDGED case, and its UNSUBACK is made truthful:
the packet snapshots the true per-filter reasons before overwriting them with 0x83, and
the packet tail puts them back if a flag set by **that attempt's own landed continuation**
is observed — never a session-state predicate, since `seqUncertain` is cleared by a foreign
disposition too and can be re-armed by a different attempt. A connection the settlement
DISPLACED is not acknowledged, and what makes that hold is the **DISCONNECT claim read
inside the enqueue's own `c.mu` critical section**: a displacement notifies through that
claim and `processSessionPersist` holds no lock of ours, so any check made and released
before the enqueue is a check-then-act and an acknowledgment appended behind the 0x8E
would violate [MQTT-3.14.4-1]. The tail also samples ownership (`sess.c == c` under
`sess.mu`, rather than the asynchronous close flag), but that half is defensive: every
displacement that can reach the tail has a written CONNACK and therefore consumes the
claim, so removing the sample changes no reachable outcome today. It states a different
fact — "this session is no longer mine" — for a future detach-without-notifying path.
And **a genuinely wedged proposal keeps the session unresolved across passes** instead of
being cleared by a guess — the availability side of the trade, where unresolved is a
posture every caller already handles and a wrong verdict is not.

Foreign soundness is per-arm and scoped to a **non-zero base**, and only ONE arm is a
proof: a subject moved past the base with other content can never accept the attempt's CAS
and can never be its landing. The gone arm is fail-safe (above), and a record BELOW the
base is
impossible within a stream incarnation (sequences are monotonic, so a subject deleted and
re-created lands higher) and implies a reset or re-provisioned store, so it is classified
foreign **fail-safe** — never adopted — rather than proven. **Base 0 is excluded by
construction**: a first-ever record write carries no CAS header, a found record's sequence
is at least 1, so an unconditional create is never classified at-base and is never
re-attempted. Its existing classification stands, with the residual `docs/CONFORMANCE.md`
states.

### Recognising a forked session record

The foreign arm above is sound about one thing and silent about another. "The subject
moved past my base with other content" proves the record is **not ours**. It does not
prove the record **succeeds** ours — and that second claim is the one the broker went on
to make, evicting its own client with **0x8E "session taken over by a new connection"**,
which instructs that client to stop reconnecting under this session.

Across a leader transition the two shapes are genuinely different events. A write derived
from the same base as ours can survive the election unapplied, apply after a second write
the new leader admitted against that same base, and discard the earlier record under
`MaxMsgsPer: 1`. The survivor is then a **sibling** of our record, not its successor, and
nobody took anything over. Two brokers can reach the takeover conclusion about each other
from that one event.

A sequence cannot distinguish them, and neither can a counter: a per-write epoch is a
branch-local Lamport counter, and a competing branch that never read our record can advance
above us and then look like our successor. What distinguishes them is **ancestry**, so
every `$MQTT_sess` record carries a parent pointer — `WID`, the write's own identity, and
`PWID`, the identity of the record it was derived from. Two records carrying the same
`PWID` are siblings of one base.

**The rule** (`classifyRecordLineage`, `sess_fork_lineage.go`) is consulted at exactly one
place in each caller — the branch that has already concluded foreign — behind that
caller's existing `Sequence > baseSeq` gate, and it emits only **forked** or
**not-forked**:

1. a different incarnation generation → not-forked (its lineage restarts);
2. `ackWID != "" && ackPWID != "" && found.WID != "" && found.PWID == ackPWID &&
   found.WID != ackWID` → **forked**;
3. anything else → not-forked, which is today's behavior exactly.

Sibling-hood is defined against the **acknowledged** pair, never the in-flight attempt:
with acknowledged `{a,p}` and attempt `{c,a}`, survivor `{b,p}` is the fork, `{d,a}` is a
legitimate child, and `{c,a}` is our own late landing (recognised by digest before lineage
is reached). All four conjuncts of arm 2 are load-bearing, and the last one is the least
obvious: `storeFramedSessionMsg` replays our acknowledged bytes verbatim and a clustered
duplicate proposal can land them at a higher sequence, so without `found.WID != ackWID`
that replay is reported as a fork of itself.

**The lineage never decides that a write landed.** `mqttAttemptOurs` and
`mqttRepairSaveLanded` credit void-register verdicts and run landed continuations, and an
identity match is not proof that an attempt's *bytes* are durable — the digest comparison
remains the only thing that licenses them.

**Five classified paths** consult it, and every one of them keeps the disposition it
already had, changing only what is reported:

| Path | On a proven sibling |
|---|---|
| definite rejection, **bound** (`sess_rejected_write.go`) | `mqttBoundUnsettled` → **0x89** and the ORDINARY close bookkeeping. Deliberately not the foreign arm's shape: that arm calls `mqttDiscardWillOnTakeover` first, and suppressing a Will on a takeover nothing demonstrated is a silent loss |
| definite rejection, **unbound** | the same foreign disposition, with the forked vocabulary. It passes `nil` for the client, so the helper returns before the notification and *nothing observable differs*; the arm exists so the classifier has one meaning everywhere |
| ambiguity resolution (`sess_uncertain.go`) | the same foreign disposition, verbatim — the same eviction, residency, continuation and parked-action treatment — with the forked vocabulary |
| CONNECT-time classified saves (`reconcileResumedConsumers`, `grantRequestedLifetime`) | the same disposition, forked vocabulary. Their **10071** arms disposed with no readback at all, so they now read once and classify — the only new I/O in the mechanism, on a pre-CONNACK rejection path |
| the persist observation (`evictDisplacedOnRemotePersist`) | the same eviction — the record provably moved with content that is not ours — with the forked vocabulary, and the displaced incarnation's **delayed-Will outbox record is left alone**, because nothing arbitrated a session continuation. See below for the worker this route required |

A fifth site, the CONNECT rollback readback (`rollbackAcceptedSession`), classified any
connected record above the candidate sequence as a legitimate newer generation and
**cancelled the expiry timer** on it. A proven sibling now yields `rollbackForked`, which
preserves the timer and emits no "session store unavailable" warning, since nothing about
the store was unavailable.

**What is reported travels as one value** (`mqttDisposalVocabulary`): reason code, Reason
String, log line and close reason. Passing a reason code alone emits 0x89 alongside
"session taken over by a new connection", which is precisely the claim the verdict exists
to withhold. **0x89 "server busy, try again later"** is the point of the whole mechanism —
it invites the reconnect that resolves the ambiguity, where 0x8E forbids it.

**This detects; it never repairs.** The forked record stays, the rightful owner's granted
state is still lost, and the verdict licenses no write of its own. It does not even prove
the survivor erased us — a teardown claim is itself a full-body CAS off the same base — only
that the survivor is no successor of ours. An absent `WID` concedes through arm 2's guard
rather than through a legacy branch, so there is no alternate code path and no
mixed-version machinery. `docs/CONFORMANCE.md` states what is left open.

**The persist-observation consult** (`processSessionPersist` →
`evictDisplacedOnRemotePersist`, `mqtt.go`) is the route that turns a fork into
client-visible damage most often, and it is the one consult that cannot run inline: a
`JSPubAckResponse` carries no record, and reading one (`loadSessionMsg`) is a blocking JSA
request on the one delivery goroutine every later persist observation shares. The eviction
therefore runs on a spawned, admitted worker, and the mechanism is **recognition plus
in-process coalescing — arbitration stays with the record CAS and the rejection probe**:

- **One worker per client, not per observation.** The callback registers with the same
  `reconcileWM` state the no-local expiry reconcile uses (both branches create entries
  now); `st.evictRunning` coalesces every further resident-delivered observation onto
  the running worker, whose trigger is a watermark, not the PubAck that spawned it. The
  eviction trigger (`st.evictLatest`) advances ONLY on the resident branch: an
  observation that arrives with no session resident belongs to the no-local reconcile
  exclusively, and must never be replayed by a lingering worker against a re-added
  object (whose obligations that branch deliberately preserves). The worker carries
  only the client hash and resolves the resident object FRESH, under `as.mu`, inside
  each decision — a replacement installed mid-run is simply the object the next
  iteration decides against.
- **A latest-knowledge-wins loop, bounded structurally.** Each iteration reads the
  trigger, loads the record once, and re-loads only when the trigger has advanced
  strictly past `max(target, loadedSeq)` — a strict advance past the iteration's own
  knowledge, so the loop is bounded by observation count.
- **The observation licenses; the read only narrows.** An absent, undecodable,
  unreadable, or below-trigger record cannot revoke the license the observation granted:
  those arms concede the takeover vocabulary and decide at the observation level
  (evict only if `target > sess.seq`). A record older than the write that licensed the
  eviction is never classified — classifying it could answer 0x89 for a write it cannot
  speak for.
- **The gate is observed, never claimed.** The decision is one `as.mu` critical section;
  `lockSession` itself needs `as.mu`, so no gate can be newly taken while it runs, and a
  gate already held defers the worker (bounded retries, then a refused exit). That is a
  deliberate narrowing versus the old synchronous handler, which barged into a gate-held
  probe; the cost is a possibly lost eviction, which the at-most-once observation
  contract already allows.
- **The fence proves adoption quiescent — not attempt-free.** `tryBeginRecordTx`
  (non-blocking, refused → the same bounded retry) is what retires the ordering hazard
  where a remote observation is handled before the local write's own PubAck adopted its
  sequence: every `sess.seq`/lineage adoption on a resident object happens inside the
  writer's open record transaction, so fence-acquired means no adoption is in flight and
  the classification runs HERE, at decision time, never from a pair sampled at load
  time. An AMBIGUOUS attempt (store landed, response lost) closes its transaction with
  the pair still stale behind `seqUncertain`; that staleness only narrows — a sibling of
  the unadopted attempt concedes to the takeover claim (the one-hop recognition miss,
  today's behavior exactly), a sibling of the acknowledged record still classifies
  forked, and no shape manufactures a false forked verdict from a stale pair.
- **Strict staleness.** Eviction requires `loadedSeq > sess.seq`. Equality is the
  session's own record — no remote write can land AT our acknowledged sequence within a
  stream epoch — and refusing it is a deliberate narrowing versus the old handler, which
  evicted on an observation at exactly `sess.seq` (e.g. the late-delivered notification
  of the very write a restore adopted).
- **A fixed release order.** State disposal happens under `as.mu`; then `as.mu` is
  released, then the fence (`endRecordTx` can synchronously run the void-verdict
  handoff, which takes `as.mu` — the other order self-deadlocks), and only then the
  notification and close, so no lock is held across the unbounded socket write. Abnormal
  exits release the fence with `abandonRecordTx` (`sess_recordtx.go`), the handoff-free
  form that is legal under `as.mu`; the handoff it skips belongs to the periodic
  backstop (`reconcileResidentRecordState` → `flushVoidVerdictGap`).
- **Terminal cleanup is one atomic decision.** Clearing the coalescing flag re-checks
  the eviction trigger in the same `as.mu` section: a trigger that arrived after the
  worker's last attempt loops the SAME worker (fresh retry budget, possibly against a
  replacement object) instead of stranding behind a clearing flag. A refused attempt —
  busy-exhausted or no-resident — consumes only the trigger it answered, never the
  record it happened to load, so an observation at exactly that record's level still
  forces a decision once the gate frees.

On the takeover verdict the worker cancels the displaced incarnation's delayed-Will
outbox records, scoped by generation and origin sequence, inline (it is already off the
delivery goroutine and tracked). On the forked verdict that scan does not run: the
records stay pending, and their fate at `FireAt` still belongs to the fire-time fences.

### A definite record-CAS rejection is outstanding work

A session-record save is a per-subject CAS. When JetStream **rejects** it — **10071**
from the single-writer check, **10164** from the clustered contention branch — the write
certainly did not land, so `sess.seq` is still right and the session stays usable; that
is why `saveWithDetail` deliberately does not mark such a session unresolved. It is
correct about the write and silent about the other fact the rejection carries: **the
subject moved**, so this broker's belief that it owns the session may be obsolete.

Nothing else re-validates a resident session. The **persist observation**
(`processSessionPersist`, over the `$MQTT.JSA.*.SP.*.*` wildcard) is an at-most-once
core-NATS notification with no retry and nothing durable behind it, and it is the only
thing that evicts a displaced object today; an uncertainty readback's foreign verdict is
unreachable here, because a definite rejection creates no uncertainty. A never-expiring
session — **every** MQTT 3.1.1 persistent session — has no expiry timer either, and the
expiry sweep skips it outright. So with the notification lost the object stays resident
indefinitely, holding the client ID's reaping duty it can no longer discharge (both the
cross-replica expiry handoff and the sweep's stamp-a-deadline branch defer to a resident
object), and a **bound** one keeps serving a client against a session another broker owns
— which [MQTT-3.1.4-3] requires to be closed.

**What settles it is a write, not a read.** JetStream returns 10164 while a write for the
subject is *in process* upstream, so a load that finds the record still at our base
sequence proves nothing — the contender's proposal can commit a moment later. The
settlement is therefore a **probe**: re-attempt the save of CURRENT memory so a fresh CAS
arbitrates ownership, and let the readback distinguish only contention from a foreign
owner. The probe re-marshals memory rather than replaying the rejected bytes, because a
definitely-failed caller has since rolled its mutation back. It CASes at our own
last-known sequence, so it can neither overwrite a record that moved (a foreign record
rejects it) nor recreate a released one (a rejection implies a non-zero base, so the CAS
carries a real expected-last-subject-sequence header). There is **no digest arm**: a
rejected write did not land, so a later record matching our bytes cannot be this attempt.

The marker (`recordRejectedSeq`, memory-only) is armed centrally at the store boundary,
so a rejection site nobody enumerated is covered; its validity is a predicate at the
point of use — non-zero, equal to `sess.seq`, and not unresolved — so every path that
moves the sequence invalidates it without being enumerated. The `!seqUncertain` conjunct
is load-bearing: an unresolved session's save is refused *before* the store, and a probe
classifying on a zero-valued attempt would read any real record as foreign.

Verdicts: **landed** → ours, and the advanced sequence retires the marker; **rejected
with the record at the probe's own base** → contention, which is never ownership, so the
marker stays; **rejected with the record gone or at another sequence** → foreign, and
`disposeForeignSessionLocked` detaches, evicts and closes; **anything else** (a refused
fence, an unreadable readback, a non-CAS error) → nothing established, marker kept. A
foreign verdict is sound because a second local writer would need the fence the probe
holds, an earlier ambiguous write of ours would have armed uncertainty — and that arming
now HOLDS, because the resolver above no longer clears it on a base-sequence read, which
is what used to leave a late landing of our own unrecognizable to this probe — and a
same-broker replacement is excluded by the gate below.

The settlement runs from the **periodic pass**, renamed `reconcileResidentRecordState` for
the three things it now settles (an ambiguous write, a definite rejection, the void-verdict
backstop). Its gate, `lockSessionForRecordRejection`, claims the session as nobody and checks
**residency by identity in the same critical section**, which is what makes "a newer record
is not ours" true: the pass snapshots pointers, and a plain claim would let a stale object
classify a same-broker replacement's own record as foreign. That gate is also the sole
unboundness check on the path, and it is what keeps the settlement off a session with a live
connection: claiming the shared client-ID gate for a bound session would fail a concurrent
SUBSCRIBE's own claim, and that packet error closes a live connection.

**A BOUND session is settled on its own readLoop, before the packet is dispatched.** The
pass above cannot do it — its claim is nobody's, and taking the shared client-ID gate for
a live connection breaks a concurrent SUBSCRIBE — so the settlement is made *by* that
connection instead, from `mqttParse`'s per-packet loop
(`mqttGateDispatchOnRejection`, `sess_rejected_write.go`).

**Before dispatch, and per packet.** A QoS 1 PUBLISH routed and acked from a displaced
connection cannot be retracted, so a settlement that ran after the dispatch switch would
have already lost; and a check hoisted to the top of the read would miss a marker armed by
the *previous* packet in the same buffer (an UNSUBSCRIBE's own record save is one of the
ways it gets armed). The fast path is one predicate read per packet.

**Two predicates, named apart.** `recordRejectionGatesServingLocked` asks whether the
ownership question is still outstanding — may this packet be served? —
`recordRejectionPendingLocked` whether a probe is valid right now. They share the first two
conjuncts and differ in `!seqUncertain`, which the serve gate deliberately omits: while a
write of ours is unresolved no probe is possible, and "cannot ask" is not permission.
Gating service on the probe's licence instead would serve every packet between a contended
probe and the next opportunity to re-probe.

**Fail closed on everything but a landed CAS.** The settlement claims the ownership gate as
the bound client (the claim a SUBSCRIBE makes), takes the record fence in its blocking form
as an outermost readLoop entry point, and probes. *Landed* → dispatch, marker retired.
*Foreign* → the Will is discarded as a local takeover discards it, the session state is
disposed under the gate, and then — off the gate, on the readLoop — the takeover
notification is ATTEMPTED through the shared tail (`mqttNotifyTakenOver`; suppressed if this
connection never saw its CONNACK, or if another displacement path already claimed its one
DISCONNECT) and the connection closed synchronously before `mqttParse` returns, which is the
graceful DISCONNECT branch's own shape: no re-entry, no second DISCONNECT, and no socket I/O
under the gate. *No verdict with the gate held* — contended, unreadable, probe ambiguous,
suspended by an unresolved write — establishes nothing and closes the connection with
`errMQTTSessionOwnershipUnsettled`, a parse error mapped to **0x89 "server busy"** rather
than 0x8E, because announcing a takeover nobody proved would tell a client to stop
reconnecting on a guess. Returning that as a parse error is what makes the stop a real
stop: the readLoop's ordinary close bookkeeping runs, and the periodic pass settles the
marker after the unbind.

*A refused claim is CLASSIFIED, never assumed*, and the claim is taken through
`lockSessionForTeardown` rather than plain `lockSession` for that reason. A refusal does
not say who holds the gate: `mqttProcessConnect` registers the client-ID gate for its whole
duration and rebinds the session only part-way through, and a resolver can hold it for a
session that stays ours. So the settlement retries while this connection is still the
session's owner — bounded, every holder being bounded — and reads the outcome:

- **acquired**: the competing operation finished; probe normally. Transient contention
  therefore does not close a healthy connection whose probe would have landed.
- **rebound**: a takeover owns this connection now. It is told **nothing** — the takeover
  makes its own 0x8E attempt, and a courtesy 0x89 appended to that on the same socket would
  be a second DISCONNECT giving the opposite advice (the claim below would refuse it in any
  case; staying silent here is what keeps the *reason* the takeover's) — and its Will is
  settled here by
  `mqttDiscardWillOnTakeover`, the takeover's own rule, applied by whichever side notices
  the displacement first. That call is what stops an early close from publishing a Will the
  takeover was about to suppress: `mqttHandleClosedClient` marks a Will OWED *before* it
  contends for the gate, and the takeover's later discard preserves an already-owed Will by
  design.
- **still ours when the spin gives up**: nothing observable is displacing this connection,
  so it is an ordinary no-verdict outcome and says 0x89.

One path still takes no gate of its own: the persist observation's eviction worker
(`evictDisplacedOnRemotePersist`) claims nothing, but it now OBSERVES `sessLocked` inside
its `as.mu` decision and defers to a holder it finds (bounded retries, then a refused
exit) instead of overlapping a gate-held probe as the old synchronous handler did. The
deferral can lose the eviction — the at-most-once observation contract already allows
that — and a second DISCONNECT is refused by the claim below in any case.

**"Displace this connection" has one owner per leg.** Four paths displace a bound
connection — a local CONNECT takeover (`mqttProcessConnect`), a remote persist observation
(`processSessionPersist`), a resolver's foreign disposal (`disposeForeignSessionLocked` →
`mqttNotifyForeignDisposal`) and the bound settlement's foreign arm above — and they race
by construction. Rather than one atomic transition shared by all four, each leg of the
displacement has its own single owner:

- **Notification**: `mqtt.disconnectSent`, consumed under `c.mu` through
  `mqttClaimDisconnectLocked` — for every enqueue-path writer in the same critical section
  as the write it authorizes, so there is no check-then-act window; `shutdownDisconnect` is
  the one exception, reserving the claim under the lock and attempting its `writeDirect`
  after releasing it, so it can suppress a competing 0x8E and then write nothing. The
  first ELIGIBLE claimant — of the four paths, the parse-error footer, the packet-too-large
  0x95, or the shutdown drain's 0x8B — consumes the connection's one notification and every
  later sender is silent, which is what [MQTT-3.14.4-1] requires ("after sending a DISCONNECT
  the sender MUST NOT send any more MQTT Control Packets on that Network Connection"). The
  guarantee is **at most one server-DISCONNECT ATTEMPT per connection**, not one delivered
  packet: a nil build never reaches the claim, the CONNACK gate can refuse before it, the
  shutdown drain's reservation can be spent without a write, and the winner's write stays
  best-effort against a socket a concurrent close may already have taken down. Nothing
  gates non-DISCONNECT traffic, so a PINGRESP or an in-flight PUBLISH can still trail the
  winner until the close lands.
- **Close**: `closeConnection` is idempotent (`c.closed.Swap(true)`). Any number of paths
  may call it; one runs.
- **Will disposition**: `willDisposition`, transitioned only under `c.mu` and consumed
  exactly once by `mqttTakeWill`, with the takeover discard rule applied by whichever side
  observes the displacement first.

The four takeover notifications share one tail, `mqttNotifyTakenOver`, which is also the only
home of the [MQTT-3.2.0-1] CONNACK gate for them: a connection displaced before its own
CONNACK is closed silently, because CONNACK must be the first packet a client sees and
`mqttAbortAcceptedConnect` leaves a refused connection bound until its readLoop detaches it.
That gate reads `connAckWritten` under `c.mu`, which is why `mqttEnqueueConnAck` stores the
flag inside the same critical section as the CONNACK write — storing it after the unlock
left a window in which a notifier saw `false` for a client that had already been
acknowledged, and dropped a DISCONNECT it was owed. `shutdownDisconnect` remains a
separate, legitimate reader of the same flag; it claims like everyone else, but writes
through `writeDirect` rather than the enqueue path.

What the mutex ordering does NOT decide is which of the two enters first. A notifier that
takes `c.mu` BEFORE the CONNACK write reads `false`, stays silent, and the client then gets
its success CONNACK followed by a silent close — the notifier-first ordering, and the price
of a gate that cannot predict a future CONNACK. The displaced CONNECT's own goroutine keeps
running until the asynchronous close lands, which is unchanged by this design and is
arbitrated where it matters: every record write it still makes goes through the session
record's CAS, so a record another broker has moved rejects it (`docs/CONFORMANCE.md`).

**At most one probe per arming**, so the gate hold is bounded by the fence wait plus a store
and a readback (roughly three JetStream timeouts) rather than by packet rate: every verdict
except the landed one ends the connection, and the landed one retires the marker. One
qualification on that arithmetic: the fence wait is bounded by the CURRENT holder's work
plus whatever other BLOCKING acquisitions precede this one, because the fence is barge-proof
against the non-blocking form only.

`beginRecordTx` ANNOUNCES itself — an atomic waiter count incremented before it contends on
`sess.mu` — and every non-blocking acquisition (`tryBeginRecordTx`, and the void-verdict
handoff's inline acquisition) refuses while that count is nonzero. Both linearization points
are exact: the announce is ordered before the mutex contention, and an acquirer
linearizes at its successful free-fence predicate load under `sess.mu`. The refusal is
double-checked, and the pre-lock half of that check is a liveness guarantee rather than an
optimization: a try-caller refused there never touches `sess.mu`, so a storm cannot starve
the announced waiter of the mutex itself — the claim rests on the code, not on
`sync.Mutex` fairness, which is undocumented. A waiter that dies abnormally
(a panic, or `runtime.Goexit` from a test seam) withdraws its announce INSIDE the critical
section that holds `sess.mu`, never after releasing it, so no reader of the count — the
void-verdict handoff decision at a fence release, above all — can act on an announce for a
waiter that no longer exists. So **try-callers cannot add a fence hold
ahead of the waiter** unless already admitted at the announce: a try load following the
announce refuses without locking `sess.mu`; a caller past only the fast check contends
once and refuses if its authoritative reload occurs while the announce remains
outstanding; at most one caller already past the authoritative predicate may still open
and hold the fence; and a delayed caller may succeed after the waiter has completed, which
lengthens nothing.

What that does NOT give is an unconditional bound on every blocking acquisition. There
is no FIFO among BLOCKING waiters — a newly arriving `beginRecordTx` can win the post-broadcast
mutex race against an older `Cond` waiter — so the individual bound holds only absent an
unbounded stream of later blocking arrivals. In ordinary operation there is at most one
blocking caller per session (the readLoop entry points share the session's one readLoop, and
the expiry-side `clear()` is serialized against a reconnect by `lockSession`), but that
is an observation, not a proven invariant. Waiter priority also carries a reciprocal
cost — refused try-callers retry on events rather than on a clock, and the never-expire
close stamp takes its unordered fallback on more occasions — both stated in
`docs/CONFORMANCE.md` residual 13 with the blocking-vs-blocking residual that stays open.

**CONNECT and DISCONNECT are exempt.** Pre-CONNECT there is no bound session, and a second
CONNECT is already a protocol error before dispatch. A DISCONNECT routes and acks nothing;
what it carries is the client's own instruction about its Will — fail-closed would publish
a Will the client explicitly discarded ([MQTT-3.1.2-8], Spec5 [3.14.4]) — an expiry override
already guarded by `sess.c == c` and by the record CAS, and the teardown that *is* the
[MQTT-3.1.4-3] convergence. PINGREQ is **not** exempt, and that is what bounds convergence:
a conformant client sends one within its keepalive, so even an otherwise silent displaced
connection closes within about 1.5 keepalive intervals.

What the mechanism guarantees is: **once a packet's pre-dispatch check observes the
outstanding ownership question, nothing further is served without a fresh landed CAS.** It
is not "nothing is served after a rejection anywhere in the cluster" — the packet already
past its check when the marker arms is served, outbound deliveries continue until the next
inbound packet, and a landed probe proves ownership at its own linearization point, not
through the dispatch that follows. The record is not a liveness lease. `docs/CONFORMANCE.md`
states those residuals.

## Reserved `$MQTT.>` namespace

MQTT topics map to NATS subjects by swapping the separators: `/` becomes `.`, so
`foo/bar` is published as `foo.bar` and read back as `foo/bar`. MQTT wildcards
map the same way (`+` to `*`, `#` to `>`).

The broker reserves `$MQTT.>` for its own state, and that state moves as ordinary
NATS traffic on those subjects:

| Subject | Contents |
|---|---|
| `$MQTT.msgs.<subject>` | every QoS 1/2 message (the `$MQTT_msgs` stream) |
| `$MQTT.rmsgs.<subject>` | every retained message |
| `$MQTT.sess.<domain>.<hash>` | session records |
| `$MQTT.JSA.<...>` | JetStream API replies and session-persist broadcasts |
| `$MQTT.willq.*`, `$MQTT.willcancel.*` | delayed-Will outbox and cancel watermarks |
| `$MQTT.qos2.in.<hash>.<gen>.<pi>[.rel]`, `$MQTT.out.*` | inbound QoS 2 move state and its release claim, outbound PUBREL state, and session-affine shared QoS 2 copies |
| `$MQTT.qos2.assign.<group-durable>.<seq>` | shared-subscription QoS 2 assignment claims |
| `$MQTT.inflight.<hash>.<gen>.<pi>` | outbound in-flight registers: the durable identity of a started QoS 1/2 delivery |
| `$MQTT.sub.*`, `$MQTT.deliver.pubrel.*`, `$MQTT.deliver.q2.*`, `$MQTT.qsub.*` | per-client delivery subjects |

Anything able to read `$MQTT.msgs.>` reads every QoS 1/2 message in the account,
so the namespace is closed on every path a client can reach:

- **PUBLISH** into it is always dropped. MQTT 5.0 QoS 1/2 gets reason `0x87`
  (Not authorized) in the PUBACK/PUBREC. **MQTT 3.1.1 QoS 1/2 gets an ordinary
  success PUBACK/PUBREC**, because 3.1.1 has no reason codes -- the message is
  still discarded, and the broker logs a warning, but a 3.1.1 publisher cannot
  tell. QoS 0 is dropped silently in both versions.
- **SUBSCRIBE** to it is rejected with SUBACK `0x80` in both versions (5.0 also
  carries a reason string). This covers the whole prefix, not just the delivery
  subjects: a wildcard filter such as `#` already cannot match a `$`-topic
  ([MQTT-4.7.2-1]), but an explicit `$MQTT/msgs/#` filter is not "reserved" by
  that rule and would otherwise be honoured. A session **record** carrying such a
  filter is **refused** at the restore boundary instead: only a development broker
  predating this deny could have written one, there is no released persisted-state
  contract to migrate from, and the CONNECT fails closed before anything derives a
  consumer name from the record or rewrites it. The refusal names the offending
  key and tells the operator to stop the account's brokers and recreate the
  bridge-owned `$MQTT_*` streams. It deliberately offers no work-around: moving the
  client to another ID would leave this record and its consumers behind, and for the
  generationless shape below nothing would ever reclaim them. A clean-start
  CONNECT is refused too, because its discard would delete the record's consumers,
  which is the same migration under another name. The record itself is not
  quarantined: with a valid generation it is still expired and reaped as ordinary
  state. The realistic record has **no** generation — this deny predates
  generation-scoped naming, so a broker old enough to have written a reserved
  membership wrote no `gen` either — and such a record is reclaimed by nothing until
  the streams are recreated (see the teardown section). The reserved check therefore
  runs *before* the generation check, so the operator is told which key is at fault
  rather than only that the generation is missing.
- **Will topics** in the namespace are rejected at CONNECT: `0x05` in 3.1.1,
  `0x87` in 5.0. A Will never passes through the PUBLISH path, and a *delayed*
  Will is replayed from the outbox over the service connection where the client's
  own NATS permissions no longer apply, so it is refused up front.

### Recommended NATS permissions

Deny both directions on the whole prefix for the identity MQTT clients
authenticate as:

```text
publish:   { allow: [">"], deny: ["$MQTT.>"] }
subscribe: { allow: [">"], deny: ["$MQTT.>"] }
```

Both layers are required and neither is redundant. The broker's own checks bind
anything speaking MQTT; the ACL is what binds someone who takes the same
credentials and connects to NATS directly. Under auth passthrough an MQTT client
holds real NATS credentials, so that is not a hypothetical.

This deny is safe because the broker creates its own delivery subscriptions
(`$MQTT.sub.*`, `$MQTT.deliver.pubrel.*`, `$MQTT.qsub.*`) on the **service**
connection rather than on the per-client one. If they were on the client
connection this ACL would break QoS 2 delivery and retained replay, and NATS
evaluates deny before allow, so the three could not be carved back out. The
conformance environment (`test/conformance/nats.conf`) ships this exact ACL, so
the suite passing is what keeps that separation honest.

## Inbound QoS 2 move protocol

An inbound QoS 2 PUBLISH is **held** on `$MQTT_qos2in` under
`$MQTT.qos2.in.<client-id-hash>.<generation>.<packet-identifier>` and **moved**
onto `$MQTT_msgs` when the client sends PUBREL. The move is recoverable rather
than destructive: the message is delivered onward **first**, and only then is the
hold record replaced. Implementation and the design invariants it obeys are in
`internal/mqtt/qos2_move.go`.

One subject carries two record states, and it is reused by every exchange that uses
the same packet identifier within one incarnation — a client is free to reuse an
identifier as soon as its exchange completes:

| state | shape | written by |
|---|---|---|
| `held` | the encoded PUBLISH plus `Nmqtt-Q2Gen: <22-char NUID>` | a **create-only** CAS (expected last subject sequence 0) |
| `delivered` | empty body, the same `Nmqtt-Q2Gen` plus `Nmqtt-Q2Done: 1` | a CAS at the held record's **exact** sequence |

`Nmqtt-Q2Gen` is the exchange's immutable identity, stamped once at PUBLISH time
and never rewritten. Because subjects are reused, a stream sequence is **not** an
identity: the finalizer compares generations and abandons without mutating anything
when it finds a foreign one, so it can never replace a **later exchange's**
undelivered payload with its own tombstone. The same comparison is what makes the
release claim's post-claim re-read safe (below). A record whose generation is
missing, malformed, or declared more than once is classified `corrupt` — never
`held` — and is disposed of rather than mistaken for a duplicate.

The subject also carries the **session incarnation generation**, so it is never
reused across incarnations and a discard names each hold subject exactly. The two
generations answer different questions and both are needed: the incarnation scopes
the subject, the exchange identity scopes the record on it.

The terminal tombstone is what makes a PUBREL retry safe indefinitely: a retry
that finds it is answered `0x00` with no second delivery, independently of any
deduplication window. The `Nats-Msg-Id` carried on the onward store (derived from
the generation, so it is identical across every retry of one exchange) only
narrows the window in which a retry duplicates the *stored* copy; correctness does
not depend on it. See `docs/CONFORMANCE.md` for the exact guarantee and its
residual deviations.

### The release claim

Delivering onward first makes the move recoverable, but it also means the hold
record's arbitration point comes *after* the irreversible act. A session discard
used to purge these subjects by filter, which competes with nothing: a PUBREL that
had merely *read* a held record went on to fan out, and its finalizer then read the
resulting absence as proof the exchange had completed. So a **release claim**
(`internal/mqtt/qos2_release_claim.go`) sits in front of the delivery — a small
create-only CAS record on a sibling subject, the hold subject plus a `rel` token:

| writer | value | when |
|---|---|---|
| the releasing PUBREL | `releasing` + the exchange generation | before the onward store |
| a session discard | `discarded` | before it destroys the hold |

Exactly one create can succeed, so exactly one of "release it" and "discard it"
wins and the loser does nothing irreversible. The claim is **discard-exclusive, not
releaser-exclusive**: the generation identifies the exchange rather than the broker,
so a second releaser of the same exchange reads its own generation back and
proceeds. Because the claim is a sibling record, the hold record is never
rewritten — its payload, properties and store time (which ages the Message Expiry
Interval) stay as the PUBLISH left them — and a discard can enumerate a session's
inbound QoS 2 state through `STREAM.INFO` without reading a single payload.

The claim is paired with an authoritative **re-read** of the hold record, and
neither replaces the other: the claim fences a *discard*, which a re-read cannot,
because read-then-fan-out is check-then-act; the re-read fences an
*already-completed* exchange, which the claim cannot, because a peer that committed
and reaped leaves nothing to contend with, so a stalled releaser's create-only claim
would simply succeed. A discard that finds a `releasing` claim waits briefly and
then leaves both records rather than destroying a payload a crashed releaser's retry
still needs; on the same broker an in-process fence refuses to admit a release at
all. A `discarded` marker is never reaped by a releaser or by the discard itself —
it is the only durable proof that a late releaser must lose — and is left to be
aged out.

`$MQTT_qos2in` must be `LimitsPolicy` + `DiscardOld` + `MaxMsgsPer: 1` (`DiscardOld`
is what makes the terminal CAS a *replacement*; `MaxMsgsPer: 1` is what stops it
appending and leaving a stale `held` record underneath), and must not withdraw the
write, CAS, subject, acknowledgement, retention, deletion or purge semantics the
protocol depends on. The purge that `DenyPurge` would block is `purgeQoS2Hold`'s
**confirmed exact-subject** purge — the session discard's and the orphan sweep's
only removal of a hold subject; the discard's earlier blind filter purge is gone.
The claim adds no requirement of its own: it lives inside the
stream's existing subject space and needs the same single-slot CAS semantics the
hold record already does — under the `<idHash>.<gen>.<PI>` layout and its `.rel`
sibling the field list is unchanged, since the subject check is a wildcard
spanning both layouts and each of the hold and the claim is its own single-slot
register (see "Stream prerequisites"). A stream that does not match **fails
broker startup** with recreate guidance rather than being updated in place —
updating it would be actively unsafe, since a broker still running the
pre-move code would silently overwrite held records once the discard policy
flipped. Upgrading a development
store therefore means stopping **all** brokers for the account and recreating the
stream, under the compatibility boundary above. This is not a rolling upgrade.

## Session-affine shared-subscription QoS 2

A QoS 2 message delivered to an MQTT 5.0 Shared Subscription is pending on the
queue-delivered **group** durable from the moment its PUBLISH is sent until the
client's PUBREC. Left there, a member leaving in that window makes AckWait offer
the started message to a different member — which MQTT 5.0 [4.8.2] forbids. The
broker therefore takes durable ownership of a started delivery and moves it off
the group durable entirely. Implementation and its invariants are in
`internal/mqtt/qos2_shared_affine.go`.

Two durable pieces:

| what | where | written by |
|---|---|---|
| assignment claim, one per `(group durable, source sequence)` | `$MQTT_qos2assign`, `$MQTT.qos2.assign.<group-durable>.<seq>` | a **create-only** CAS elects the winner; every later transition is a CAS at the record's exact sequence |
| affine copy of the message | `$MQTT_out`, `$MQTT.out.q2.<client-id-hash>.<generation>.<claim-hash>` | a **create-only** CAS, so exactly one of several legitimate materializers wins |

The claim is the arbitration record and it is honored **indefinitely** — not for
the lifetime of a deduplication window. `Nats-Msg-Id` on the copy store is an
optimization that narrows a duplicate-copy window; ownership never rests on it,
for the same reason the inbound move protocol does not rest on it.

**Session generation.** `<client-id-hash>` is a hash of the client ID, so a later
clean session of the same client reuses it. A random 22-character generation,
persisted with the session record, therefore scopes all three affine names — the
copy subject, the consumer durable `$MQTT_Q2AFF_<hash>_<generation>`, and its
deliver subject `$MQTT.deliver.q2.<hash>.<generation>` — so a leaked consumer from
an earlier generation can neither read a new generation's copies nor inject into
its delivery subscription. On resume the three are validated together and a
mismatch **fails closed**: the consumer that cannot be accounted for may still
hold an in-flight PUBLISH, so it is preserved for diagnosis rather than replaced.

**Claim state.** A claim carries a monotonic phase (`claimed` → `materialized`,
"does a copy provably exist?") plus three independent completion facts:
`SourceAcked` (the group delivery's ack is proven committed), `AffineDone` (the
copy reached a terminal state) and `Discarded` (the owning session ended). Every
transition preserves facts already proven, and a claim may be deleted only when
`SourceAcked ∧ (AffineDone ∨ (Discarded ∧ the copy is authoritatively absent))`.
`Discarded` deliberately does **not** imply `SourceAcked`: reaping a discarded
claim whose group ack never committed would let the source redelivery elect a
fresh winner. For the same reason the group ack is proven by a JetStream double
ack (`jsa.confirmAck`), not by the ordinary fire-and-forget `sendAck`.

A fifth field, `Lost`, records the one outcome that is neither completion nor a
permitted discard: the affine consumer was destroyed out of band, and interest
retention took its pending copies with it, so a QoS 2 message the broker accepted
was never delivered. `Lost` is terminal (no contender waits on it, and the group
delivery is released) but never reapable, because the record is the only durable
evidence that the loss happened and a clean reap would erase it. `Lost` and
`AffineDone` are mutually exclusive and are not independently merged: whichever
the durable record already proved stands.

Telling the two apart is a matter of WRITE ORDER, not of observation. `AffineDone`
is recorded **before** the copy is acked, so an absent copy under a claim that
never reached `AffineDone` was never acked by a delivery — it was dropped with its
consumer. Every broker reads that identically, which an "I saw the consumer
disappear" flag could not: the broker that observed it may be gone, or the
consumer may have been recreated, by the time anyone reconciles.

That inference is authoritative **while the owning session record still exists**.
Once it is gone, a legitimate teardown deleting the consumer and an out-of-band
destruction of it are indistinguishable — both leave a materialized claim with no
copy and no `AffineDone` — so orphan reconciliation records `Discarded` rather
than `Lost`. A `Lost` flag already committed is permanent and never reaped; what
cannot always be recovered is a loss that happened before the session record
disappeared and was never observed by the owner. See "Orphan reconciliation". The inverted
window (recorded, not yet acked) is benign and self-correcting: the copy stays
live, a live copy always keeps its claim (`reapable` requires the copy to be
authoritatively absent), and the redelivered copy is disposed of by the delivery
gate's terminal branch.

**A provisioned path with no membership, and what removes it.** The affine path
is provisioned in `mqttProcessSubs`' FIRST filter loop, before any filter is known
to succeed, so it can outlive the subscription it was built for. The variants are
told apart by their REMOVAL POINT, not by their shape:

- A generation this call **minted**, whose provisioning save went AMBIGUOUS, is
  ELIGIBLE for removal at SETTLEMENT. The settlement makes the attempt's own bytes
  durable, so the record names the path, and nothing in the packet can undo that
  because every save is refused while the session is unresolved. What licenses the
  removal is that the PACKET FAILED and no NEW membership became durable — not that
  the generation is provably empty, which it is not in general. A packet carrying a
  SECOND shared QoS 2 filter re-enters provisioning with the consumer already
  present, skips the save and OPENS the readiness gate on a still-unresolved
  session, so a move can transiently claim and copy under the generation before the
  packet's membership rollback. The reclaim therefore RETAINS the path whenever the
  generation still owns an assignment: a failed packet is not an ended session, and
  `Discarded` means the owning session ended, so a delivery already started to a
  session that is still live is one [MQTT-4.8.2-5] requires it to complete — session
  teardown is the event that may end it. The check is not check-then-act, because
  the unwind fences and drains the path first and a detached generation admits no
  new move. (The packet's own two unwinds still discard on a failed packet; see
  `docs/ROADMAP.md`.) The
  pre-affine RESUME shape is different in kind: the restored record already carried
  shared QoS 2 memberships, restore loaded them before the provisioning attempt,
  and they survive the settlement — so the path is owed to a LIVE subscription and
  must not be removed at all. The provisioning save therefore registers a landed
  continuation (`unwindProvisionedQ2AffineOnLanded`) that runs the ordinary
  three-arm unwind under the resolver's token, and `hasSharedQoS2Sub()` is what
  tells those two apart: eligible, never automatic. The guard is mandatory rather
  than defensive, and
  `TestQ2AffineAmbiguousProvisioningKeepsAPathAMembershipOwns` is the resume shape
  it exists for. Only a MINTED generation qualifies at all: a resume that
  re-establishes a path the record already named reaches the same save, and its
  generation may owe assignments to a delivery already started to it. After a crash
  inside that window nothing removes the path at settlement, because there is no
  longer a continuation to run: a later shared QoS 2 SUBSCRIBE REUSES it — making
  it useful again without deleting it or releasing its cost — and session teardown
  is what removes it.
  Its unwind is prepared with `disposeNow` FALSE, so ownership is arbitrated before
  anything irreversible: the cleanup record is CASed first, and only once that write
  is confirmed are the generation's assignments discarded, its delivery subscription
  and MaxAckPending charge released, and its consumer deleted. A remote winner that
  took the session over and preserved the
  generation therefore wins the CAS and keeps its started deliveries, and the
  definite arm restores a path whose claims were never touched. This is the order
  `claimTeardown` already uses, and it is safe to dispose late because
  `materializeQ2AffOnBehalf` terminates a claim whose record no longer names its
  generation. The packet's own two unwinds still dispose first; see
  `docs/CONFORMANCE.md` and `docs/ROADMAP.md`.
- The **claim-cleanup-failure abort** and the path retained after UNSUBSCRIBE of
  the last shared QoS 2 filter are deliberately KEPT: the path may still owe
  outstanding assignments or started deliveries. A later SUBSCRIBE reuses them;
  session teardown is the only thing that removes them.

**Stream prerequisites.** `$MQTT_qos2assign` must be `LimitsPolicy` +
`DiscardOld` + `MaxMsgsPer: 1` — permanent retention is as load-bearing as the
per-subject singleton, because claims are read by direct subject lookup and are
never consumed, so interest or work-queue retention would drop them. `$MQTT_out`
must stay `InterestPolicy`: that is how a discarded session's undelivered copies
disappear with its consumer, and it is why a copy must never be stored before its
consumer exists (a store with no interest returns success and keeps nothing). Both
have validators (`validateQoS2AssignStream`, `validateOutStream`) that **fail
broker startup** rather than update a non-conforming stream in place, and the two
lists differ deliberately: each is derived from what that stream's owner actually
does.

**Mixed version.** The storage schema is rolling-compatible — an older broker
ignores the new stream and the new session-record fields. The delivery *protocol*
is not: an older broker serving the same shared group never consults
`$MQTT_qos2assign` and will redistribute a started delivery a newer broker has
claimed. Shared QoS 2 affinity holds only once every broker that can serve the
group is q2aff-aware. See `docs/CONFORMANCE.md` for this and for the
packet-identifier residual.

## UNSUBSCRIBE drain

[MQTT-3.10.4-3] requires the server to complete the delivery of any QoS 1/2
message it has already **started** sending, even after the client unsubscribes.
UNSUBSCRIBE used to tear the plain consumer down immediately, and since
`$MQTT_msgs` is interest-retained, deleting the durable deleted the messages it
still held: the started PUBLISH was simply lost.

A **drain** is what that teardown becomes when started deliveries remain
(`internal/mqtt/unsub_drain.go`). The durable and its delivery subscription
survive; already-started deliveries keep being redelivered and completed; fresh
messages on the filter are discarded. The drain is a field on the session record
(`Drains`), so it survives disconnect, restart and failover, and it lives until
every started delivery completes or the session ends. It is deliberately kept
**out** of the record's `Cons` map: an older broker restoring it there would find
`cons[sid]` on a re-SUBSCRIBE, take the early return, and never create a delivery
subscription.

The invariants, in the order they matter:

- **Persist before ack.** A delivery of a claimed sequence is acked only after a
  *confirmed* save in which the drain no longer claims it. One helper owns every
  such ack; a refused fence or failed save acks nothing and the message is simply
  redelivered.
- **The commit points.** QoS 1 has one phase. For QoS 2 the durable **PUBREL
  intent** — written *before* the PUBREL is stored — commits the exit from the
  PUBLISH phase, and only a **confirmed** PUBREL (stored *and* in delivery)
  authorizes removing and acking the source claim. The phase is three-valued, not
  a boolean, because "a PUBREL exists" and "the store outcome is unknown" must not
  authorize the same actions.
- **Only transmitted deliveries drain.** A message withheld by the in-flight cap
  was never sent, so it is discarded rather than enrolled.
- **Single ownership.** From staging, a drained consumer lives exclusively in
  `sess.drains` and its `cons[sid]` entry is gone, so nothing can reach it through
  `cons` and create a second durable on the same filter. A re-SUBSCRIBE therefore
  **adopts** the drain explicitly.

Recovery is positive, never an inference from absence. Because the phase is
memory-only, a restart would otherwise establish source interest and resend the
payload of an exchange already past PUBREC — before the correlated PUBREL callback
ever ran. So a barrier runs **before any source interest exists**: it reads the
persisted intent, indexes the PUBREL records still on the session's subject (they
are unacked, hence incomplete), and resolves each claim. Intent with a record →
confirmed. Intent without one → recreate it under the *original* identifier,
having first verified the PUBREL durable exists and then checking **presence at
the returned sequence** rather than trusting the store response. A missing record
can never mean "already completed", because the callback removes claim and intent
before forwarding. No intent → the client provably never PUBREC'd durably, so the
payload is redelivered with DUP and the exchange completes. Any failure withholds
that drain's source interest for the connection rather than denying the CONNECT.

A stored PUBREL names its source (`Nmqtt-Q2SrcDur` / `Nmqtt-Q2SrcSeq`), which is
what links it back after a restart — `cpending` is memory-only. A **drained**
PUBREL additionally carries a deduplication identity scoped by an attempt counter,
so a superseded broker's late store collapses into it; the ordinary path
deliberately carries none, because there the only legitimate re-store is the
post-restart one after a redelivery, and a surviving dedupe entry would silently
swallow it.

## Plain QoS 1/2 consumer reconciliation on resume

The session record persists each plain QoS 1/2 subscription's JetStream consumer
config (`Cons`), and the CONNECT restore path used to trust it verbatim: a
durable reaped by an inactivity threshold — the broker's own
`consumer_inactive_threshold` or a stream-forced
`ConsumerLimits.InactiveThreshold` on `$MQTT_msgs`, both of which reach
consumers requesting 0 — was silently restored, and the client got Session
Present = 1 with a subscription that never delivered again until a clean start.

The fix is a dedicated resume phase (`sess_resume_reconcile.go`), run inside the
CONNECT's accept transaction **after** register recovery (so every in-flight
register is classified against the *old* durable names before any repair
exists) and **before** the CONNACK (so a failure refuses the CONNECT and
nothing has been transmitted when it fails). It is deliberately CONNECT-only:
repairing inside SUBSCRIBE processing would make every record-write site in
that thicket replacement-aware, and the packet's final update may legitimately
not write at all. Placing the repair before the CONNACK makes its save the
first record write after the accept save *by construction* — no general
subscription-processing write can observe a half-committed replacement.

The pass:

1. **Collect.** Every plain (non-shared) QoS 1/2 filter's `(subject, sid)`
   pairs from the restored subscriptions — the level-up companion of a
   multi-level filter included, since it is an independent consumer. A QoS 1/2
   sid with **no** `Cons` entry at all is classified *absent identity* and
   minted here (left alone, restoration would create its consumer after the
   CONNACK and never persist it: the restore path runs no final subscription
   save). No filter here can be in the reserved `$MQTT.>` namespace: such a
   record is refused at restore, and a resident session could never have
   subscribed to one. A stale `Cons` entry with **no** matching subscription is not an active
   delivery entry and is deliberately not verified (a stated residual in
   `docs/CONFORMANCE.md`).
2. **Subscribe everything closed, then batch-verify.** Before any lookup, a
   **closed, flushed** delivery subscription is installed on every recorded
   deliver subject, marked *phase-owned* — for present consumers too, because a
   consumer that is present at lookup can be reaped a moment later (the lookup
   does not stop the inactivity timer), and its dead recorded deliver subject
   would then be restored: the original bug, reproduced. The flush is what
   makes the interest authoritative before the lookup result means anything.
   It **narrows** that window rather than closing it outright: JetStream checks
   a consumer's activity, releases its lock, and only then performs the
   deletion, so interest that registers after the decision cannot revoke it
   while `ConsumerInfo` still reports the consumer present. A consumer reaped
   inside that last gap is trusted for this connection and repaired at the next
   CONNECT — the same disposition as any other out-of-band deletion, and a
   bounded fraction of the original defect, which persisted until a clean
   start.
   Installing interest this early means JetStream immediately pushes the
   session's whole **offline backlog** into a window in which nothing may be
   transmitted yet, so a QoS 1/2 delivery arriving there **waits** on the
   subscription's *phase gate* rather than being dropped: dropping it would
   hold every queued message until AckWait (30s by default) — turning an
   immediate resume delivery into a half-minute stall — and then re-send it
   with DUP set on its **first** transmission to the client. Waiting keeps the
   backlog first deliveries, in order, at adoption speed. The wait is bounded
   by the connection's effective **AckWait** (capped by
   `mqttPhaseGateWaitMax`), which is the deadline past which waiting buys
   nothing because JetStream re-pushes the message anyway, and which also stops
   a CONNECT that died without releasing — every ordinary exit releases, so
   that means a panic — from parking a delivery goroutine. A delivery that does
   run out its wait, or that belongs to a filter the sweep reclaims unadopted,
   is **recorded as withheld** exactly as an in-flight-cap deferral is, so the
   attempt that finally goes out is still marked a first transmission
   ([MQTT-3.3.1-1]) and a freed slot can NAK it forward instead of waiting out
   AckWait. Its position in the stream's order is not preserved, which is the
   ordinary consequence of any redelivery and is recorded as a residual.
   Then one bounded, batched `lookupConsumersMulti` pass resolves every
   collected durable: **present** (the closed subscription holds its interest),
   **absent** (authoritatively gone → repair), or **indeterminate** (a
   transport failure or unreadable entry → **refuse the CONNECT**; a session
   resumed with silently dead subscriptions is the bug this phase exists to
   fix, and nothing is ever recreated over a consumer that may still exist).
3. **Repair, under a fresh identity.** Recreation never reuses the recorded
   durable name, because that name is an identity to two mechanisms: an
   in-flight register naming `(dur X, seq N)` classifies *Lost* while X is
   authoritatively absent (identifier reserved — the safe direction) but flips
   to a false *Complete* if a new instance of X reaches `AckFloor.Stream >= N`,
   freeing identifiers the client may still hold; and recorded `$JS.ACK`/NAK
   subjects embed the consumer name with per-instance counters. So the repair
   mints a fresh nuid durable and deliver subject, subscribes it closed
   **first** (interest exists before the consumer, so the replacement cannot be
   reaped in the create-to-subscribe gap), retires the stale charge
   (`refundConsumerChargeLocked` — permanently: it named a consumer that no
   longer exists, so no failure path re-adds it), creates the durable
   `DeliverNew`, charges the replacement, and replaces the session's `cons`
   entry. Any failure undoes exactly the phase's own work — phase subscriptions
   removed, created durables deleted (refunded exactly once), replaced entries
   restored **uncharged** — and refuses the CONNECT.
4. **One repair save, still pre-CONNACK.** Ownership is revalidated by the CAS
   at that instant — there is no lease. The two wrong-last-sequence kinds are
   deliberately not equivalent, and the raw store error is captured at the
   store boundary (`saveWithDetail`) because `save()`'s formatting destroys the
   kind: **10071** (the single-writer check) means another broker wrote after
   our accept save — the session was taken over mid-CONNECT — and is disposed
   through `disposeForeignSessionLocked` in a DISCONNECT-suppressing mode (no
   success CONNACK was written, so no packet may precede one). **10164**
   (clustered contention) is a rejection, never a verdict: a bounded re-read
   loop retries the **identical** base sequence and body bytes while the record
   provably has not moved, adopts the landed sequence when the re-read finds
   our exact bytes at a newer one (the response was lost to contention), turns
   a moved record into the foreign disposition, and turns an unreadable re-read
   into an explicitly established pending-write state — a CAS rejection is
   definite, so `save()` deliberately does not mark it uncertain, and the phase
   must do so itself for the deferred settlement to classify or re-attempt anything. An
   **ambiguous** save refuses the CONNECT with the replacements KEPT: the
   settlement re-attempts the phase's own bytes, so the record ends up naming
   them and the next resume verifies them present — nothing further is owed, so
   no continuation is registered. Resolution is
   necessarily deferred — the accept transaction is still open — and no traffic
   is served meanwhile: no CONNACK was sent and every phase subscription is
   closed.
5. **Adopt in place.** After the CONNACK, subscription restoration reaches each
   collected filter through `processJSConsumer`'s restore exists-branch, which
   — on finding a phase-owned delivery subscription — refreshes the MQTT
   metadata and clears the closed flag and the marker **on the same
   subscription object**, under the same dual-lock write the re-SUBSCRIBE
   refresh uses. Re-issuing it through `processSub` would unsubscribe the old
   subscription and install a new one, putting a no-interest gap right back
   into the flow — the exact stream-forced inactivity race the subscribe-first
   rule closes. The phase gate is released last, in the same critical section,
   so every delivery parked on it wakes, re-reads the fields just written, and
   goes out under this filter's live options.
6. **Sweep what restoration did not adopt.** Restoration can legitimately skip
   a collected filter after the CONNACK (a PUBREL setup failure fails a QoS 2
   filter; a raw-subscription failure continues) while the connection stays
   live — and nothing else would ever remove that subscription: it would sit
   closed forever, holding consumer interest, and a later re-SUBSCRIBE's
   metadata refresh does not clear `closed`. So still-phase-owned
   subscriptions are swept immediately after `processSubs` (before its error
   is even tested), and an idempotent deferred fallback covers the exits that
   never reach it (the CONNACK write failure, the 3.1.1 forced-leave failure
   returns). Every reclamation path — the sweep and the phase's own undo —
   releases the gate **before** unsubscribing, so a parked delivery wakes,
   finds the subscription still closed, and skips instead of waiting out the
   bound. A swept filter stays unavailable until the next CONNECT.

**Pre-CONNACK abort discipline.** Every post-bind failure exit before a
confirmed success CONNACK — the register-recovery error, the reconciliation
refusals, the lifetime-grant failure below, the CONNACK size/write failure
(`sendConnAck` now surfaces its write error, so "a success CONNACK was written"
is a checked fact) — goes through one abort helper that discards this
connection's Will and clears the parser's pre-CONNACK connected flag. Without
it, the readLoop would send a v5 DISCONNECT as the **first** packet
([MQTT-3.2.0-1]) and close handling could publish the new connection's Will for
a session that was never granted. The helper deliberately preserves
`c.mqtt.sess`/`c.mqtt.cp`: for a **local** refusal the ordinary close
bookkeeping must still run — it is what detaches the client and
persists/schedules the expiry.

**The requested lifetime is applied only when the resume is granted.** A
resuming CONNECT does **not** overwrite the session's Session Expiry Interval or
Clean flag at rebind, because the accept save that follows would persist them —
and this CONNECT can still be refused. A record describing terms the client was
never told it has is not harmless: with a requested interval of 0 the close path
would discard a persistent session outright (`doClean`), and the client's retry
would find Session Present 0 with its state gone. So the accept save carries the
lifetime the session already had, which is truthful until something is granted,
and the requested values are applied — and persisted, in the same breath — at
the **grant point**, once everything that could refuse the CONNECT has passed
and immediately before the CONNACK. The write happens only when the terms
actually changed, so an ordinary reconnect adds none, and it owes the ordinary
outcome discipline (`grantRequestedLifetime`): a definite failure puts the two
fields back; an ambiguous one puts them back **immediately** and re-applies the
grant only once a readback proves the write landed — because a close happening
while the write is unresolved snapshots the lifetime into its deferred
teardown, so speculative terms there would let a grant that never landed
discard the session, and the landed continuation correspondingly retargets that
snapshot; a 10071 hands the session to the broker that now owns it, with 10164
resolved by the same bounded re-read the repair save uses. A resume
refused before this point therefore leaves the session exactly as it found it,
in memory and in the record, with no repair machinery at all. A session that
was CLEARED rather than resumed — clean start, a legacy clean session, an
overdue record — takes the requested terms at rebind instead: it has no prior
lifetime to protect and its record is created fresh by the accept save.

**Crash windows.** A crash after a create but before the repair save leaves a
durable with a closed subscription that never transmitted and that no record
names: the next resume repairs again, and the orphan sweep reclaims the
abandoned durable after its grace (the sweep's `Created`-age guard covers the
in-flight construction window). A crash after the confirmed save but before
the CONNACK leaves the record naming the new durable; the next resume verifies
it present. A cross-broker takeover between the accept save and the repair
save CAS-rejects definitively (the later broker accepted *on top of* our
accept sequence) — our consumer never transmitted and is swept; the winner's
own resume verified whatever record it read. Register identity is invariant
throughout: recovery precedes the phase in the same CONNECT, and the old name
stays absent forever, so *Lost* verdicts stay *Lost*.

**Why re-SUBSCRIBE needs no verification.** After this phase, every `cons`
entry on a live connection was verified or repaired at its CONNECT — an
indeterminate verification refused the CONNECT, so no connection carries an
unverified entry — and a successfully adopted filter's consumer cannot be
inactivity-reaped mid-connection, because its delivery subscription holds
interest. The qualification matters: a filter restoration skipped (and the
sweep reclaimed) holds no interest and stays unavailable until the next
CONNECT — a live re-SUBSCRIBE cannot recover it, because the exists-branch
never recreates missing interest. Live re-SUBSCRIBE is **not** a recovery
mechanism; the recovery for anything that slips through is to reconnect. Drain
adoption is untouched: a re-SUBSCRIBE that adopts a drain whose durable was
reaped inherits a dead entry — as today — and the **next CONNECT** repairs it
(the adopted entry is back in `cons` and its filter in the subscriptions),
while `recoverDrains` keeps owning drain-side loss reporting. Both stated in
`docs/CONFORMANCE.md`, together with the delivery-start policy: a replacement
is `DeliverNew`, and no backlog is guaranteed recoverable across a reap.

## Outbound in-flight registers

`pendingPublish`, `pendingPubRel`, `cpending` and the packet-identifier allocator
were memory-only. A same-process reconnect therefore re-sent a started QoS 1/2
delivery under its original packet identifier, but a broker restart, a failover, or a
resume on another broker did not: the tracking maps came back empty, the delivery
looked unstarted, and the redelivery went out with DUP set under a **freshly
allocated** identifier — which a client is entitled to read as a new message.
[MQTT-4.4.0-1] requires both sides to re-send unacknowledged PUBLISH *and* PUBREL
packets under their **original** identifiers, so this bounded the broker's
exactly-once guarantee to one process lifetime, for QoS 1 as much as QoS 2.

An **in-flight register** supplies the missing durable fact
(`internal/mqtt/inflight_register.go`): one small record per started PUBLISH-phase
delivery on `$MQTT_inflight`, at `$MQTT.inflight.<idHash>.<gen>.<PI>`, naming the
source durable and stream sequence it came from. Keyed by the identifier, so the
create-only CAS is an allocation arbitration as well as a record, and so one
incarnation is bounded at 65 535 subjects — the same bound, for the same paging
reason, as the inbound hold walk. The record carries an immutable nonce, because the
subject is reused by later exchanges on the same identifier and a stream sequence is
therefore not an identity.

It is a stream of its own for two reasons that are not preferences: `$MQTT_out` is
interest-retained, so a store with no consumer succeeds and keeps nothing, and a
register has no consumer by design; and a per-delivery write to the session record
would put every outbound QoS 1/2 message behind the record CAS and the record fence.

| phase | who owns the identifier |
|---|---|
| PUBLISH sent, not yet acknowledged | the in-flight register |
| past PUBREC | the correlated PUBREL record on `$MQTT_out` (unchanged) |

The handoff is the PUBREL store. On that path the source acknowledgement is
**confirmed** before the register is deleted — the standing rule wherever a durable
record is deleted on the strength of an ack — which keeps the register from being
removed over a source that was never actually released, and so keeps the redelivery's
identifier. Where a register and a PUBREL record name the same source at recovery,
the PUBREL record wins.

An acknowledgement that cannot be confirmed needs one more thing. The source may still
be pending, so JetStream will re-offer it — and its payload is past PUBREC, so sending
it again is a duplicate. The source is therefore **retired**: the next delivery attempt
of it is acknowledged (with confirmation, under that attempt's own reply subject) and
discarded rather than delivered, which both suppresses the duplicate and settles the
message.

The fact is recorded against the **source**, not against the packet identifier, and that
distinction is the whole design. Re-establishing the source's link to its identifier
would make the ordinary in-flight guard suppress the redelivery — but only until
PUBCOMP, because PUBCOMP removes the pending-PUBREL entry that guard tests. From then on
the payload of an exchange the client has already *completed* would simply be re-sent. A
fact about the source outlives the identifier's bookkeeping; this is the same shape the
affine path already uses (its claim's `AffineDone`) and for the same reason.

It is recorded in **both** places, in that order: in memory first, because the PUBREL
whose delivery is already in flight can bring a redelivery at any moment and the
suppression has to be in place before this path spends a round trip; then on the
register, as a `Retired` mark written by a CAS at its exact sequence. The durable half is
not optional. Without it the next resume sees an ordinary PUBLISH-phase register over a
still-live source — indistinguishable from a delivery that never reached PUBREC — and
re-sends a payload the client has already completed. Leaving the source deliberately
unacknowledged is precisely what makes that window reachable.

That mark is not best-effort either, and the reason is worth following. While the
exchange's **PUBREL record** exists a restart is safe whatever the register says, because
recovery reads register-plus-PUBREL as "past PUBREC" — and converts it into the mark it
could not write earlier. So there are two pieces of durable evidence, and the rule is
simply that the second is not destroyed before the first exists: a PUBCOMP does **not**
acknowledge its PUBREL away while the retirement it covers is not durably recorded. The
cost of withholding is a PUBREL redelivery, which [MQTT-4.4.0-1] makes legitimate and
which the client answers with another PUBCOMP — bringing the broker back to retry, paced
by AckWait. Persist before ack, the same rule the UNSUBSCRIBE drain follows.

The source is left unacknowledged at PUBREC rather than retried best-effort: a retry that
landed would settle the message but strand its retirement, since nothing else would drain
it. One discarded redelivery settles the source, releases the retirement, and deletes the
register.

The invariants, in the order they matter:

- **Persist before send.** A PUBLISH under a new identifier goes on the wire only
  after its register is confirmed at a sequence. Between allocation and confirmation
  the entry is *unsendable by anyone*: a concurrent redelivery declines it, the resume
  NAK path skips it, and an UNSUBSCRIBE drain does not enrol it. A definite store
  failure releases the identifier and sends nothing, which is safe precisely because
  nothing could have sent it. An ambiguous store is resolved by re-reading the subject
  and matching the nonce, never by the shape of the response. This is also what makes
  a superseded broker's late register write inert: an identifier the client can be
  holding implies a confirmed register, so it implies the next broker's scan sees it.
- **Reserve before interest.** On resume every identifier named by a register or by a
  PUBREL record is reserved *before* any source interest exists — the barrier runs
  ahead of the CONNACK, both because a client may publish the instant its CONNACK
  lands and because the drain barrier stores PUBRELs under recorded identifiers that
  must already be reserved. This also closes a pre-existing hazard: `pendingPubRel`
  was empty until the PUBREL callback replayed a record, so a fresh allocation could
  mint an identifier an unreplayed PUBREL still owned.
- **An incomplete identifier space withholds allocation, not the connection.** The
  session stops *allocating* new identifiers while everything else keeps working:
  QoS 0 flows, reserved exchanges complete, and drained PUBRELs are still recreated
  under their recorded identifiers. Allocation is the only operation an unknown space
  makes unsafe, and a withheld QoS 1/2 delivery is redelivered by AckWait exactly as
  one withheld at the in-flight cap is. Denying the CONNECT was rejected: it takes a
  whole session offline over a transient failure, and it contradicts the drain
  barrier's contract. Three conditions trigger it, and they are one condition in
  different clothes — the identifier space cannot be fully accounted for: a walk that
  could not complete (identifiers may be in use that were never seen); a register whose
  body is unreadable (the identifier is known, the delivery that owns it is not); and
  two registers naming one source on different identifiers (both identifiers are known,
  but which one the client holds is not, because a stream sequence orders the *writes*
  and not the sends). In each case reserving identifiers is necessary but not
  sufficient — what stays dangerous is issuing a new one, or re-sending a source under
  a guess. A session discard lifts the restriction along with the incarnation whose
  space it protected.
- **Absence is proven, not inferred.** A register is dropped only where its source is
  provably finished — acknowledged, or authoritatively absent while the consumer that
  would have delivered it still exists. A source whose *consumer* is gone is not
  "complete": that delivery was lost rather than finished, and the client may still
  hold its identifier, so the identifier stays reserved. The same rule covers a record
  whose *body* is unreadable: its subject still names an identifier this incarnation
  wrote, so the identifier is reserved and the record kept, rather than deleted as
  uninterpretable. Reading "I cannot interpret this" as "nothing is in flight" is how
  an identifier the client is holding gets handed to a second message — and because
  such a record's *source* cannot be identified either, it additionally makes the
  identifier space incomplete (above).
- **Every terminal path deletes the register**, at its exact sequence: PUBACK, a
  failed PUBREC, the PUBREL store, Message Expiry, Maximum Packet Size, and every
  consumer teardown that abandons started deliveries. A failed delete costs a stale
  record that the store path's reconcile, the next resume scan, or the orphan sweep
  removes; it never wedges an identifier.
- **A surviving register is assumed client-held unless it proves otherwise.** Two very
  different things can leave a register behind: a delivery that really went out and
  whose consumer was later destroyed, and one this session gave up — an allocation
  withdrawn before it was ever sent, or a transmitted delivery whose consumer the
  session abandoned. The first must keep both its identifier and its Receive Maximum
  slot: that limit bounds what the *client* is holding unacknowledged, and a consumer
  disappearing proves nothing about the client, so a session stuck this way is a lost
  message being reported honestly, not an accounting bug. The second must keep neither,
  and what tells them apart is durable evidence with two homes. On the register itself:
  a **cancellation tombstone**, written when the record's deletion could not be proven.
  On the session record: a **void-register verdict**, the register's NONCE in
  `CancelledRegs`, staged by the same transaction that removes the consumer's
  membership. Recovery frees the identifier of a register either one names.
- **The verdict is keyed by nonce, because a nonce is minted once and never reused.**
  Every other candidate identity is recycled: a teardown frees the packet identifier for
  immediate reuse, subjects are reused within a generation, and shared group durables
  are named deterministically so the same name returns on a rejoin — a verdict scoped by
  any of those eventually condemns a genuine later delivery, or resurrects an abandoned
  one, and both were tried and failed here. The nonce is minted with the ALLOCATION, one
  step ahead of the store that writes it, so a cancellation that lands while the CAS is
  still on the wire has something immutable to name. A verdict is removed only against
  proof: the register's confirmed deletion, its tombstone landing, or a cancellation
  settled before any CAS was ever issued — the one place absence is provable. A scan
  that fails to FIND the register deliberately keeps the verdict, because scan-time
  absence cannot rule out a superseded broker's create-only CAS landing late; kept, the
  verdict is self-healing — the scan that finally sees the register deletes it and
  retires the verdict then, including a register found already TOMBSTONED, which is
  proof in hand rather than an absence question. Proof-keyed retirement is also what
  keeps `CancelledRegs` bounded across a client that subscribes and unsubscribes
  forever; the stated residual is one nonce per crash that lands between a register's
  confirmed deletion and the very next session save, since only a fence against a
  superseded broker's late CAS could license retiring that verdict on absence, and a
  time-based fence would rest a correctness invariant on a timing assumption.
- **Its durability is a tracked revision, not an assumed save.** The verdict is often
  recorded by a delivery goroutine while the UNSUBSCRIBE that caused it holds the record
  fence, and that transaction may already have marshalled its record — so the addition
  is refused its own save *and* missed by the save that does happen. A revision counter
  pairs what the set is with what the record provably carries, and every convergence
  point (the packet tail, each register store, each resume, and the settlement of an
  ambiguous save — which re-attempts the very bytes that marshalled the verdict and
  credits the revision they carried) retries until the two agree. Each verdict additionally
  carries the revision of its own insertion, because "the record carries the set" is a
  claim about a MARSHAL: a landed save proves durable exactly the verdicts recorded at
  or before the revision it captured, and a release keyed on membership alone would
  treat a verdict a concurrent teardown added mid-save as proven.
- **A consumer teardown writes the tombstone itself, and holds the identifier while it
  does.** The session record is the wrong home for this fact on the generic teardown
  paths (a QoS downgrade's deferred consumer delete, a forced shared leave, a shared
  release): each runs AFTER its packet's save, so an intent recorded there may not reach
  the record for a while. A tombstone at the register's own subject needs no fence and
  no packet, so it is durable exactly when it has to be — which is before the consumer
  that would classify the register's source is destroyed. The identifier stays RESERVED
  across the in-flight store rather than being freed at once, because a new delivery
  could otherwise take it and write its own register at the same subject, and nothing
  would then tell that record from the tombstone. The write is create-only first and a
  replacement at the exact sequence its own nonce is found at; a foreign occupant is a
  failure, never a success. And because such a tombstone can outlive its owner — where
  the teardown wins the race, the store returns without ever writing — the next
  allocation of that identifier REAPS a cancelled occupant instead of refusing it. A
  record that says "never transmitted" cannot be holding an identifier.
- **The cancellation goes in the write that removes the membership.** Recording it in
  the teardown instead cannot be made equivalent, and keeping the consumer does not
  redeem it: once the membership is gone there is no subscription left to redeliver
  through, so a register that lands afterwards holds its identifier and its Receive
  Maximum slot for the life of the session whatever becomes of the durable — under a
  Receive Maximum of 1, a session that can no longer deliver at all. So every
  transaction that removes a consumer's membership (an UNSUBSCRIBE, a QoS downgrade,
  a 3.1.1 forced shared leave, a SUBSCRIBE rollback) withdraws
  that consumer's in-flight registrations first, and its own save carries both facts.
  The drained sids are exempt because stageDrain already did it and their consumer
  survives. What the teardown itself then finds can only be a registration that arrived
  after the staging, for which the tombstone is written and the obligation is otherwise
  left to the convergence points — the consumer is deleted anyway, since keeping an
  unreferenced durable nothing reaps makes no identifier recoverable.
- **The delivery barrier comes down before the snapshot.** Staging is only a moment in
  time otherwise: the delivery callback stays live until the packet commits, so a
  message arriving after the snapshot allocates an identifier whose cancellation misses
  the write that was supposed to carry it. Marking the doomed consumer's delivery
  subscription closed first — under the same lock the snapshot is taken with, and the
  same lock the callback tests it under — is what makes the snapshot complete rather
  than current. A rollback lifts the barrier again, because a packet whose write did not
  land leaves those subscriptions the client's. Two details of that reversal are
  load-bearing: it is scoped per FILTER, because one filter of a SUBSCRIBE can fail
  while the packet succeeds and its membership then survives unchanged; and both halves
  of a multi-level filter go through the same staging helper, since the level-up
  companion is a consumer in its own right.
- **A teardown's register removals walk one durable ladder.** Most register deletes may
  be fire-and-forget, because the record left behind names a source that stays live and
  classifiable — the store's reconcile, the next resume, or the orphan sweep removes it.
  A consumer teardown breaks that premise on purpose: it is about to delete the very
  consumer that would classify those sources, after which a surviving register is
  deliberately not read as complete. So its removals run BEFORE the consumer goes and
  walk a ladder whose rungs are ordered by strength: (1) the confirmed delete, (2) the
  cancellation tombstone at the register's exact sequence, (3) the nonce verdict
  persisted with the session record. A proven rung ends the story and forgets the
  verdict; the first two rungs share a storage domain (`$MQTT_inflight`) and usually
  fail together, which is exactly when the third matters. The one exception is a
  **retired** QoS 2 register: its record is not a claim on an identifier but the durable
  *suppression* of a completed-but-unproven exchange, so its cleanup is delete-or-leave
  — a tombstone or a verdict would have recovery drop it without reinstating the
  suppression, and a crash before the consumer delete would then resend the completed
  payload. Left alone, recovery re-reads it as a retirement; the residue when the
  consumer is also gone is a parked retirement, which reserves no identifier and no
  Receive Maximum slot.
- **A withdrawn identifier is released only settled AND proven.** The reservation a
  teardown leaves on a cancelled allocation guards two different hazards, and both
  release conditions are load-bearing. Until the original store SETTLES, the identifier
  is not free even with a durable verdict: a new allocation could take the subject, reap
  what it finds, and then race the old create-only CAS onto the same subject. And until
  some rung is PROVEN, a settled identifier is not free either: released with nothing
  durable calling the register void, a crash resurrects it as an ordinary delivery
  holding a Receive Maximum slot forever. The settle points (the store's confirmation or
  abandonment) release on proof — including proof a rung established EARLIER: a securing
  tombstone retires the nonce while the store is still out, and a settle point that then
  parked the reservation pending a save would park it forever, since a retired nonce has
  nothing left to save. A settled-but-unproven reservation is released by the save that
  finally proves the verdict. Release is checked at EVERY edge with one shared
  predicate, because the proofs and the settlement race in both directions: a verdict
  durable before the settle leaves no later save to release (the settle itself must
  recognise the record's proof), and a stream-rung proof after the settle retires the
  nonce and with it the membership a save-keyed release would need (the retirement
  itself must release the settled reservations — through a small nonce-keyed index,
  since retirement runs once per voided register and a pendingPublish walk per nonce is
  quadratic teardown work). And every one of those touches is keyed by the toucher's OWN
  nonce, read under the lock that allocated it: a store can stall across its
  allocation's cancellation, release and the identifier's reuse, and the reservation it
  then finds belongs to a newer allocation whose own CAS is still on the wire — not the
  stale settler's to release.
- **Rollback withdraws verdicts on transmitted deliveries, and only those.** A packet
  whose save DEFINITELY failed keeps its subscriptions, so its transmitted deliveries are
  live and still owe their identifiers — a standing verdict would have the next restart
  free identifiers the
  client holds, the reverse of the bug and a worse one. A CANCELLED allocation is never
  reinstated: nothing was transmitted, the source simply re-offers under a fresh
  allocation, so its verdict stands on every branch.
- **A drained exchange's release accepts both "already absent" answers.** The drain
  holds its register's sequence in its own persisted state and releases it — confirmed,
  before the claim removal and the ack — in its finalizer. That retry must read BOTH
  authoritative shapes of "the sequence is already gone" as success, or a crash between
  the delete and the claim save would withhold the exchange's completion forever.
- **Ownership transfers explicitly, it is never assumed.** Three teardown paths take a
  register out of the ordinary terminal path's reach, and each takes the delete with it.
  A **drained** QoS 2 exchange holds its source claim until its PUBREL is proven, so
  nothing else reaches the delete: the drain records the register's sequence in its own
  state (persisted, so it survives a restart) and releases it — confirmed, before the
  claim removal and the ack — in its finalizer. A **consumer teardown** releases both the
  registers its `cpending` entries name and those of its *retired* sources, which are not
  in `cpending` at all. And an **UNSUBSCRIBE that races a first-delivery register write**
  cancels that allocation rather than skipping it, so the store's own ownership
  revalidation refuses the send instead of putting a PUBLISH on the wire whose tracking
  the UNSUBSCRIBE has just destroyed. A register left behind by any of them outlives its
  consumer, and a consumer-gone source is deliberately not read as complete — so the next
  resume would reserve that identifier for the life of the session.
- **A surviving disconnect's shared purge derives its targets from the pending state, not
  from the group membership.** The purge exists to drop this session's pending group
  deliveries without acking them, so a resume does not NAK-steal messages AckWait would
  have redelivered to other members. Neither view of the group can name what it has to
  reach. A session restored from its record has the membership but no configs — shared
  consumer configs are deliberately excluded from the persisted `Cons` and rebuilt only by
  the post-CONNACK subscription restoration — while register recovery has already rebuilt
  its pending entries under the deterministic group durable names, so a config walk finds
  nothing. A SUBSCRIBE whose rolled-back save definitely failed leaves the opposite: the
  re-added configs are live and their group key is not back in the membership, so a
  membership walk misses them. And a 3.1.1 forced leave and an UNSUBSCRIBE with no config
  to tear down each strand a TRANSMITTED delivery's entries
  on purpose — the staging records the verdict and leaves the entry "for the teardown's
  purge" — with NEITHER view left naming the durable, which is why even the union of the
  two is not enough. So the purge takes every durable named by the maps it is about to
  remove from (`cpending`, the withheld markers, and the retired sources) whose name
  carries the shared-group prefix. For broker-minted state that name IS the protocol fact
  "deliveries under this consumer are group deliveries": the shared names are minted in
  one place and no other durable class can collide with the prefix (plain consumers are
  hash-and-nuid, PUBREL and session-affine QoS 2 copies carry their own prefixes, and a
  drained consumer keeps its plain name). For state restored from a register the name is
  trusted rather than re-derived, the same trust every other restored identity gets.
  Re-purging an entry a removal already voided is idempotent end to end: a known nonce is
  not re-voided, the delete is at the exact sequence, and both release predicates key on
  the toucher's own nonce.
- **Both surviving closes run the purge, and the DEFERRED one runs it as a single
  classified write.** A close whose own record write was unresolved is parked and applied
  later, and that path cannot simply call the purge: the purge's third rung writes the
  session record, and an independent write of its own can land ambiguously — after which
  every later write on the session is refused, including the disconnect time's, with the
  parked action already consumed and nothing left to retry it. The record would then say
  `DisconnectedAt=0`, i.e. connected, while the deadline existed in memory only: the local
  timer fires, session expiry finds the record bumped past its stale sequence and delegates
  to the record, and the record reads as "connected, nothing due" — leaking it until a
  reconnect that may never come. So the deferred close opens the record transaction BEFORE
  its first mutation, stamps the ORIGINAL disconnect time, runs the purge in a form that
  only STAGES (the register-stream rungs, then the nonce in memory), and makes ONE
  classified save carrying both. An ambiguous save needs no continuation at all: the
  deadline and the verdicts are in the bytes it attempted, and the settlement re-attempts
  exactly those, crediting the revision they marshalled; a definite failure keeps the
  in-memory purge and the armed timer and loses only restart-survival of the deadline,
  which is the ordinary close's posture too. The Will's fate is decided on a FRESH clock
  sample rather than the one taken at entry: the branch's rungs and its save are round
  trips, and a deadline that passes during them would otherwise read as still future.
  An already-due Will enqueued into the outbox is cancellable, so a reconnect arriving
  after the deadline could suppress a Will that was already owed — precisely what the
  direct publication exists to prevent. And because the parked action is CONSUMED
  before the discharge runs, the discharge re-checks that the record state is still
  resolved — once cheaply on entry and again with its own fence held, which is the sound
  test since the fence is what serializes writers — and RE-PARKS the untouched action if
  it is not. Holding the ownership gate does not exclude an off-readLoop writer: a
  register store settling between the resolution and the fence acquisition can leave its
  own write ambiguous, and applying the close anyway would have every write refused with
  nothing left holding the original deadlines. The DEFERRED DISCARD branch cannot hold a
  fence across `clear()` — that call ends the record generation and must own its
  transaction — so `clear` performs the late refusal itself: a re-check WITH THE FENCE
  HELD, before the first deletion, releasing without ending the generation because nothing
  was mutated, and returning a distinct sentinel. The branch re-parks on that sentinel
  alone and never on "is the session uncertain now", because any other failure means a lost
  teardown claim or a half-done teardown, where re-parking would resurrect an obligation
  that is not this broker's to discharge.
- **`clear()` always refuses before its first deletion on an unclassified sequence.** That
  is the delivered invariant, and it is unconditional: both refusals — at entry and with the
  fence held — always carry the sentinel. What each caller then does is NOT uniform, and
  is enumerated below rather than summarized: the shared recovery can decline, so this
  section deliberately asserts no universal recovery.
  The refusal was once opt-in for the deferred discharge alone, because no other caller
  could act on it; three more now can — four of the six call sites in all — so the flag
  was a mechanism reading
  "true" at every site it could reach. The refusal alone is not enough either:
  "never tear down on an unclassified sequence" would otherwise become "strand the object
  while its record, its consumers and the shared group durables it owed a release all
  survive", because those callers evict before clearing. The ORDINARY CLEAN CLOSE and
  SESSION EXPIRY share one helper
  (`reparkRefusedDiscard`): it parks a discard-only obligation — `expiryInterval` 0, so the
  discharge takes the deferred discard branch above — and then RESTORES RESIDENCY, which is
  what every resolver needs, since the retry pass and CONNECT both scan the resident
  sessions and the expiry sweep looks at no `ExpiryInterval == 0` record at all. Park
  first, publish second: the obligation must exist before the object is reachable. The WILL
  is deliberately NOT parked — a parked Will is memory-only and a remote takeover drops it,
  regressing a Will the broker delivers today — so the close falls through to
  `mqttHandleWill`, which publishes immediately for the expiry-0 session a discard always
  is; expiry has no Will, its own was discharged by the close that started the deadline.
  The helper ARBITRATES NOTHING against an obligation already parked: it accepts only the
  shape it would have written itself (ends at disconnect, no Will), never mutating or
  replacing it, and declines every other shape back to the caller's historical
  evict-and-log, because a parked action carries no owner identity and nothing there can
  prove whose deadline or whose Will a retarget would overwrite. CONNECT's clean start
  instead REJECTS WITHOUT EVICTING, the same answer its early path gives: it has a client
  waiting for a CONNACK, not an obligation, and the retry's own uncertainty check resolves
  the session. Session expiry also stops reading a higher record sequence as a remote owner
  while its own write is unresolved — an ambiguous write that LANDED with its response lost
  produces exactly that shape, and evicting there would discard the only object that can
  classify it — so the decision is deferred to the digest classifier, and it is made inside
  ONE record transaction spanning the load, the `sess.seq` read and the decision, since a
  mutex sample alone is check-then-act and arming uncertainty requires the fence. The two
  callers built from a record just read (the record-based expiry, the restore path's
  overdue discard) cannot reach the sentinel at all: nothing writes through those transient
  objects before the discard, so no save can have armed uncertainty. They keep the
  unconditional refusal as a fail-closed invariant and carry the proof as a comment. What
  the recovery does NOT make durable is stated in `docs/CONFORMANCE.md`: the obligation is
  memory-only, so a crash or a remote takeover in that window drops it.
- **Every landed record write credits the verdicts it marshalled.** Durability of a
  verdict is a property of the write that carried it, not of the intent of whoever was
  persisting, so the credit lives at the write: `save` samples the void-register revision
  in the same critical section that marshals the record, and a CONFIRMED store advances
  the saved-revision watermark to that sample and releases the reservations it frees. The
  same sample travels with the attempt's identity, so every settlement that PROVES those
  exact bytes durable credits it — a read finding them at a newer sequence, the resolver's
  own re-attempt landing them, and each landed classification of the contention retry —
  none of which passes through the confirmed-store path. Never on an unresolved,
  definitely-failed or FOREIGN outcome: a record that is not ours
  carries nothing of ours. Crediting only at the paths that set out to record a verdict
  left the other half open — a reconnect's accept save inherits the staged verdicts and is
  not a verdict path at all, so it made the fact durable while its identifier stayed
  reserved in memory with nothing due to look again. The one deliberate exemption is the
  accept ROLLBACK's raw write: its snapshot is built inside the rollback, so a nonce staged
  after it is not in the bytes that land, and crediting the then-current revision would
  claim exactly that nonce durable (`docs/CONFORMANCE.md` states what happens to it).
- **A lock release is what re-checks a verdict staged behind somebody else's lock.** The
  stager and the writer are not the same thread of control: a register store settling
  off-readLoop stages its nonce and finds its own persistence refused, because at that
  instant the record fence — or the ownership gate a resolver needs — is either held by
  somebody else, or free but RESERVED for an announced blocking writer under waiter
  priority. The second cause needs nobody to hold anything: a blocker announces itself,
  stalls before `sess.mu`, and every non-blocking acquisition refuses meanwhile — with that
  waiter's eventual release as the retry edge. A BOUND session
  converges through its own packet tails; an UNBOUND one has no traffic, and a
  never-expiring one no timer, so the identifier could stay reserved for the life of the
  session. So both canonical releases re-check: the RECORD FENCE release, sampling the
  decision inside its existing critical section (after the fence is cleared, before the
  unlock — a decision made after the unlock does not compose atomically with staging), and
  the OWNERSHIP GATE release, at both forms — `unlockSession` and CONNECT's own inline
  delete, since a CONNECT does not release through the former. The re-check fires only for
  a session that is resident, unbound, not uncertain (the uncertainty machinery owns that
  retry, and its resolver's releases re-check afterwards), not mid-transaction, and whose
  revision is above the HANDOFF WATERMARK. "Not mid-transaction" is also where waiter
  priority applies: an ANNOUNCED blocking writer suppresses the trigger outright, because
  the attempt's own acquisition is a non-blocking one and taking the fence there would be
  exactly the barge waiter priority refuses — while an announce landing after the
  release-time load produces a trigger whose inline re-check then defers instead. Either
  way the deferral is a handoff, not a strand: that waiter's own release re-checks, and
  provided blocking arrivals are finite the chain reaches a release with none announced.
  That watermark advances when an attempt ACQUIRES the fence, never when a release merely
  decides to try: a trigger that lost the fence to a waiter it just signalled stored
  nothing, and burning the watermark on it would strand the very nonce it fired for — the
  fence winner's own release is the next re-check. One attempt per newly staged revision
  makes the sequence of attempts finite; keeping it to one STACK FRAME is a separate
  property, and a LOOP is what provides it. The releases inside an attempt would otherwise
  re-enter the decision in their own frames — once per late staging, each frame holding a
  JetStream round trip, so a session with many outstanding register stores could build a
  stack proportional to them. So an attempt marks the session as having a handoff in
  progress, the releases route work to that loop instead of recursing, and the flag is
  cleared only in the same critical section that finds nothing left pending, which is what
  keeps a staging racing the exit from being dropped between the two. A loop that gives up
  on GATE contention hands that ownership back inside the gate's own critical section, for
  the mirror-image reason: a gate owner deletes its entry under the manager lock and only
  then asks whether a loop is running, so a hand-back made after that lock was released
  could be seen as still running — the owner suppresses itself, the loop exits, and the
  staged revision has no successor at all. **The releases alone are not the whole liveness
  argument**, and the gap is the watermark's own doing: a DEFINITE store failure leaves
  the revision AT the watermark, so no release triggers again, and an unbound
  never-expiring session may never stage another nonce to lift it. So the periodic pass
  carries a BACKSTOP — for a resident, unbound, resolved session with an unsaved revision
  it puts the watermark back below it and runs the ordinary attempt. Re-arming rather than
  bypassing keeps one path to the store, and the sweep interval is the retry cadence,
  which is the right one for a failure that did not look transient. The backstop re-arms
  WITHOUT regard to the waiter count, deliberately: the attempt it drives still refuses
  while a blocking writer is announced, and the re-armed watermark is precisely what lets
  that waiter's own release trigger the store instead of waiting a whole further sweep
  interval. So the backstop is a liveness carrier past a definite failure, not an
  unconditional one: while blocking writers keep arriving, each attempt defers again. The
  attempt itself runs UNDER the ownership gate, taken fail-fast and skipped on contention:
  gated, any ambiguity it creates exists only while the gate is held, so a CONNECT that
  ever holds the gate sees it at its own uncertainty check and resolves it — ungated, an
  ambiguity landing just after that check would have the CONNECT bind, find its accept
  save refused, and evict the object holding the continuation. The unbound scoping is
  equally deliberate: fired at CONNECT's accept-transaction release it could inject
  uncertainty right behind a success CONNACK, with the gate held and no packet tail yet to
  resolve it.
- **A resume claims the shared registers it recovers before installing them.** A
  cross-broker takeover constructs a new session object but preserves the session
  incarnation generation, so that generation correctly finds the durable state but cannot
  distinguish the two owners. Each object therefore has an unpersisted register claim.
  Every register write stamps it, and recovery CAS-rewrites each readable live or retired
  shared register with its claim at the exact sequence it loaded — before the
  duplicate-source grouping, so that even the members of a group that fails closed have
  moved: a reservation protects nothing while the registers behind it still sit at
  sequences a stale close knows, and deleting one would let the next resume install the
  survivor as authoritative, resolving by deletion the ambiguity the group refused to
  guess about. A live member whose claim fails — a concurrent resumer owns it, or it is
  unreadable — keeps its source identity and fails its whole group closed for the same
  reason: installing the sibling as a singleton would resolve the duplicate by the same
  guess through the split-claim door. The single-slot subject
  moves, invalidating every sequence a stale close knows. If the stale close deleted first,
  or its matching-nonce cancellation tombstone won, recovery consumes that durable close
  decision and does not install the identifier. The negative dispositions also reconcile
  a REUSED object's memory, because "in-memory state wins" holds only for records
  recovery would re-install: a consumed close decision retires the live entry it backed,
  and a poisoned group demotes a live member's entry to a bare reservation and removes
  its source mapping — each touch guarded by the record's own nonce, so an entry a newer
  allocation owns is never disturbed. A reservation left by a LOST arbitration is marked
  foreign: its sequence may be the rival's, and no terminal path settles a record this
  object does not own — the completion path's register delete carries no nonce or claim
  check, so the mark is what keeps a stale object's late acknowledgement off the rival's
  only durable identity. The acknowledgement paths decline a foreign reservation before
  any state moves: a successful PUBREC would otherwise store an uncorrelated PUBREL from
  a bodyless entry, and a later resume would then hold an identifier-first record against
  the rival's still-live register. The reservation stands, nothing durable is written,
  and the owner the client ends up bound to answers the retransmission. A READABLE live
  record under an active rival's claim, observed mid-barrier, is stronger than any of
  this: the rival claims only after its own accept save, so it is durable displacement
  proof, and the RESUME IS REFUSED — nothing installs, the CONNECT aborts, and the
  client's retry binds to the authoritative owner. A dead rival's standing claim never
  reaches that re-read (the ordinary CAS at the loaded sequence re-claims it), so
  foreign reservations arise only from unreadable records, which prove nothing and stay
  fail-closed. There is no own-claim fast path: every shared register is rewritten on
  every barrier run, because the moved sequence is what turns a terminal path's paused
  register delete into a no-op — a same-object rebind between an acknowledgement's two
  halves legitimately reinstalls the entry, and the paused durable half must not then
  complete the exchange invisibly. Registers dropped on proof of completion reconcile
  memory exactly as consumed closes do (cancelled reservations exempt), so such an entry
  is released at the next barrier run rather than counting against Receive Maximum
  forever. An own-claim re-read is adopted only when the sequence has MOVED from the
  attempted one: without a fast path the loaded record was own-claimed already, so an
  unmoved re-read proves nothing about the write and is retried — the move is the
  fence — and both exhausting those retries and an INDETERMINATE re-read after a failed
  CAS refuse the resume rather than reserving at an unproven sequence. Every contest
  therefore ends owned-with-a-moved-sequence, consumed as a close decision, opaque from
  an unreadable (and therefore rewritten) record, or refused; no outcome leaves an
  installed entry at a sequence a paused terminal path still holds. Claims are ordered
  by their accept save's record sequence — unique by
  record CAS — so a contest between two live resumers has exactly one newer side: the
  newer takes over an older claim by re-targeting its CAS, the older reads displacement,
  and the mutual-refusal loop two unordered claimants could otherwise enter cannot
  happen. The contest budget is generous and exhausts to displacement, never to an
  opaque reservation: a fixed budget cannot bound unbounded contenders, and the newest
  owner stranded behind a chain of older claims would otherwise complete CONNECT with
  its allocation withheld. Every
  nonce-keyed reconcile on a shared
  durable — ambiguous first store, retirement, cancellation and pending cancellation —
  must also match the current claim before it may adopt a sequence or rewrite the record,
  and so must the store's same-delivery replacement, the one reconcile whose occupant
  carries the OTHER writer's nonce and which no nonce gate can therefore cover. A
  mismatch abandons the stale operation without producing a competing session-record
  verdict. The complete-source reap is deliberately claim-independent: a provably
  complete source means the exchange terminated, and continuity protects only
  incomplete exchanges. Plain registers are not claimed and therefore not claim-gated;
  applying one side without the other would turn a resumed owner's own plain records
  into foreign state.
  The ordinary exact-sequence delete needs no added read: the claim rewrite is the fence,
  and a delete holding the pre-claim sequence removes nothing. Thus each shared register's
  close-versus-resume decision is durable and consumed once at its own subject sequence.
- **Registered if and only if transmitted.** A delivery withheld by the in-flight cap
  was never sent, so it is never registered. A retained-message send has no JetStream
  backing at all, so it has no register and no post-restart redelivery.

A session discard purges the retiring incarnation's registers by filter. That blind
purge is correct here and the contrast with the arbitrated inbound hold walk is the
point: a register has no second party that could still be committed to acting on it,
and the filter names a generation `clear()` has already re-minted.

**Cost, stated plainly.** One create-only CAS round trip per *first* transmission of
every outbound QoS 1/2 message, one delete per completion, and on QoS 2 a confirmed
rather than fire-and-forget source ack. Redeliveries and recovered deliveries pay
nothing. Batching the *writes* was rejected because it would delay first transmissions
behind a batch, and registering *after* sending is not a cheaper version of this design
but a reintroduction of the deviation as a narrower window.

**Resume reads are batched, and that is a correctness-adjacent property.** The barrier
runs before the CONNACK, and a session may legitimately hold up to 65 535 in-flight
identifiers, so one request per record would put tens of thousands of serial round trips
in front of every reconnect — pushing restart recovery past ordinary client timeouts and
making failover impractical for exactly the sessions that need it most. Instead the
registers are enumerated by subject in one request (the identifier's 16 bits bound the
filter below JetStream's subject-detail truncation point, which is why this enumeration
is safe here although the account-wide orphan sweep refuses it) and loaded in bounded
batches; sources are then classified once per *source*, with one consumer lookup per
durable and batched message reads. Shared-register claim writes are likewise pipelined in
a bounded concurrent window; their subjects are distinct and carry no ordering dependency,
so serializing them would restore the same pre-CONNACK latency problem. Every uncertainty
is preserved exactly as the one-at-a-time path preserved it: a missing response, an
unreadable consumer and a
consumer that is gone are all "unknown", never "complete".

An unreadable register cannot participate in claim arbitration: its body cannot be
rewritten while preserving the allocation identity, so a stale object may still know its
sequence. It remains in the existing loud, fail-closed opaque state — the identifier is
reserved and new allocation is withheld — rather than being guessed owned or closed.

## Session incarnations and teardown

A client ID hashes to a stable token, and a session's JetStream resources used to
be named from that hash alone. But the hash is reused: a clean start, a session
expiry and a takeover-discard each destroy one session and let a later one adopt
the same hash. A deterministic name is therefore **not an identity**, and a
teardown issued by a superseded broker could delete a resource a fresh session had
just created.

**Incarnation generations** (`internal/mqtt/sess_incarnation.go`) fix that
structurally. Each session carries a persisted generation (record field `gen`) that
scopes every otherwise-hash-only name: the PUBREL durable, its `$MQTT_out` subject
and its delivery subject; the `$MQTT_qos2in` hold subjects and the filters a
discard purges; and each `$MQTT_willq` outbox record. It is minted when a session
is first created and re-minted by `clear()` — which every discard path runs,
including the two that *reuse* the session object as a fresh session — while a
plain reconnect or resume **preserves** it, because a resumed session must still
find its own PUBRELs, QoS 2 holds and Will. A late delete from a retired
incarnation can then only name something that is already gone.

This is the same conclusion the QoS 2 move protocol and the affine delivery path
reached independently, and it is why the fix is naming rather than a lock: a nonce
re-check before each delete is a check-then-act, and no in-process lock arbitrates
anything against another broker.

**The teardown claim** (`internal/mqtt/sess_teardown.go`) covers what naming
cannot: two brokers tearing down and resuming the same record at once. Before
deleting anything, a discard CASes a `Teardown{Origin, Nonce, StartedAt}` marker
onto the record at the sequence it resolved against, preserving the record's
content so any broker can finish the job and so the orphan sweep still reads its
durables as referenced. Losing that CAS means a newer owner exists, and the loser
deletes **nothing** — a complete outcome, not a failure. Releasing is a
sequence-targeted delete whose result is classified by **re-reading**, because
`MaxMsgsPer: 1` may already have removed that sequence on a peer's behalf: absent
means done, our own nonce means retry, and a foreign nonce or no marker means a
newer owner exists. The release is always confirmed even on the no-wait discard
path, since an unclassifiable release is not a release.

Nobody resumes a marked record. Every CONNECT that can interpret the record (and the
record-driven expiry path) **finishes** the teardown when it finds a marker —
repeating the idempotent, generation-scoped deletions and releasing the record — and
only then starts a fresh session with a fresh generation and Session Present false.
That also makes a crashed teardown self-healing instead of leaking until the orphan
sweep. The exception is a record the restore boundary refuses outright — a subscription
in the reserved `$MQTT.>` namespace, or a missing or malformed generation: the CONNECT
is refused without adopting the marker. What finishes it then depends on which refusal
it was, and only one of the two is recoverable:

- **Reserved namespace, generation valid**: the expiry path and the orphan sweep's
  adoption both still act on the record — the deletions they issue are
  generation-scoped and the record's generation is sound — so the marker is finished
  and the state is reclaimed without a CONNECT.
- **No valid generation**: nothing adopts it. `expireSessionFromRecord` refuses to act
  on a record it cannot validate, `mqttValidateTeardownTargets` rejects it through
  `mqttValidateSessionRecord`, and the orphaned-consumer sweep counts the consumers the
  record names as *referenced*, so they are not reaped either. The record, its
  consumers and its marker persist until an operator recreates the `$MQTT_*` streams.
  That is the pre-existing consequence of the generation refusal — there is no
  generation to scope a safe deletion by — and it is why both refusals prescribe
  recreating the streams rather than offering a way to work around the record.

Shared group durables stay outside all of this: they are not owned by one session
and are released last, by the existing last-holder record scan, only once this
session's record is gone.

**Every session carries a generation.** A record without one is **rejected** at the
boundary rather than resumed on hash-only names: it predates this design, and under
the compatibility boundary above the development store is recreated. Accommodating
it would not be a smaller version of the same safety — a hash-only session has no
identity to scope an arbitration with, so the inbound QoS 2 release claim could not
be arbitrated for it and the discard would fall back to a blind hash-wide purge,
which destroys a held payload whose releaser claimed it and then died. A
partially-arbitrated path is worse than a rejected one.

**Mixed version.** An older broker ignores the `Teardown` marker and would
overwrite it, and uses the old names — so, as with the affine protocol, both halves
hold only once every broker understands them. See `docs/CONFORMANCE.md`.

## Orphan reconciliation

A broker that dies in the wrong window leaves durable state nothing else ever
revisits: an inbound QoS 2 hold and its release claim on `$MQTT_qos2in`, a
session-affine `$MQTT_Q2AFF_*` consumer on `$MQTT_out`, an assignment claim on
`$MQTT_qos2assign`, and the teardown marker that pins all three in place. None of
them loses a message — a stalled claim is honored, never released — but all of
them accumulate. A periodic per-account sweep converges them
(`internal/mqtt/orphan_qos2_sweep.go`), beside the existing session-expiry and
orphaned-consumer sweeps.

**Teardown markers are adopted first.** A marked record deliberately reads as a
reference to everything it names, so until it is finished none of that session's
QoS 2 state can become eligible. Once the marker has been *stably observed* for the
grace — keyed by the record's sequence and teardown nonce, so a resave or a peer's
teardown restarts the wait — the sweep re-reads it, requires that same sequence and
nonce, and runs `finishAdoptedTeardown`. No stored timestamp participates: neither
the record's store time nor `Teardown.StartedAt`. A young marker, an unreadable
record or a failed adoption leaves everything referenced.

**Absence is proven, never inferred.** Candidates are listed *before* a complete
walk of `$MQTT_sess`, so a resource created between the two is never judged
against a snapshot that predates it; local in-memory sessions count as references,
covering the create-before-save window on this broker. Ownership is the
`(client-ID hash, generation)` pair carried in the subject or durable name, never
the hash alone. An incomplete walk, an undecodable record, a client ID that does
not hash back to its subject token, or an unvalidatable generation aborts the
whole destructive pass.

**Two grace policies, and neither is an age.** `mqttOrphanSweepGrace` (five
minutes) covers construction windows — a resource created just before the record
save that references it — and applies to teardown markers, holds, assignments and
affine consumers. `mqttQoS2ReleaseOrphanGrace` (one hour) applies to release
claims, which arbitrate against a releaser that may be paused between loading a
payload and delivering it.

**Nothing subtracts two timestamps.** There is no clock to compare against: a
JetStream store time is assigned by whichever server led that stream's Raft group
at that instant, four streams are four groups, and one stream spans several clocks
across a leadership change — so a fast leader's timestamp minus a slow leader's
says nothing about elapsed time. Comparing either against the bridge's own
`time.Now()` is worse, for the reason the release claim's self-reported `At` was
rejected: the bridge is an external client on its own host.

So a grace is a **stable-observation requirement** measured on this process's
monotonic clock. A candidate becomes eligible only once it has been seen,
unchanged and authoritatively orphaned, across a monotonic span at least as long
as the grace — and **never on the pass that first saw it**: the first valid
observation only records the identity, and a later complete authoritative pass is
what may qualify it. Any identity change restarts the wait (a record's stream
sequence, a consumer's durable plus `Created`, a teardown nonce), as does the
resource becoming referenced again. Stored timestamps are diagnostic only. Failed
or incomplete scans record nothing and authorize nothing, and a mutation is still
revalidated at the moment it happens, separately from that eligibility check.

The observation table is bounded, and when it is full it **refuses new entries
rather than evicting**. Evicting is what a cache does and is exactly wrong here: a
deterministic scan revisits candidates in the same order, so admitting each new key
by evicting the one due next would make every sighting look like a first sighting
and nothing past the cap could ever qualify. Refusing keeps a stable resident
cohort that ages, is acted on, and releases its slot; entries nobody has revisited
in a very long time are pruned so a vanished resource cannot hold a slot forever.
State that is **retained by design** — a `Lost` assignment, or a `Discarded` one
whose source is not yet proven closed — is resolved before any observation and
holds no slot at all: it needs no wait, since it is already durably terminal, and
it is exactly what accumulates. Letting it occupy the table would let one account
fill it and refuse every other candidate permanently.

Two consequences, both erring towards leaving state alone: a broker that restarts
more often than the grace never reaps, because quarantine is in-memory and starts
over; and convergence takes several passes rather than one.

**The hold dies before its fence.** A `discarded` marker is reaped by nobody at
teardown time — it is the only durable proof that a late releaser must lose — and
the sweep may retire an aged one **only** after destroying the hold record and
confirming it gone. A releaser that preloaded the payload and resumes afterwards
then wins a fresh create-only claim against an empty subject, but its mandatory
authoritative re-read returns `mqttQoS2VerifyGone`, so it delivers nothing.
Reversing the two steps returns `mqttQoS2VerifyHeld` and fans the message out
again. An unclaimed hold is fenced by a fresh `discarded` marker before it is
purged, and every claim delete targets the exact sequence just read.

A record that cannot be *read* is still a fence — the claim write is a create-only
CAS, which fails while any record occupies the subject — so an aged unreadable
claim retires by the same route: purge behind it, then delete its exact sequence.
It is deliberately not deleted first and replaced with a fresh marker, because
that empties the subject while the payload is still there.

**One definition of an interpretable record, shared by every path that reads one.**
The reference walk fails closed on a record it cannot interpret, but the
post-snapshot re-reads are what actually authorize a mutation — so they apply the
same validation, not a subset of it. A record written after the walk whose client
ID does not hash back to its subject, whose generation is malformed, or whose
affine pair is inconsistent is **indeterminate**, never proof of absence: reading
it as "a valid record for a different generation" would discard a live assignment
or delete a referenced consumer along with its interest-retained copies.

**Every deletion target is validated against the record that names it.** A teardown
adoption deletes the `Cons`, `Drains`, `PubRel` and `Q2Aff` durables a marked record
carries, all copied verbatim, so a record naming another session's durable would
have that consumer deleted and its interest-retained messages with it. Each name is
bound to its session — a plain durable is `<idHash>_<nuid>`, the PUBREL and affine
ones are fully derived — and adoption checks all four. An affine consumer is
additionally re-looked-up immediately before deletion and must still be the same
instance quarantine observed: one deleted and recreated under the same name is a
different consumer, and deleting it by name would destroy a peer's fresh consumer
inside the very create-before-save window the wait exists to protect.

**The affine side is the mirror image.** An assignment is committed `Discarded`
*before* its consumer is deleted, because `$MQTT_out` is interest-retained and
deleting the consumer drops the copies it still held. The consumer is deleted only
after a complete assignment inventory, a second complete inventory as a
verification barrier, and a final re-read of the session record; a capped or
failed scan authorizes nothing. Assignments are deleted only through
`reapQ2AffClaim`, whose existing predicate refuses a claim marked `Lost`.

No stream gains a `MaxAge`: JetStream age eviction cannot perform this ordering or
these cross-stream proofs, so protocol state stays explicitly reconciled (every
stream's startup validator now rejects a global `MaxAge` — see "Stream
prerequisites"). The
inbound walk is bounded per tick by both a record count and a byte budget (the
message-get API returns whole records, so scanning a hold transfers its payload),
with a cursor seeded pseudo-randomly once per process and advanced sequentially
from there — random so a restart loop does not rescan the same prefix forever,
sequential so coverage inside one process is linear. The cursor is scheduling
state only.

## JetStream domain

The broker is a plain client of exactly one external JetStream, so `-js-domain`
(`MQTTNB_JS_DOMAIN`) is the single source of truth. There are no leafnode or
per-account topologies to resolve between, unlike the NATS Server this code came
from. Empty -- the common case -- means the domain of the server the bridge is
connected to.

The value is used in two places that must agree:

- **JS API subjects.** Requests are rewritten from `$JS.API.<x>` to
  `$JS.<domain>.API.<x>`.
- **Session subjects.** Sessions are stored at
  `$MQTT.sess.<domain>.<client-id-hash>` in `$MQTT_sess`.

Because the domain is part of the session subject, **changing `-js-domain` on an
existing deployment orphans every existing session**: the records remain in the
stream under the old token and the broker looks for them under the new one.
Treat a domain change as a session reset, not a configuration tweak.

The same setting also drives the `internal/backend` JetStream client
(`jetstream.NewWithDomain`), so both paths address the same domain.

`-js-domain` must be a **single subject token**: no `.`, `*` or `>`, no
whitespace, no control characters. This is deliberately narrower than the NATS
Server, which rejects `.`, `*` and `>` but would accept a domain containing
invisible whitespace or control characters -- such a value looks right in config
and logs while silently producing a JS API prefix and a session namespace that
are not what anyone typed. Ordinary non-ASCII is accepted, so a legitimate
Unicode domain the server would take is not refused here. Invalid values fail at
startup rather than surfacing later as unroutable requests.

## Backend seams (see docs/PORTING.md)

| Concern            | Mechanism                                             |
|--------------------|-------------------------------------------------------|
| Sessions           | JetStream stream (last-msg-per-subject), keyed by CID |
| Retained messages  | JetStream stream + subject tree metadata              |
| QoS1/2 persistence | JetStream durable consumers                           |
| QoS0 routing       | NATS core pub/sub                                     |
| Asset management   | `mqttJSA` request/reply over the service `*nats.Conn` |
| Per-client authz   | Auth passthrough: one `*nats.Conn` per MQTT client    |

## Auth passthrough

On CONNECT, the MQTT username/password are used to dial a per-client
`*nats.Conn`. NATS then enforces that client's publish/subscribe permissions for
every MQTT topic (mapped to a NATS subject). JetStream asset operations use a
separate service connection with its own credentials. In v1 all MQTT users must
belong to the same NATS account as the service connection, because the
session/retained/QoS streams are account-scoped.
