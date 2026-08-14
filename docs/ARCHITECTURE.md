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

The floor is **nats-server 2.14.5**. Older releases are unsupported outright: there
are no fallbacks. Every JetStream peer must meet the floor; a mixed-version cluster
is unsupported. Stream-shape prerequisites are validated at startup and fail closed,
but the bridge cannot infer every peer's binary version from one client connection,
so the cluster-wide version floor is an explicit deployment prerequisite.

This boundary applies only to bridge persistence and its JetStream backend. MQTT
3.1.1 and 5.0 wire-protocol compatibility remains a product requirement and is
covered by the conformance suites.

### JetStream behaviors this design depends on

Correctness rests on these specific semantics, each stated as the contract the
code assumes. This is the list a version bump has to re-verify — it is what makes
the "no fallbacks" stance reviewable rather than implicit.

- **Per-subject CAS** (`Nats-Expected-Last-Subject-Sequence`). A store carrying it
  is accepted only if the subject's last sequence matches. The check is made before
  the proposal and is not repeated at clustered apply (`canConsistencyCheck :=
  !isClustered`, `server/stream.go`). Since 2.14.4, however, JetStream passes the
  stream leadership term into Raft and Raft rejects a proposal whose captured term
  is no longer current (`nats-io/nats-server#8370`); leadership changes also tear
  down the prior stream-leader state. F-030 owns the deterministic differential
  proof of whether that upstream fence eliminates the previously documented
  same-base cross-term window at the broker boundary. Until that proof and its
  consequence-specific audits land, the existing lineage and conformance residuals
  remain conservative rather than being deleted from source inspection alone. A
  rejection is reported structurally
  (`JSStreamWrongLastSequenceErrF` 10071, or 10164 under clustered inflight
  contention). 10164 means *transient contention*, not "the subject moved", so
  every rejection is resolved by re-reading rather than classified directly.
- **`MaxMsgsPer: 1`** turns a subject into a single-slot register: a new record
  evicts the old one, so a stream sequence names a record, never a subject.
- **Every store receives a fresh sequence, assigned before the proposal and applied
  in log order.** The leader stamps the sequence into the replicated entry and every
  replica applies entries in that order (`server/stream.go`), so no two accepted
  writes to a stream ever share a sequence and store state is always a prefix of the
  log. That is what makes a sequence usable as an **advancing identity** where a body
  cannot be: identical bytes are exactly what a re-sent claim produces, so a body
  field can be replayed while a sequence cannot. The session-interval register
  depends on this (see "The session-interval register").
- **The message-get API is answered by the stream leader.** `loadLastMsgFor` and
  `loadMsg` are served by the leader alone (`server/jetstream_api.go`), so a read
  issued after a confirmed write observes that write; a lagging follower cannot
  answer with an older view. Several orderings here are load-bearing on that — a
  watermark read taken after the record subject is observed empty is at least as new
  as the deletion that emptied it.
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
there, never the config it asked for. The ten streams are entries in one ordered
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
| `$MQTT_sess` | per-subject record CAS, `loadLastMsgFor`, filtered scans, exact-sequence confirmed deletes (teardown release, expiry); no purge; **one ephemeral `AckNone` session-record watch consumer per broker** (see "The session-record watch"); and, on `$MQTT.accept.>`, **create-only CAS writes of permanent per-CONNECT acceptance facts** committed in ONE atomic batch with the record, plus bounded filtered walks of them. Every record carries `WID`/`PWID`, the write's own identity and its parent's | **exactly two subjects** — `$MQTT.sess.<domainTk>>` and `$MQTT.accept.<domainTk>>` — compared as a SET (order-independent, duplicates refused); **`AllowAtomicPublish` required** (the accept batch carries `Nats-Batch-*` headers, which a stream without it rejects, failing every CONNECT); `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; **`DenyDelete` fatal** (the teardown delete is a record's only exit; nobody resumes a marked record, so a denied delete locks the client ID out permanently); no `Sources`; no `SubjectDeleteMarkerTTL`; a **valid incarnation epoch** in `Metadata` (validated on the same snapshot; unrelated keys accepted); **`MaxConsumers` unlimited** (one watch consumer per live broker, so a finite cap eventually rejects a legitimate broker start — note a configured `0` normalizes to unlimited) | `DenyPurge` (nothing purges); `ConsumerLimits.*` (a stream `InactiveThreshold` below the watch's requested 5 minutes, or any positive `MaxAckPending` against `AckNone`, already fails the consumer create at initialization, naming the consumer — as on `$MQTT_rmsgs`); `AllowMsgTTL` (no `Nats-TTL` header sent) |
| `$MQTT_willq` | create-only CAS enqueue per WillID, state-advance CAS, filtered scans, exact-sequence deletes (confirmed terminal, no-wait scan cleanup); no purge, no consumers | exactly ONE subject (`$MQTT.willq.<domainTk>>`); `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1` (each per-Will subject is a single-record CAS slot); no global limits; no `Sources`; no `MaxMsgSize` cap; **`DenyDelete` fatal** with the accumulation rationale: per-WillID subjects mean deletion is the only removal, so denying it grows the outbox monotonically and every scan re-walks every Will ever fired (`done` precedes the delete, so nothing re-fires) | `DenyPurge`; `MaxConsumers`/`ConsumerLimits`; `AllowMsgTTL`; **`AllowAtomicPublish`** — audited and NOT required here: only the accepting session save emits batch headers, and it writes to `$MQTT_sess`. This row states its own prerequisites rather than deriving them from `$MQTT_sess`, which would now import that stream's two-subject set and its atomic-publish requirement, neither of which applies |
| `$MQTT_teardown` | per-client retirement watermark CAS and reads, one filtered walk by the orphan sweep — **nothing else, ever** | `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; no `Sources`; plus the **adopted `$MQTT_sess` incarnation epoch** in `Metadata`, required to match this account's (unrelated keys accepted): an `mseq` is a session-stream sequence, so watermarks are comparable only within the incarnation they were written under | **`DenyDelete`/`DenyPurge`** and `SubjectDeleteMarkerTTL` (no operation deletes or purges — a fence is permanent by design, so no delete marker can ever be created); `MaxConsumers`/`ConsumerLimits` (no consumers). Both denial waivers are load-bearing here, since reaping a fence would unfence a generation whose resources are already deleted |
| `$MQTT_origin` | per-client interval register: a create-only CAS (the establish), a plain confirmed publish that replaces the standing register (the advance), and exact-subject reads — **nothing else, ever**. No scan, no consumer, no delete, no purge | `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; **no global limits** (they would reap registers continuously and silently, disabling recognition for every client whose register ages out — not because eviction could replay an identity, which it cannot); no `MaxMsgSize` cap; no `Sources`; **its own incarnation epoch** and the **adopted `$MQTT_sess` epoch** in `Metadata` (unrelated keys accepted) | **`DenyDelete`/`DenyPurge`** (no operation ever deletes or purges a register, and an erasure cannot replay an identity — a delete does not rewind the sequence counter, so the next establish mints a strictly higher one; what it costs is recognition, the same loss pruning the watermark streams carries); `SubjectDeleteMarkerTTL` (no delete occurs here); `MaxConsumers`/`ConsumerLimits` (no consumers); `AllowMsgTTL` (no `Nats-TTL` sent); `Duplicates` (no `Nats-Msg-Id` set) |
| `$MQTT_msgs` | plain stores with per-message TTL; durable, drained and shared queue consumers, each with an explicit `MaxAckPending`; consumer deletes; `loadMsg` by sequence; **no message delete, no purge** | **`InterestPolicy`**; `MaxMsgsPer` unlimited; no global limits; no `MaxMsgSize` cap (a review round proposed a threshold at the broker's max payload, since the PUBLISH admission preflight bounds the complete stored form; disproven — QoS 1/2 Will stores bypass that preflight, so no positive cap is provably inert); **`AllowMsgTTL` required when MQTT 5.0 is accepted** (the default; a `max_protocol_version: 4` broker never emits `Nats-TTL`, so it waives the field); no `Sources`; no `SubjectDeleteMarkerTTL`; `MaxConsumers` unlimited (one durable per subscription/group/drain); `ConsumerLimits.MaxAckPending` fatal when non-zero and **below** the broker's effective cap | `DenyDelete`/`DenyPurge` (no delete or purge exists); `Discard` (inert with every limit unlimited); **`ConsumerLimits.InactiveThreshold`** (runtime refusal by the drain and shared/affine guards — see below); `Duplicates` (best-effort narrowing only) |
| `$MQTT_rmsgs` | per-topic store with TTL incl. empty-body tombstones (the durable deletion authority); batched `loadLastMsgForMulti` reads; **no explicit message-delete or purge API calls**; one ephemeral `AckNone` consumer per broker | `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; **`AllowMsgTTL` required when MQTT 5.0 is accepted** (as on `$MQTT_msgs`); no `SubjectDeleteMarkerTTL`; `MaxConsumers` unlimited (one ephemeral consumer per broker) | **`DenyDelete`** (the owner issues no delete API request at all; the tombstone plus `MaxMsgsPer: 1` store-time replacement carry correctness, and both that eviction and TTL expiry are store-internal reaping `DenyDelete` does not gate); `DenyPurge`; **`Sources`** (supported cross-account retained topology); `ConsumerLimits.*` (a hostile limit already fails the consumer create at initialization, naming the consumer) |
| `$MQTT_qos2in` | hold/claim CAS at `<idHash>.<gen>.<PI>` (+ `.rel` sibling), exact-sequence deletes, confirmed exact-subject hold purge (discard + orphan sweep), bounded walk | `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; `DenyDelete` fatal; **`DenyPurge` fatal** (blocks the confirmed exact-subject hold purge); no `Sources`; no `SubjectDeleteMarkerTTL` | `MaxConsumers`/`ConsumerLimits` (no consumers); `AllowMsgTTL`; `Duplicates` |
| `$MQTT_qos2assign` | create-only claim CAS, transition CAS, exact-sequence reap; no purge, no consumers | as `$MQTT_qos2in` but `DenyPurge` waived | `DenyPurge` (nothing purges by subject); `MaxConsumers`/`ConsumerLimits`; `AllowMsgTTL` |
| `$MQTT_inflight` | create-only register CAS, exact-sequence deletes, per-incarnation filter purge (teardown), bounded walk; no consumers | as `$MQTT_qos2in` (`DenyDelete` and `DenyPurge` both fatal) | `MaxConsumers`/`ConsumerLimits`; `AllowMsgTTL` |
| `$MQTT_out` | append-only stores (one a create-only CAS), per-session PUBREL + affine durables, interest-based drop; **no message delete, no purge** | **`InterestPolicy`**; `MaxMsgsPer` unlimited; no global limits; no `MaxMsgSize` cap; no `Sources`; no `SubjectDeleteMarkerTTL`; `MaxConsumers` unlimited (two durables per session); **`ConsumerLimits.InactiveThreshold` fatal** (it would reap a disconnected session's affine consumer); `ConsumerLimits.MaxAckPending` fatal when non-zero and below the effective cap | `DenyDelete`/`DenyPurge` (interest retention removes messages); `Discard` (inert); `AllowMsgTTL`; `Duplicates` (best-effort narrowing only) |

Three asymmetries in that table are deliberate, and each is pinned by an
acceptance test so an over-strict "fix" fails:

- **`DenyDelete` is fatal on `$MQTT_sess`/`$MQTT_willq` and waived on
  `$MQTT_teardown`/`$MQTT_rmsgs`** — the former have deletion as a record's
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

`$MQTT_sess`, `$MQTT_teardown` and `$MQTT_origin` are **one logical unit**, and all
three validators say so in their recreate guidance. The retirement watermarks order
themselves by `$MQTT_sess` sequences and the register names intervals of `$MQTT_sess`
record subjects, so a partial recreation leaves state that describes an incarnation
that no longer exists.

Recreating `$MQTT_sess` now costs one thing more, and it is not recoverable: the
permanent CONNECT **acceptance facts live on that stream**, so recreating it discards
every client's acceptance history. A delayed Will enqueued before the recreation can
then no longer be suppressed by the acceptance that was entitled to suppress it, and
it fires. That is a deliberate consequence of putting the facts where the record is —
the atomicity that removes the split-acceptance interval is only available within one
stream — and it is stated here rather than mitigated, because there is no honest
mitigation short of a second write nobody could make atomic with the first.

Startup catches only part of that, and the exact rule matters to an operator judging
whether a mistake would be caught. Both `$MQTT_teardown` and `$MQTT_origin` carry the
session epoch in STREAM METADATA. So **a recreated `$MQTT_sess` is refused if and only
if an OLD `$MQTT_teardown` or `$MQTT_origin` survives beside it** — recreate both of
those together with it and startup admits — and **with `$MQTT_sess` kept, either of the
other two may be recreated and is admitted**, starting cleanly with no historical
fences and no registers. `docs/CONFORMANCE.md` records those losses; the three-stream
recovery is specified as a procedure for that reason rather than because every mistake
fails closed.

That second direction is safe for different reasons on the two kinds of stream, and
the register's reason is **conditional on the mandatory restart** below. A recreated
`$MQTT_origin` mints a fresh incarnation epoch, so every identity it later hands out
diverges from every old one — but only for a broker that has adopted that epoch.
`as.originStreamEpoch` is captured once per account manager, so a **live** manager
keeps pairing the old epoch with sequences that have restarted at 1 and can reproduce
an identity an earlier interval held. See `docs/CONFORMANCE.md`.

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
- **Takeover suppression (permanent per-CONNECT acceptance facts).** An accepted
  CONNECT records one durable, dated fact of its own acceptance on a subject nothing
  else ever writes — `$MQTT.accept.<domainTk><cidHash>.<epoch>.<acceptID>` — and that
  fact is committed in ONE JetStream **atomic batch** together with the session
  record, on the shared `$MQTT_sess` stream. That is the whole design, and each half
  answers a defect the previous one could not.

  *Why atomic.* A suppression written after the record leaves a window in which a
  crash keeps the acceptance and loses the evidence; written before it, a window in
  which the evidence outlives a takeover that never happened at all. Neither ordering
  is available to a two-write protocol, and no gate closes a window the protocol itself
  creates. The batch removes the interval **between the two writes**: the fact and the
  record commit together or neither exists, so a fact exists only where a session
  record committed — only where a takeover really did occur.

  What atomicity does **not** do is make the fact conditional on the CONNECT
  *succeeding*. The batch commits before consumer reconciliation, the lifetime grant and
  the CONNACK, and several exits between them still refuse the connection; the fact is
  durable by then and will suppress the prior Will even though the client never learned
  it was accepted. That is a disclosed residual in `docs/CONFORMANCE.md` — the family the
  pre-CONNACK disclosure already covered — and not a window this ordering closes.

  A batch is per stream, which is why the
  facts live on `$MQTT_sess` under a **separate top-level prefix** rather than nested
  under `sess.` — every session walk in the broker filters `sess.<domainTk>>` and
  decodes what it gets as a session record.

  *Why unique subjects.* The fact carries its own `acceptID`, so nothing ever
  overwrites it and the set of acceptances is monotone. A single-slot watermark is
  regressible: a sibling write erases the acceptance an earlier CONNECT recorded, and
  an erased acceptance can never be recovered because nothing else knew about it.

  *Why dated.* MQTT 5.0 suppresses a delayed Will only for a reconnect that happens
  DURING the Will Delay ([MQTT-3.1.3-9]); once the delay has elapsed the Will is owed
  and must be published ([MQTT-3.1.2-8]). A watermark carrying no date could answer
  only the first, which is why both the watermark and the direct reconnect-time
  outbox cancel are gone: neither could say WHEN the acceptance happened.

  *Ordering inside the batch.* The fact is staged FIRST and the session record
  carries the commit. The batch's PubAck therefore reports the record's sequence (the
  reply rides the commit entry, which is the sequence the session adopts), and the
  fact sorts strictly BELOW the record of the connection that wrote it — so a
  connection can never suppress its own Will. A fact's sequence is read back from the
  stream, never derived arithmetically from the ack.

  *The predicate.* A claimant suppresses a Will iff some acceptance fact of that
  client satisfies all of: the fact belongs to the incarnation this broker is running
  in; its client id matches the Will's EXACTLY (equal hashes are not equal clients,
  and the hash only routes); `At < FireAt`; and, within the Will's own incarnation,
  the fact's stream sequence is strictly above `OriginSessionSeq`. Across incarnations
  that last comparison is meaningless and a current-incarnation acceptance supersedes
  outright. Every binding is re-checked at decode — the body's client id must hash to
  the subject's token, its epoch must equal both the subject's and this broker's, its
  `acceptID` must equal the subject's, and `At` must be present — because an
  unchecked binding is a way to make one client's acceptance destroy another's Will. A
  fact that fails any of them is REFUSED and DEFERS the Will; a well-formed fact
  naming a different client that merely hashes the same is simply non-qualifying.

- **The claimant decides once, and its decision is durable.** The order is CLAIM
  FIRST, then decide. Deciding first and claiming afterwards leaves the decision
  unowned: two claimants can reach opposite verdicts from cutoffs that straddle one
  acceptance, and the loser's verdict is not recorded anywhere to be contradicted.

  1. Claim or reclaim (`pending -> publishing`, or `publishing -> publishing` at the
     scanned sequence), carrying `Decision` and `DecisionCutoff` forward unchanged.
  2. If the claimed record is already LICENSED TO PUBLISH, execute that decision — no
     cutoff is sampled, no fact is read, and whatever acceptances have landed since
     are irrelevant.
  3. Otherwise sample a cutoff `U` (the `$MQTT_sess` last sequence), walk this Will's
     acceptance facts within it while renewing the claim, CHASE the cutoff as below,
     and license the outcome with one CAS: publish (staying `publishing`, lease
     refreshed) or cancel (straight to `cancelled` — there is no licensed-cancel
     `publishing` state, which would be a state with two meanings).

  **The cutoff is CHASED, not sampled once.** A walk examines everything at or below
  the cutoff it was given, so an acceptance that commits WHILE it runs sits above that
  cutoff — and because the decision is persisted, no later claimant ever returns to
  read it. So after a walk finds nothing, the cutoff is re-sampled and only the delta
  is examined, up to `mqttWillCutoffChases` times, and the cutoff PERSISTED is the one
  actually verified through rather than the first sample. This is sound in exactly one
  direction: `At < FireAt` is the term that DATES eligibility, so a later cutoff can
  never suppress a Will an earlier one would have published — it can only find evidence
  the pass would otherwise have missed.

  What bounds the chase is that **an empty delta is not required to decide.** A client
  reconnecting steadily after its Will fell due — or one that merely hashes the same —
  produces facts forever, none of which can qualify, so a chase that insisted on an
  empty delta would defer an OWED Will on every pass and never fire it: the wedge shape
  again, arrived at from the other side. Exhausting the limit therefore LICENSES under
  the cutoff verified so far. Deferral stays reserved for the four conditions that make
  a decision unsafe rather than merely less informed — shutdown, an exhausted budget, an
  unreadable fact, and a lost renewal — and an unavailable re-sample stops the chase
  without abandoning a pass that has already read its evidence.

  The chase NARROWS and does not close: an acceptance still uncommitted at the final
  sample is unreadable, and `docs/CONFORMANCE.md` carries that residual with its
  interleaving. Do not read the chase as a fence.

  `Decision`/`DecisionCutoff` are persisted BY the licensing CAS and are immutable
  once set, because a fresh decision per claimant is destructive without needing any
  leader transition at all: a claimant that scanned under one cutoff, licensed a
  publish, published, and crashed before the terminal commit would otherwise be
  recovered by a peer that samples a LATER cutoff, sees an acceptance that landed in
  between, and cancels — the same Will reinterpreted from published to suppressed
  after the irreversible act. `DecisionCutoff` is a POINTER: `LastSeq == 0` is a valid
  cutoff on a fresh or recreated stream, so a numeric-zero sentinel would collapse "no
  cutoff" into it. The legal `(state, decision, cutoff)` matrix is enforced at every
  decode boundary and everything outside it is refused.

  **A bounded scan has to be able to FINISH an unbounded history**, and that needs
  persisted progress: the record carries `ScanFrom`, the highest `$MQTT_sess` sequence
  a claimant has examined and REJECTED for this Will, and a later claimant resumes
  above it. It is sound because a rejection is permanent — the record's predicate
  inputs (client, epochs, origin sequence, `FireAt`) are immutable and a fact is never
  rewritten or removed. Without it every claimant restarted at the same sequence, so a
  non-qualifying prefix long enough to exhaust the budget wedged the Will permanently:
  neither published nor cancelled, with every lease-expiry recovery re-reading the same
  prefix. `ScanFrom` is advanced by the RENEWAL CAS, which already writes during a
  scan, so progress costs no extra round trip; every reclaim carries it forward, and a
  `pending` record may never hold one (only a claimant renewal sets it, and a claimant
  is `publishing` by then).

  **It is a `$MQTT_sess` SEQUENCE, so it is meaningless outside the incarnation that
  issued it**, and it therefore travels with `ScanEpoch`. The pair is validated together:
  a bare sequence is uncomparable and a bare epoch says nothing. `$MQTT_willq` is not
  part of the coupled recreation unit, so a pending Will SURVIVES that operation carrying
  a floor recorded against a stream that no longer exists, while the new one restarts at
  sequence 1 — which makes this reachable through an operation the documentation
  supports, not a theoretical concern.

  Both sides are scoped, and each was a live defect when it was not:

  - **Reading.** Honouring a foreign floor skips exactly the facts a cross-incarnation
    Will qualifies on, and with the floor above the new cutoff the walk runs ZERO times
    and reports "nothing qualifies" — licensing a publication having read no evidence at
    all.
  - **Writing.** Progress is compared against the USABLE floor, never the stored number.
    Comparing against a numerically larger stale value persists nothing, so every pass
    re-reads the same prefix and re-exhausts the same budget: the wedge this field exists
    to prevent, reached by scoping only one of the two sides. The stored number therefore
    moves DOWNWARDS when a real current-epoch sequence replaces a large foreign one,
    which is correct and is why monotonicity is a property of the usable floor rather than
    of the field.

  An ignored floor costs one re-walk, which is the conservative direction.

  Timing is derived from the claim lease (30s) rather than configured beside it:
  the renewal cadence is `lease/3` and the scan budget `2 x lease`. The one input
  that is NOT derived is the JetStream request timeout, which is an operator option
  while the lease is a constant — so an account whose effective timeout is not below
  the default lease is **REFUSED at account-manager construction**, the same
  treatment a bad stream shape gets: at or above the lease, a licensing acknowledgement
  can arrive after a peer has already reclaimed the expired record. The check compares
  against the DEFAULT lease deliberately, because tests shorten the lease per-manager
  and a two-second lease against a five-second request timeout is their intent, not a
  misconfiguration.

  **That check bounds ONE REQUEST, not the worker's licensed lifetime**, and must not be
  read as the latter. `mqttInitiateMsgDeliveryJSA` fans a Will out to subscribers first
  — irreversible and external — and only then waits on `mqttSubsBarrier` before storing
  the QoS 1/2 copy, a wait bounded by SHUTDOWN rather than by the lease. The barrier is
  held for a whole `processSubs`, which creates a JetStream consumer per filter, so a
  CONNECT session restore carrying a few dozen subscriptions on a cluster timing
  requests out holds it past 30s. A peer then reclaims the still-`publishing` record,
  reuses the persisted decision and publishes again, with both brokers alive and nothing
  crashed. Bounding the barrier would be worse: the fan-out has already happened, so
  abandoning there would skip the store that protects the racing subscriber and trade a
  duplicate for a LOSS. `docs/CONFORMANCE.md` states the resulting failure boundary, and
  end-to-end phase-aware ownership is a `docs/ROADMAP.md` item. The budget starts
  BEFORE the cutoff sample, not after it — that sample is itself a JetStream request,
  and a budget beginning afterwards would leave it unbounded. Both the budget and the
  shutdown signals are checked BEFORE and AFTER every blocking read, because a request
  runs to its own timeout and cannot be cancelled: a qualifying answer arriving after
  shutdown began would otherwise license a durable cancel on a broker that is leaving.

  Three places check them, and each covers a window no other does — the scan's own
  pre/post-read bracket, the chase before each re-sample, and the final guard before the
  licensing CAS. The final guard is not redundant: a verdict reached WITHOUT entering the
  scan's loop (a cutoff at or below its start) is a path the bracket cannot cover, and a
  qualifying verdict skips the chase entirely. A FOURTH check, between the cutoff read and
  the scan, was written and then deleted — it caught the same condition one step later
  than the scan's own pre-read check, so no test could distinguish the two and a mutation
  of either survived. The rule that separates the three kept from the one deleted is that
  a check earns its place by having a window a test can distinguish, which is why the
  chase's is pinned on a SAMPLE COUNT rather than on an outcome.

  An UNCONFIRMED renewal abandons — it may be transient, an
  ack-lost success, or proof that a
  recovery claimant advanced the record, and only the last is a verdict, so all three
  are treated alike. The budget is checked before every fact load AND again
  immediately before the licensing CAS: a renewed lease is not permission to act once
  the budget is gone. `finalizeWillPublish` runs only for a licensed publish and
  requires the current record to carry the SAME decision and cutoff; `ClaimOwner`
  equality is deliberately not required, because after a lease expires a peer may
  reclaim and republish that same licensed decision.

Residuals that keep literal exactly-once impossible:

- *At-least-once duplicate* — the sanctioned one, whose boundary is **external
  publication with no durable evidence of completion**. A crash is one route, not the
  definition. Two triggers: lease expiry with `done` uncommitted (where a crash, a pause,
  a partition and a live broker stalled in the shutdown-bounded `mqttSubsBarrier` wait are
  indistinguishable to recovery — one stall yields two publications, not unboundedly many,
  since the barrier is per account manager and the reclaiming peer is not stalled); and
  enqueue ambiguity, where retry exhaustion publishes immediately through the fail-safe
  while an unconfirmed record may exist and fire later. `docs/CONFORMANCE.md` carries
  both.
- *Cross-term claimant divergence* — a distinct destructive class, NOT the sanctioned
  duplicate, and *Terminal regression*, which is the part of F-030 that stays open.
  Both are stated with their exact interleavings in `docs/CONFORMANCE.md`.

The reconnect-suppression miss is closed WITHIN a `$MQTT_willq` leader term by the
permanent facts and the persisted decision. This is a first-release storage format:
pre-Layer-2 in-record Wills, outbox records without an exact client identity or a
legal decision shape, and a `$MQTT_sess` stream without a valid generated epoch are
unsupported and rejected with recreate guidance rather than carried forward.

**Refusal of an unsupported outbox record.** That rejection is a mechanism, not a
policy statement. Every decoded `$MQTT_willq` record is validated
(`validateWillOutboxRecord`): a well-formed incarnation generation, a nonzero
`OriginSessionSeq`, a well-formed `OriginSessEpoch`, a known state, `WillID` and
`ClientIDHash` that are each a nonempty **single subject token**, and the binding
that those two identities name the subject the record is stored on, plus the exact
`ClientID` the acceptance predicate matches on. **Both** decode boundaries validate —
the point loader and the claim scan — so nothing can act on a record that does not
satisfy them. There is no third boundary: the reconnect-time cancellation walk that
was the third is gone, replaced by the acceptance-fact scan the claimant runs. The
enqueue validates before persisting and falls to the immediate-publish fail-safe
rather than write one; consequently the claimant is handed only validated records and
does not re-check their origin.

Two of those checks answer failure modes the obvious ones do not. Body validation
alone would admit a well-formed record carrying another client's identities stored on
this client's subject, and the suppression predicate would then judge it against the
WRONG client: `mqttAcceptFactQualifies` matches a fact's exact `ClientID` against the
record's, so a mismatched body would be tested against acceptances that were never
this Will's to be suppressed by — while `finalizeWillPublish`'s `WillID` equality
would not catch it (that detects a post-claim replacement, not a body that never
matched its subject). And subject equality alone would admit a dotted identity: NATS
parses these into subject tokens, so a `WillID` of `a.b` would make the subject
`<domainTk><cidHash>.a.b`, breaking the one-to-one mapping between a subject and the
`(cidHash, WillID)` pair it is supposed to name — which is what makes the binding
check meaningful at all, and what keeps `MaxMsgsPer: 1` a per-Will CAS slot rather
than a per-prefix one.

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
- *The reconnect path no longer pays this cost.* It used to be the worse of the two,
  because a client waited on it: `mqttProcessConnect` called a synchronous per-client
  cancel walk before the CONNACK, so `N` refused records for one client cost `O(N)`
  serial round trips on every reconnect of that client, forever. That walk is gone
  with the reconnect-time cancel itself, so a refused backlog now costs only the claim
  scan above and no longer degrades a client's connect time.

This is the same accumulation hazard the `DenyDelete` rationale states for the
stream as a whole, arriving by the other route: there, deletion is denied; here, the
bridge declines to delete what it cannot read. The refusal is therefore not a stable
resting state to be left alone — the diagnostic asks for prompt cleanup, and an
operator who leaves refused records in place is accepting a growing scan cost and a
growing connect cost for the affected client. Deliberately no quarantine stream, no
bounded scan and no reaper: each would be a second mechanism deciding the fate of
records the bridge has already said it cannot interpret.

Operational constraints for the coupled streams:

- **Back up, restore, delete, or recreate `$MQTT_sess`, `$MQTT_teardown` and
  `$MQTT_origin` as one logical unit.** Recreating `$MQTT_sess` discards every
  client's permanent acceptance history along with the records, so a delayed Will
  enqueued beforehand can no longer be suppressed and fires; recreating only
  `$MQTT_teardown` loses every open
  generation-retirement fence; recreating only `$MQTT_origin` loses every session
  interval identity, so the next record-absent create for each affected client concedes
  a takeover it cannot demonstrate. Recreating `$MQTT_sess` alone is not merely
  discouraged — `validateTeardownStream` and `validateOriginStream` refuse to start
  against state stamped with a different incarnation epoch (see "Stream
  prerequisites").
- **Recreating `$MQTT_sess` or `$MQTT_origin` requires restarting every bridge serving
  the account before MQTT traffic resumes.** Both incarnation epochs are captured once
  per account manager (`as.sessStreamEpoch`, `as.originStreamEpoch`); a live manager
  that outlives stream recreation retains the stale epoch — stale session-CAS state in
  the first case, and in the second an identity derivation that pairs the old epoch
  with sequences that have restarted at 1, which can reproduce an identity an earlier
  interval used. A process restart recreates the manager and adopts the new epochs.
- **Retention on `$MQTT_origin` is intentionally unbounded**, exactly as on the two
  watermark streams: one permanent register per distinct client hash ever seen, no
  `MaxAge`, no global limits. Monitor its message count and storage. Administrative
  pruning is permitted and is not a correctness hazard — a delete cannot rewind the
  stream's sequence counter, so the next create establishes a strictly higher
  identity — but it weakens recognition for the pruned client until its next
  teardown, which is recorded in `docs/CONFORMANCE.md`.
- **Acceptance-fact retention is intentionally unbounded and shares `$MQTT_sess`**:
  one permanent record per ACCEPTED CONNECT — not per client — so the stream grows
  with connection churn rather than with the client population, which is a different
  and faster growth curve from the per-client watermarks it replaces. Nothing reaps a
  fact: retiring one needs proof that no delayed Will it could still suppress remains,
  which nothing here has. Monitor `$MQTT_sess` message count and storage.
  Administrative pruning of facts silently removes suppression evidence and can let a
  superseded Will fire; `docs/CONFORMANCE.md` records it.
- **The full `$MQTT_sess` configuration is validated at startup** (see "Stream
  prerequisites"), including the exact two-subject set and `AllowAtomicPublish`. A
  stream that merely looked right — one subject space, or batches disabled — would
  fail every CONNECT at the accept batch rather than degrade quietly.

### Two frontends taking over one session from the same base

The session record is the sole arbitration, and per-subject CAS linearizes it only
WITHIN a stream-leader term. Nothing can prevent two writes off one base. What the
design delivers is that every interleaving converges on one owner and each client is
told something true.

**Every record write is conditional, base 0 included.** The create was the one write
that opted out: with no header it landed on top of whatever was there, so a clean-start
CONNECT on one frontend erased the record another was acknowledged for, with no
rejection anywhere. At sequence 0 the header means *the subject must currently be
empty*. Two consequences follow. A create can now fail — 10071 for an occupied subject
and, deliberately, for a subject load the server folds into the same code, so **10071
never proves a record exists**; 10164 for contention, including with an in-process write
of any kind. And the pre-proposal check enrolls the write in the clustered in-process
contention checks, which is what makes a concurrent create fail rather than silently
interleave.

That last clause is a **dependency contract, and the evidence for it lives upstream**:
nats-server rejects a write whose subject is already in process
(`jetstream_batching.go:777-782`) and tests that guard directly
(`jetstream_batching_test.go:1576-1586`) and on a replicated stream
(`jetstream_cluster_4_test.go:5158-5164`). This broker's own replicated tests assert the
property that follows — from an empty subject exactly one create is admitted, the loser
is definitely rejected, and the winner's bytes survive — but cannot force two proposals
into the pre-proposal window, because no seam holds a proposal between admission and
apply. That is a limit on what a test here can observe, not on what the broker does.

**A superseded teardown claim re-drives; it never proceeds at base 0.** `clear()`
reported both of its superseded outcomes as success, and a CONNECT read that as a
completed discard and went on to its accept save — at base 0, because `clear()` had just
reset the sequence. The two halves close the same stomp from opposite ends: the write is
refused, and the path that produced it no longer runs. The outcome is now typed
(`errMQTTClearSuperseded`); four callers are terminal on it and two re-drive, and each
terminal one has its OWN branch, because a normal outcome logged as a teardown failure
sends an operator hunting something that never went wrong.

**The loop is the contract, not an enumeration of branches.** Three review rounds each
found one more reachable path through the CONNECT's control flow, so the design does not
depend on having enumerated them:

- **R1** — any outcome meaning *the base I was going to write at is not current* (a
  superseded claim, a superseded release including one inside `finishAdoptedTeardown`,
  or a definite 10071-class rejection) re-enters at the authoritative restore, from the
  top. No path continues to a write on a base it learned was stale.
- **R2** — every write's base and parent come from the same authoritative read. The base
  is what the re-evaluated clean-start decision produces; the parent is that record's
  identity, with one named exception — an adopted teardown marker's successor parents
  from the marker's `PWID`.
- **R3** — the loop is bounded, and every exit leaves nothing resident at `seq == 0`.

One re-entry is driven by *not being able to know*, and it is the one door that reports
nothing. A CONNECT whose session has an unresolved record write resolves it first, and a
resolution that comes back ours discharges whatever the close parked. When that is a
deferred discard, the discharge runs `clear()` and consumes the outcome — the session
leaves the maps whether the teardown completed or was superseded — so the CONNECT cannot
tell the two apart by asking. It re-reads instead of reusing the object left behind,
which would write at the sequence `clear()` reset to 0 after the base was already stale.
That re-entry is admitted as a **cycle**: the pass it starts performs a fresh
authoritative restore and may run a whole teardown and accept save off it, which is
exactly the work the admission deadline exists to stop. The cycle cap can never be what
refuses there — the arm is reachable at most once per CONNECT, since the restore yields
an object that is not uncertain — so what the admission enforces is the deadline.

R3's second half is not housekeeping. `clear()` resets the object to sequence 0 and
deliberately does not detach its client, and `reconcileResidentOwnership` returns early
on `ourSeq == 0`, because an absent subject is the ordinary state of a session whose
first record has not landed. A reset, still-bound, still-resident object is therefore
unreachable by every reaper the broker has — permanent split-brain, not a leak.

**Which is why the re-drive is a binding state machine, not one disposal primitive.**
The discard runs *pre-bind*, when the bound client is the genuinely old one; the accept
save runs *post-bind*, when it is the connecting client itself. Four transitions:

| | State | Disposal |
|---|---|---|
| **T0** | pre-bind, not resident (the restore's own overdue discard) | nothing to dispose; re-read and restart the decision |
| **T1a** | resident, unbound | evict, tell nobody — there is no connection to notify |
| **T1b** | resident, old client attached | evict, then the ordinary takeover tail: Will drop, flappers entry, 0x8E, close |
| **T2** | post-bind, the connecting client is the bound one | **silent** detach and evict, then re-bind the SAME client to the replacement |

Each disposes BEFORE anything fallible, which is what makes the reset-resident state
unreachable rather than merely unlikely. T2 also hands over the **accept record
transaction**: it ends the transaction on the object it is leaving before switching, so
the deferred close cannot end an old token against a new object and leak the discarded
one's fence. A plain end, never a new generation — epoch advancement is `clear()`'s
mechanism for an object that will be REUSED, and T2 abandons its object instead.

**Two independent bounds, because there are two loops.** Full re-drive cycles are capped
at 3; 10164 create retries repeat one write rather than one takeover, so they draw on
their own cap of 5 with a 100 ms backoff. The two are deliberately unequal: with the
same number, a shared counter is indistinguishable from two separate ones. Over both
sits a 5 s **admission** bound — no new top-level cycle and no new accept-save retry is
admitted after it. It is not a ceiling: an attempt already in progress runs to the
JetStream request timeout, and the nested retry loops inside `claimTeardown` and
`releaseTeardown` are not deadline-checked. On exhaustion the CONNECT is refused, with
nothing resident and nothing bound — the state a fresh CONNECT starts from.

**The CONNECT rollback ARBITRATES before it fences, and its own documented rationale is
REVERSED** (F-029b). The rollback that undoes an accepted CONNECT whose Will-cancel
watermark could not be advanced used to say, in as many words, that it deliberately takes
no teardown claim: it removed only the record, at an exact sequence, and deleted no
consumers, so it had nothing it could wrongly delete. That was true while a bare delete
had no consequence beyond the record. It has one now — the next create into the emptied
subject inherits the deleted record's identity as its parent — so a delete that recorded
no retirement would leave two records in DIFFERENT intervals carrying the same parent,
which is a FALSE fork, the one direction that suppresses a delayed Will's cancellation.
Four consequences, each load-bearing:

- **The predicate is the session's EFFECTIVE persisted lifetime**, never the interval this
  CONNECT requested. The requested value is not applied at the call site — the grant
  happens after the watermark advance whose failure calls the rollback — so branching on
  it would let a rejected RESUME that merely asked for 0 destroy a persistent session's
  consumers, QoS 2 holds, affine state and in-flight registers. That was a live defect in
  the bare-delete form, and fixing it is part of the same change.
- **A zero lifetime tears the candidate DOWN rather than deleting it**: `clear()`'s
  seventh call site. The CLAIM is what licenses the fence, because it is a CAS at the
  candidate's own sequence — a same-generation successor rejects it, nothing is deleted
  and nothing is fenced, while a won claim means the record it retires really is
  current. Recording a retirement without holding the base would instead fence a
  legitimate successor permanently and route it through a destructive discard. What the
  claim does NOT bound is the cross-term apply that regresses the single slot: this
  caller inherits that residual from every other `clear()` caller, and
  `docs/CONFORMANCE.md` states it as new exposure on this path, because the bare delete
  it replaces could not reach it. The teardown also deletes this incarnation's consumers
  and durables, which the bare delete left to the orphan sweep, and which is what a zero
  lifetime means on the ordinary close path too.
- **The classification reads the pair captured at ENTRY**, because `clear()` zeroes
  `wid`/`pwid`; read afterwards, a genuine sibling of the candidate would be reported as
  a legitimate newer generation. A newer record MARKED for teardown classifies applied —
  it is never resumed, so the candidate is not falsely connected — and is gated
  explicitly so a marker never reaches the sibling test.
- **A refused teardown parks rather than abandons.** `clear()` refuses while a record
  write is unresolved, having deleted nothing, and the rejected session object is the only
  thing that can resolve that write — so the discard is parked and the session stays
  resident, reported as its own outcome. The function's unconditional "the in-memory
  session is removed" therefore becomes "removed, or retained as a parked discard".

**What this does not close.** The shape this design left open — telling two parentless
first-creates racing across a leader term apart from two unrelated ones — is closed by
inheriting the retired record's identity (see "Recognising a forked session record"), for
every interval a valid retirement watermark describes; what still concedes is stated in
`docs/CONFORMANCE.md`. The other — the window in which a teardown that WON its claim has
already deleted the consumers a cross-term survivor's record still names — is closed by
the generation-retirement watermark, which is durable state off the record subject and
therefore outside this design's reach (see "The generation-retirement watermark").

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
  exactly ONE deleter of a session record — `releaseTeardown`, at a marker sequence a
  claim CAS won — so a record can only vanish under a party that first claimed it.
  The CONNECT rollback used to be a second, unclaimed one; it now tears down through
  `clear()` instead, which is the stronger statement this arm always wanted. The bound
  outcome is therefore **0x8E**, not 0x89; `docs/CONFORMANCE.md` records the one
  imprecision that leaves.

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
absent any write ownership provably never moved. What remains is the serving window
between an observation and the worker's decision. Nothing recreates a deleted or
inactivity-reaped watch consumer under
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
issues those (only `clear()` now, the CONNECT rollback included). Disposing on absence therefore
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
foreign **fail-safe** — never adopted — rather than proven.

**Base 0 is no longer excluded, because the create is no longer unconditional.** The
exclusion was sound while a first-ever write carried no CAS header: an unconditional
re-send arbitrates nothing and could only overwrite whatever was there, so an absent
subject at base 0 had to be read as somebody else's. Every record write now carries the
header at every sequence, 0 included, where it means **the subject must currently be
empty** — so base 0 flows through the ordinary arms. *Gone* re-attempts, and acceptance
against an empty subject proves the create ours; *at base* stays unreachable at 0
because stream sequences start at 1, which is a property of the store rather than an
assumption; *moved* is ours on a digest match and otherwise foreign, or forked when the
lineage test below says so.

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

1. `ackWID != "" && ackPWID != "" && found.WID != "" && found.PWID == ackPWID &&
   found.WID != ackWID` → **forked**;
2. anything else → not-forked, which is the pre-lineage behavior exactly.

**The incarnation generation is deliberately not consulted, and that is a reversal.** It
used to short-circuit ahead of the pointer test, on the reasoning that a different
incarnation restarts the lineage. That hid the fork this rule exists to catch. Record
identities are globally unique, so a parent match states descent from one actual record
whatever generation wrote it; a generation mismatch adds nothing to that and only
suppressed it.

**The chain also crosses the teardown re-mint**, which is what makes a clean-start fork
visible at all. `clear()` zeroes `wid`/`pwid` — the session is acknowledged for nothing
once its record is deleted, and six sites read `wid` as exactly that — but it records the
WID of the record the teardown CLAIMED in a third field, `retiredBaseWID`, and both
parenting sites take their parent from `parentForNextWriteLocked()`: the record we hold,
or that one when we hold none. So the sibling of base `a` written after tearing `a` down
is `{c, a}` rather than `{c, ""}`, and a resume child `{b, a}` recognises it. The field is
memory-only and self-retiring: once a save lands, `wid` is authoritative again.

**A create descends from the durable identity of the interval it is creating into**, and
that is one rule with several sources rather than one rule per source (F-029b, then R0).
Where a retirement watermark survives, that identity IS the record whose deletion emptied
the subject, so the create becomes a *child* of the record it replaces; where none
survives, it is the `$MQTT_origin` interval register's own identity (see "The
session-interval register"), which is not a record WID at all. Either way, two creates in
one interval are siblings of each other and a create in a later interval is a descendant
rather than a sibling, so ordering survives deletions with no new comparison anywhere —
which is why neither source is a fresh nonce minted at the create.

The watermark-backed sources all name the same RECORD:

| The teardown that emptied the subject | Where the create's parent comes from |
|---|---|
| performed by this object itself | `clear()`'s `retiredBaseWID`, the record its claim held |
| ADOPTED by this broker, marker still readable | the marker's own `PWID` — the record that teardown claimed, not the marker, which the release deletes and which is nobody's base |
| already complete, no marker survives | the generation-retirement watermark's `RWID` (see "The generation-retirement watermark") |
| already complete and no watermark survives either | there is no record to name, so the parent is the interval register's identity instead (see "The session-interval register") |

Without the third source the one shape the recognition could not see was the **cross-term
create pair**: both brokers find the subject empty, both are admitted at base 0
pre-proposal, both apply, `MaxMsgsPer: 1` discards the earlier, and both records are
parentless — so arm 2's `ackPWID != ""` conjunct concedes, and the erased side answers
0x8E about a takeover nothing arbitrated. Inheriting the retired record's identity makes
the pair siblings from either side, including the MIXED pair where one broker tore the
record down locally and the other only ever saw an empty subject. Inheriting the
watermark's `Nonce` instead would miss exactly that pair: the two sides would carry
different tokens and both would concede.

A create-parented fork is **recognised and repaired by nothing**: a create has a parent
POINTER and no parent CONTENT, so `buildForkedGrantLocked` returns nil and no capsule is
minted. Merging against an empty base is the union "Repairing a recognised fork" refutes.
`docs/CONFORMANCE.md` states the residuals, which are one missed recognition (a
record-absent create with no usable watermark still concedes) and four false-fork routes.

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

**What is reported travels as one value** (`mqttDisposalVocabulary`): reason code, Reason
String, log line and close reason. Passing a reason code alone emits 0x89 alongside
"session taken over by a new connection", which is precisely the claim the verdict exists
to withhold. **0x89 "server busy, try again later"** is the point of the whole mechanism —
it invites the reconnect that resolves the ambiguity, where 0x8E forbids it.

**This detects. What it recognises is then REPAIRED, for the recoverable subset, by
"Repairing a recognised fork" below.** The verdict itself still licenses no write of its
own, and it does not prove the survivor erased us — a teardown claim is itself a full-body
CAS off the same base — only that the survivor is no successor of ours. An absent `WID`
concedes through arm 2's guard rather than through a legacy branch, so there is no alternate
code path and no mixed-version machinery. `docs/CONFORMANCE.md` states what is left open.

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

### Repairing a recognised fork

Recognition stops a false takeover claim; it does not give the client back the
subscriptions it was SUBACKed for, nor the UNSUBSCRIBEs it was UNSUBACKed for. MQTT
requires the Session State to carry the client's subscriptions ([MQTT-4.1.0-1], Spec5
[4.1.1]), so that loss is a conformance break. `sess_fork_repair.go` closes the
recoverable subset of it, and everything below is memory-only: no persisted field, no
encoding change, no new stream or subject.

**What is recoverable is the record DELTA.** The anchor condition (`found.PWID ==
ackPWID`) means the survivor and our erased record are siblings of one NAMED parent, so
that parent is a single-hop common ancestor and a three-way merge against it is well
defined.

**A union is wrong, and this is the trap the design exists to avoid.** The verdict proves
siblinghood only — explicitly *not* that the survivor erased us. With base `P = {a}`, our
child `X = {a,b}` and a survivor `Y = {}` carrying an **acknowledged UNSUBSCRIBE of `a`**,
a union yields `{a,b}` and resurrects a subscription the client was UNSUBACKed for.
"Absent from the survivor" is ambiguous between "our addition was erased" and "the
survivor removed a base field"; only the parent's content disambiguates it.

**Two retained record BODIES** (`grantBody`, `grantBodyParent` on `mqttSession`): the
exact bytes of the record `wid` names and of the record `pwid` names. The grant itself —
the per-filter map plus the two session-scoped members, the cancelled-register nonces and
the PUBREL consumer — is materialised from those bytes by one constructor, and only on the
rare fork path.

Bytes rather than a built grant, because building it eagerly put O(grant size) work on the
save path: an earlier revision constructed the full grant inside `saveWithDetail` while
holding `sess.mu`, on every save ATTEMPT including rejected ones and retries. With
`MaxSubs` unset the grant is unbounded, and `BenchmarkGrantMaterialise` measures 2.7 us at
one filter, 189 us at 100 and **2.5 ms at 1000** — milliseconds under the session lock per
attempt. Retaining the body instead costs two slice assignments and no allocation
(`BenchmarkGrantBodyRotation`, 13 ns), because the save that is landing has already
marshalled it.

The bytes are immutable by construction: `json.Marshal` hands out a fresh buffer per call
and the CAS framing copies into its own, so nothing ever writes through the retained
slices. That is also a STRONGER provenance than a snapshot of live memory could be —
`buildPersistedSessionLocked` returns `sess.subs`, `sess.subIDs` and `sess.sharedSubs` by
reference and shares `*ConsumerConfig` pointers, so a memory-sourced snapshot would alias
state that keeps changing. And it inherits one rule for free: the writer omits a drain
that claims nothing, so the retained record can never re-assert a reference to a durable
whose deletion is already licensed.

The retention is **bounded**, not trusted: a record over `mqttGrantBodyMax` (64 KiB) is
not retained at all, and the session forgoes the repair rather than doubling its
footprint. Both bodies are cleared together in that case, because a parent whose child is
unknown computes no delta either. Stated as a residual in `docs/CONFORMANCE.md`.

**And the bound fails CLOSED on the survivor side.** The anchor consults only lineage, so
it still licenses a merge when the restore retained no body — an oversize survivor, or
bytes that stopped decoding. A survivor grant defaulted to empty there would make every
conflict invisible and invert survivor-wins: filters the survivor decided about would read
as absent-therefore-unconflicted, and the losing branch would overwrite them. The reclaim
therefore refuses the WHOLE merge for that pass when the survivor's grant cannot be
materialised — no per-filter fallback, no partial repair — and `mergeForkedGrantLocked`
takes the grant as a parameter so the fail-open default is unrepresentable. The capsule is
RETAINED across the refusal, not discarded: this pass's accept save can itself be rejected
and T2-re-driven onto a SECOND sibling of the same parent — the same leader-transition
mechanism that forked the record — and a smaller, readable sibling still satisfies the
anchor, so the repair applies on that pass. Once an accept save lands, the record is ours
and the anchor drops the capsule on the next reconnect; until then the cap bounds it like
every retained capsule. (The refusal is the holistic review's second round; the retention
is its third, correcting the second's own discard recommendation.)

**Rotation** happens at every adoption that can later mint, and the source is always the
exact record adopted: the resume restore (the loaded bytes, parent nil),
`saveWithDetail`'s success arm (the body it marshalled), and `adoptRecordLineageLocked`,
which every landed-proof site already calls beside its `sess.seq` adoption. Because the
body already rides `mqttSessSaveDetail` as `d.body`, and `mqttPendingWrite` already
carries it inside `framed`, no field was added to either. The restore sets the parent
**nil**, which is the honest answer: this broker holds no copy of that record's own
parent, so nothing about it is repairable. `clear()` drops both — and that asymmetry
against `retiredBaseWID` is the point: a sibling of the retired base must stay RECOGNISABLE,
but nothing of the retired incarnation may be RE-ASSERTED into the fresh one. **The
invariant is not "`grantBodyParent` always names `pwid`"; it is "a nil body means nothing
is repairable".**

**The grant representation.** One filter is one compound value, because QoS, No Local,
Retain As Published and the Subscription Identifier are ONE grant and merging them
independently resurrects stale options. `mqttGrantTerms` holds that acknowledged half as
comparable scalars; `sids` holds the per-sid durable state, which is EXCLUSIVE — a live
consumer or a drain, never both, the invariant `stageDrain` and `adoptDrainLocked`
maintain. A multi-level filter has TWO identities, the main sid and the level-up
companion, and a persisted sid is mapped back to its filter by
`buildPersistedSessionLocked`'s own reversal (`mqttUnsubSids` is forward-only, and a
record can persist a companion's drain after the filter is gone).

**The conflict predicate is membership, the options, and each sid's DRAIN PRESENCE** — not
the `*ConsumerConfig` contents, not whether a consumer identity is present at all, and not
a drain's started set. That line is exactly the one between a grant decision and its
implementation. A shape change is a grant consequence: only an acknowledged UNSUBSCRIBE
turns a live sid into a draining one. Consumer identity is not — the resume reconciliation
recreates a reaped durable with no MQTT decision behind it, so comparing identity would
turn a server-side repair into a subscription conflict and suppress a correct repair. A
drain's started set is in-flight accounting both sides advance independently, so it is
carried, compared only when both sides drain, and never merged entry by entry. **Delta**
equality (base vs ours) adds drain CONTENT and excludes consumer identity entirely; drain
equality, wherever used, is order-independent, keyed by `Seq`, over every
`mqttPersistedDrainSeq` field, because `startedSeqs` ranges a map and slice order carries
no meaning.

**The capsule** (`mqttForkedGrant`) is the delta, held on the account manager keyed by
client ID: the generation and the `(ackWID, ackPWID)` pair the verdict was reached
against, the per-filter changes, the cancelled-register nonces our record held and the
survivor's does not, and the PUBREL consumer. `CancelledRegs` is carried by **union**,
because the set is a monotonically accumulating record of fact, and it is merged through
`voidRegNonceLocked` rather than by assigning the map: the restore asserts the restored
set is already durable, so a merged nonce must be marked NOT durable, and that helper's
own insertion revision is what makes credit-every-landed-save persist it. The PUBREL
consumer is installed only where the survivor has NONE, and **never compared**: the
durable name is deterministic per generation but the config around it is not, so two
branches of one generation can hold different configs for one durable. Nothing else is
carried — `Will`, `ExpiryInterval`, `DisconnectedAt` and `Clean` are per-connection terms
the reconnecting CONNECT establishes, `Origin`/`ID` are identity, `Gen`/`WID`/`PWID` are
the anchor, `Q2Aff*` belongs to the affine path's own reclaim, `Teardown` is a marker, and
`SharedSubs` is excluded pending F-050 (the success CONNACK precedes shared restoration,
so a re-asserted group membership would be durable while a per-filter failure leaves no
join and no consumer — the accepted-but-dead state F-050 records).

**Seven mint sites, at the point the verdict is MADE.** Every disposition arm returns
after its record transaction is released, so minting at the classification point is what
samples the record and the pair under one fence: `probeRecordRejectionInTx` (covering the
bound and unbound rejection halves at once — the bound half never disposes, and between
its verdict and the asynchronous unbind the reconnect can arrive first), both
`classifyAttemptReadback` callers (the initial readback and the post-re-attempt one are
separate classifications), `resolveContendedRecordSave`'s moved closure,
`disposalForDirectRejection`, `evictDecideOnce`, and `resolveAcceptPair` — the accepted
CONNECT's paired classification, whose forked arm is reached from the FACT's verdict
rather than the record's and would otherwise leave that repair unrecorded.
**There is no site-specific refusal.** What makes that safe on a CONNECT that repaired
nothing is a property, not a special case — **an empty delta mints nothing**, and "empty"
means no changes, no nonces and no PUBREL consumer, since those are independent persisted
members. No other verdict
mints: the write landed (`mqttRejectionOurs`), contention is never ownership (`Keep`),
nothing was established (`Unresolved`), and a demonstrated successor descends from our
record so our change is in its ancestry. `evictDecideOnce` holds `as.mu` for its whole
decision and calls a `...Locked` form; the other six take `as.mu` themselves. Two named
functions, not one that guesses.

**The applier is the client's own CONNECT.** `reassertForkedGrantInto` runs in
`mqttProcessConnect` immediately after the accept record transaction opens and before that
block's own mutations, because a transaction that begins after the mutation it protects
cannot make that mutation invisible to another writer — and the `sessLocked` ownership
gate is not a `recordTx`. That placement makes one call serve both entry states (restored
and resident), puts the merge immediately before the save that persists it, and is
strictly after the CONNECT's own uncertainty resolution on every path including the
re-driven ones. A clean start needs no special case: `clear()` re-mints the generation
earlier in the same function, after which the anchor cannot match ([MQTT-3.1.2-6]). **No
repair write, no worker, no durable state** — the accept save's existing CAS is what
persists the merge, so the repair inherits its conditional arbitration, its rejection
classification and its T0/T1a/T1b/T2 re-drive.

**The anchor has THREE outcomes**, and it re-evaluates the licence rather than remembering
it:

- `wid == ackWID` → **keep**. A resident object that has not re-read the record still
  carries the capsule's own base, so the forked test would say not-forked and a
  two-outcome anchor would drop a capsule that is still valid. Keeping costs nothing: that
  accept save CASes at a sequence the record has moved past, is definitely rejected, and
  T2 re-drives to the authoritative restore, whose pass merges — which is also what
  preserves the no-extra-reconnect property.
- lineage forked against `(ackWID, ackPWID)` **and** `gen == g.gen` → **merge**. The
  generation conjunct is separately load-bearing because `classifyRecordLineage`
  deliberately ignores the generation: a clean-start sibling is parented across the
  teardown re-mint, so it classifies forked while belonging to an incarnation a teardown
  destroyed.
- otherwise → **drop**. Any write since the verdict makes the current record a CHILD of
  the survivor.

The anchor does **not** arbitrate ownership of the re-asserted state. Only the accept
save's expected-sequence CAS does that, and it linearizes only within a leader term.

**Capsule lifecycle.** A merge does not consume it: within the reclaim only the anchor
refusing it removes it, and the accept save landing is what makes the anchor refuse. So a
CONNECT that merges and then fails forfeits nothing and a re-drive simply re-merges,
idempotently, because the merge is a function of the survivor's content. Lookup does not
remove; the delete happens only on `drop` and only under pointer identity, so a capsule
minted by a newer verdict in between is not deleted by a stale decision. Two mechanisms
remove one from outside the reclaim, and they are separate rather than exceptions: a later
mint for the same client supersedes it, and `mqttForkedGrantMax` (256) evicts the oldest
by mint time. The anchor bounds HARM; the cap bounds MEMORY — a successful repair leaves
an entry behind by design. There is deliberately **no TTL**: an old but still-anchored
capsule is a correct repair, so a TTL would only add a second reason to refuse one.

**The apply rule has two halves from different sides.** The GRANT becomes ours —
membership, the options, and each sid's drain presence — because that is the acknowledged
decision the fork erased. The CONSUMER IDENTITY stays the survivor's wherever the
resulting state is LIVE, because identity is never a grant decision and the reconciliation
resolves the durable and deliver subject from whichever config the session holds. And **a
resulting DRAIN always keeps its own persisted `Cons`**: `drainByDur` is keyed by it,
in-flight registers correlate through the durable they were created under, drain recovery
queries that durable's consumer state, and completion deletes it — so re-pointing our
drain at a survivor's live durable would evaluate our started sequences against another
consumer's ack state, orphan our registers and eventually delete the wrong durable.

A change applies only where `mqttSurvivorDecidedGrant` is false — only where the
survivor's grant for that filter equals the parent's. The one refused row below is refused
PER SID, not per filter, and that is not a half-measure: a drained sid has no membership
on either side, so no membership change can accompany it, and a multi-level filter's two
sids drain and complete independently, so refusing one must not withdraw the other. Ten
action rows over nine state pairs, no row unreachable:

| survivor sid | ours sid | action |
|---|---|---|
| absent | live | install `cons[sid]` from ours |
| absent | draining | rebuild with `mqttDrainFromPersisted` and file it in BOTH maps (`restoreDrainLocked`) |
| live | absent | delete `cons[sid]` |
| live | draining | delete `cons[sid]`, install our drain **with its own `Cons`**; the survivor's displaced live consumer goes to the orphan sweep |
| draining | live | `adoptDrainLocked(sid)` — keeps the durable; the started obligations are re-established by the later durable scans |
| draining | draining, equal | nothing |
| draining | draining, differing | **refused**, a conflict. Losing nothing: the base was already draining, a committed drain adds no fresh claims and completion only removes them, so the survivor still names everything it says is owed |
| draining | absent | drop both map entries, delete NO durable. Reachable through last-claim completion: our record's omission IS the acknowledged completion, and `finishDrainIfEmpty` already deleted the durable after proving its own save |
| absent | absent | nothing |
| live | live | **nothing — the survivor's config is preserved**, stated explicitly rather than by omission; installing ours would repoint delivery at a durable the survivor may have replaced |

Every sid, not just the main one: a removal that dropped only the main sid would leave a
stale `cons` entry that is invisible durably (the writer omits a consumer whose filter is
absent) but makes `processJSConsumer`'s existing-consumer arm return on a later
re-SUBSCRIBE without rebuilding delivery interest, so that filter silently stops
delivering. A re-asserted removal keeps its drain's durable referenced, which is how the
started deliveries [MQTT-3.10.4-3] requires survive. **The reclaim deletes no durable on
any row**, creates none, and installs no delivery interest of its own — an addition whose
durable was reaped is recreated by the resume reconciliation's absent arm, which runs
after the merge and before the CONNACK. Charge accounting needs no transfer, and that is
provable rather than assumed: a merge is only reachable on a session that restored the
survivor's record this pass (no local save can produce the survivor's lineage pair), and
record-restored consumers and drains are deliberately uncharged. One `Warnf` per merge
names what was re-asserted and what was left to the survivor, so a non-repair is counted
rather than guessed at.

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

The marker (`recordRejectedSeq` plus `recordRejectedArmed`, memory-only) is armed
centrally at the store boundary, so a rejection site nobody enumerated is covered; its
validity is a predicate at the point of use — armed, equal to `sess.seq`, and not
unresolved — so every path that moves the sequence invalidates it without being
enumerated. The `!seqUncertain` conjunct is load-bearing: an unresolved session's save is
refused *before* the store, and a probe classifying on a zero-valued attempt would read
any real record as foreign.

**The armed flag replaced a non-zero sequence test**, and it is not bookkeeping. Zero
used to be a safe stand-in for "no marker" because a base-0 create could not be rejected
at all; now that every create is conditional it can, and a real rejection at base 0 must
arm the marker like any other. That leaves exactly one fact the predicate cannot derive
from the sequence, so `clear()` — the one route that re-mints at `seq = 0` — carries an
explicit obligation to DISARM. Left armed there, the marker would match the fresh
session's own sequence and gate its serving for the life of the session.

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
  exactly once by `mqttTakeWillLocked`, with the takeover discard rule applied by whichever
  side observes the displacement first while holding that lock.

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
| `$MQTT.willq.*` | delayed-Will outbox |
| `$MQTT.accept.*` | permanent per-CONNECT acceptance facts |
| `$MQTT.teardown.<domain>.<hash>` | generation-retirement watermarks |
| `$MQTT.origin.<domain>.<hash>` | session-interval registers: the identity a record-absent create inherits when no retirement watermark survives |
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
broker startup** with recreate guidance rather than being updated in place.
Nonconformance already present when a broker starts is refused by every
initialization alike; a config mutated *after* a manager initialized leaves that
manager serving on its original decision while any later initialization refuses.
Both recover the same way: stopping **all** brokers for the account and recreating
the stream, under the compatibility boundary above. This is not a rolling upgrade.

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

**A persisted qos-2 shared membership implies an affine generation.** No writer
here can separate them: the path is provisioned in the SUBSCRIBE's first filter
loop, before the membership is persisted, and the record builder writes
`SharedSubs`, `Q2Aff` and `Q2AffGen` as one record. A record that does separate
them predates the affine path, so it is **refused** rather than interpreted —
at CONNECT, in the startup expiry sweep, at a fired expiry timer, and in
`mqttValidateRecordAffinePair`, which the reference walk and teardown adoption
share.

Refusing it is not bookkeeping. `Q2AffGen` is what a contender compares a claim
against to conclude its winner is gone, and an **empty** one mismatches every
claim: read rather than refused, such a record turns a malformed shape into proof
of absence and discards an assignment owed to a delivery already started — the
redistribution [MQTT-4.8.2-6] forbids. `materializeQ2AffOnBehalf` therefore
validates the winner's record **before the comparison**, not inside one of its arms,
because the record is used in both directions: a mismatch discards the assignment,
and a match materializes the copy and **acknowledges the group source**, which is
irreversible and would strand the delivery on a session that can never resume. A
record can match a claim's generation and still be uninterpretable. Its sibling
`discardAbsentWinnerClaim` already validated before the same decision.

The rule is deliberately narrow:
shared QoS 0 and QoS 1 memberships provision no affine path, and a generation with
no consumer config — the shape the `minted` gate is built around — is one the
validator deliberately continues to permit.

It is narrow but not literal, and the difference matters. A persisted shared QoS
byte outside 0..2 is **also refused**, because `processSubs` CLAMPS a filter above
QoS 2 down to 2 while restoring and the resume feeds these values in verbatim: a
record carrying 3 becomes a live shared QoS 2 membership, so a rule matching only
the literal 2 would pass it straight through to the destructive readers the refusal
exists to stop. Every writer stores the value after that clamp has already bounded
it, so no record here can carry one. Whether any writer originates that
second shape is a pre-existing question about the affine design; it is not settled
here, and refusing it would be a new refusal of a shape whose non-existence is
unproven.

The four enforcement points call `mqttValidateRecordAffinePair` **whole** rather
than the new rule alone, which deliberately widens them: the three malformed
pairings it already refused for the sweep — a consumer config with no generation, a
malformed generation, and a config whose names do not match its generation — now
refuse a CONNECT and an expiry too, where before they resumed and were rewritten.
Splitting the predicate is the failure the validator's own comment records
("splitting them once let adoption delete a consumer the reference walk would have
refused to interpret"), and a record uninterpretable to the sweep is not one a
resume may rewrite.

Sharing the validator also means the refusal reaches `buildSessionRefs`, and there
the consequence is scoped **by resource class**. Such a record contributes no affine
reference while the qos-2 memberships it carries mean the session may own an affine
consumer and outstanding claims, so no affine resource may be judged: the reference
walk reports the affine half indeterminate and the caller skips affine claim
reconciliation and affine consumer deletion for the whole account, permanently,
since nothing rewrites the record and expiry will not remove it either. Its identity
and generation are sound, so the inbound, in-flight and PUBREL references it
contributes are complete and **those classes keep being reclaimed**.

That partition is the rule the affine claim inventory already follows, for the
reason stated at its own call site: otherwise one bad record disables every other
class account-wide, forever, on evidence about a different resource class. A record
whose *identity* cannot be established is the stronger case — the inbound and PUBREL
references are derived from the id hash and the generation, so nothing about it is
trustworthy — and that still aborts the pass outright. Both are logged as warnings;
only a record-shaped failure carries the recreate-the-streams guidance, because a
scan that failed on a timeout or a leader change resolves itself. The cost is stated
in `docs/CONFORMANCE.md`.

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

**Scope.** Affinity is a property of the brokers that consult `$MQTT_qos2assign`,
which is every broker serving the account — they all run this tree (see the
Compatibility boundary). It is stated as scope rather than as a rollout condition: a
broker serving the same shared group without consulting the claims would redistribute
a started delivery another broker had claimed, and there is no runtime capability gate
that would stop it. See `docs/CONFORMANCE.md` for this and for the packet-identifier
residual.

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
**out** of the record's `Cons` map, because in this tree `Cons` restores straight
into `sess.cons` and a live re-SUBSCRIBE that finds `cons[sid]` takes the
exists-branch: a drain carried there would satisfy that lookup and the filter would
never get a delivery subscription. Drains restore through their own path instead, and
a re-SUBSCRIBE reclaims one by adopting it explicitly.

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
  could act on it; four more now can — five of the seven call sites in all — so the flag
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

### The generation-retirement watermark

The claim licenses the deletions, and the release that says whether the claim is
still ours runs *after* them — it has to, because it proves absence rather than
authorship, and moving it earlier only lets an already-admitted proposal recreate the
same shape one step later. So while the deletions run, the marker is the only durable
record of the retirement, and the marker lives on the record's own subject. A
cross-term resume derived from the same base can apply after it landed (per-subject
CAS is checked pre-proposal, never at apply — see "JetStream behaviors this design
depends on") and `MaxMsgsPer: 1` then **evicts** it. What survives is a record naming
exactly the resources the winner deleted, with no marker for an adopter or the sweep
to find: a **hollow survivor** that a reconnect resumes with Session Present 1 for
consumers, holds and in-flight registers that no longer exist.

The fix keeps the retirement fact **off the replaceable subject**. Every teardown that
wins its claim first records a permanent per-client watermark on `$MQTT_teardown`
(`{id, gen, mseq, rwid, nonce, origin, started}`, one subject per client,
`MaxMsgsPer: 1`), and only then deletes. It is written on both writers — `clear()` and
`finishAdoptedTeardown`, which deletes the same resources under a marker just as
evictable — always confirmed, even on a no-wait discard: a fence that only reached a
send queue is not a fence. There are two direct writers and a **third initiating
path**: the CONNECT rollback's zero-lifetime branch, which routes through `clear()`
rather than fencing on its own (see "Two frontends taking over one session from the
same base"), so nothing else can record a retirement without holding the base. A write
that cannot be confirmed **aborts the teardown with nothing deleted**, leaving the record
marked, which is the already-handled self-healing state.

**A generation whose retirement was ever recorded is retired, and stays fenced.**
`mseq` — the `$MQTT_sess` sequence the claim marker landed at — is the ordering field,
and replacement is decided by it *alone*: a standing watermark at or above ours is
already at least as new, a lower one is CAS-replaced at the sequence we read, and a
rejection is resolved by re-reading rather than classified. The generation plays no
part in that decision, because a generation-equality early return would let a newer
same-generation retirement be skipped and leave a paused older writer's write final.
The monotonicity is **decision-time** monotonicity; an admitted CAS that applies after
a leader transition can still regress the single slot, which is a stated residual.

**`rwid` names the RECORD the deletion removed** — the record the teardown claimed,
never the marker that claimed it — and it is REQUIRED: a watermark without it is
corrupt to every reader, because it is the field a create inherits as its parent (see
"Recognising a forked session record"). It cannot be optional and cannot be filled in
later: the validator cannot tell "field absent" from "field present and empty", so
accepting an empty one would accept the pre-`rwid` shape the compatibility boundary
refuses. By the same rule, a teardown of a record carrying no `WID` **fails closed**
with the record left marked, rather than retiring a record on a name nothing can
attribute.

Four readers consume the fence, and each applies the same validation
(`mqttValidateGenRetirement`: decodable, the client ID hashes back to the subject
token, a valid generation, a nonzero `mseq`, a non-empty `rwid`) with the same
fail-closed disposition — a watermark that is unreadable or malformed in any field is
never repaired in place, because it may name a retirement newer than the reader's own:

- **The winner's release-superseded arm.** The release now hands back the record it
  classified against; when that record carries the generation just retired and no
  marker, it is the hollow survivor and is condemned with a fresh teardown claim.
  **Mark only** — no deletions, no release — because a marked record is already the
  fully-handled state: the survivor's broker observes the move on its session-record
  watch (or the committed-read backstop) and evicts, its client's reconnect adopts the
  marker and starts fresh with Session Present false, and with no client the expiry
  path or the sweep finishes it. A *different* generation is a legitimate successor and
  is left alone; a record already marked by a foreign teardown belongs to that
  teardown's finisher. `finishAdoptedTeardown` carries the identical arm, because its
  deletions ran on the same licence.
- **The CONNECT restore.** One exact-subject read per record-restoring CONNECT: a
  record whose generation the watermark names is routed into the *existing*
  overdue-discard shape rather than resumed, so there is one discard path and not two,
  and the client gets Session Present false — the truthful answer. This is the only
  prompt reader for a winner that crashed after recording the retirement; without it
  every reconnect resumes the same hollow record until the sweep runs.
- **The orphan sweep**, for a client that never comes back. See "Orphan
  reconciliation".
- **The record-absent CREATE**, which is the only non-destructive reader: it draws no
  conclusion about a record, because there is none. It reads `rwid` as the parent its
  first write descends from, immediately AFTER the proof that the subject is empty —
  and that order is load-bearing, since the watermark is confirmed before the deletion
  it precedes, so a watermark read after observing the subject empty is at least as new
  as the deletion that emptied it. Its dispositions differ from the destructive readers'
  in exactly one row, and deliberately: **corrupt REFUSES the CONNECT**, because
  admitting a session over a watermark this broker cannot read leaves its own later
  teardown unable to record a retirement — the record would then stay marked for every
  future finisher — while **indeterminate concedes**, creating parentless rather than
  failing a fresh CONNECT over a transient `$MQTT_teardown` read. Nothing unsupported is
  established by conceding: no record exists yet, and a later teardown re-reads and
  fences normally. **Absent also concedes**, and makes no positive claim from the
  absence: `$MQTT_teardown` can be recreated alone and validate clean with no historical
  fences, so "no retirement is recorded here now" is not "none ever happened". That is
  the stated residual in `docs/CONFORMANCE.md`.

Two things a marker means constrain the three DESTRUCTIVE readers: it licenses its
adopters to delete every durable the record names, and the CONNECT discard deletes them
itself. So each
of them validates the record with `mqttValidateTeardownTargets` before it acts — the
CONNECT *refuses* rather than discarding — and a record whose names are not this
session's is condemned by nobody. The create-side reader names no target and deletes
nothing, so it has none to validate.

Nothing ever reaps a watermark. Storage is bounded by the client-ID population, and
permanence is what keeps the residual from being widened by a GC choice. Because
`mseq` is a `$MQTT_sess` sequence, the two streams are one logical unit:
`$MQTT_teardown` carries the adopted session-stream incarnation epoch in its
metadata and a mismatch is **refused at startup** — see
"Stream prerequisites".

The deliberate exception to the fence is a crash *between* the won claim and the
watermark write. It is non-destructive by construction, since the write precedes every
deletion, so that generation has lost nothing and is legitimately resumable until an
adopter fences it.

**Every session carries a generation.** A record without one is **rejected** at the
boundary rather than resumed on hash-only names: it predates this design, and under
the compatibility boundary above the development store is recreated. Accommodating
it would not be a smaller version of the same safety — a hash-only session has no
identity to scope an arbitration with, so the inbound QoS 2 release claim could not
be arbitrated for it and the discard would fall back to a blind hash-wide purge,
which destroys a held payload whose releaser claimed it and then died. A
partially-arbitrated path is worse than a rejected one.

What this leaves open is stated in `docs/CONFORMANCE.md`: the fence names only the
latest retirement, and the single slot can itself be regressed by a cross-term apply.

### The session-interval register

`RWID` answers the record-absent create whose watermark read is **valid**. When that
read is **absent** there is nothing to inherit, the create stays parentless, and
`classifyRecordLineage` requires a non-empty acknowledged parent precisely so two
unrelated first creates are not read as siblings. The cost is not vocabulary: the
conceding side reports 0x8E and **cancels its delayed Will**, where a recognised fork
reports 0x89 and destroys no durable Will state. Absent covers a genuine first-ever
interval, a client whose retirement history was **lost** (`$MQTT_teardown` recreated
alone, an administrative deletion, a partial restore), and a transient read.

**An absence is not a fact**, so the answer cannot be a reserved token stamped on that
arm: that design was refuted, because it mints a positive claim out of missing evidence
and its failure direction is a false fork — the one direction the recognition exists to
prevent, where every other row concedes. What closes it is a positive durable fact of
the interval itself: the **per-client interval register** at
`$MQTT.origin.<domain>.<hash>`, `MaxMsgsPer: 1`
(`internal/mqtt/sess_origin_interval.go`).

**The identity must ADVANCE, not merely persist.** A permanent per-client token would
be shared by every interval of that client, and a shared parent pointer is a false fork
as soon as two records carrying it are classifier-visible to each other — which is not
the same as durably co-existing: `classifyRecordLineage` compares a broker's in-memory
*acknowledged* pair against whatever record exists now, so a broker whose own record has
already been deleted is still a counterparty. One stranded owner plus one erased
watermark suffices. The register therefore has exactly one rule: it moves forward at
every interval boundary, and no two intervals hold the same value.

**The identity is `<origin-epoch>.<sequence>`, and the sequence is the advancing half.**
Every store gets a fresh sequence assigned at propose time (see "JetStream behaviors
this design depends on"), so a delayed or duplicated writer replaying identical bytes
produces a *different* identity, never a reused one. A body field cannot promise that —
identical bytes are what a re-sent claim produces — so every field in the register body
is **diagnostic only** except `ID`, which must equal the connecting client's ID
**exactly**: `getHash` keeps eight base-62 characters, so it is collision-resistant but
not injective, and two colliding client IDs sharing one register would share one parent
pointer. The epoch is the register stream's own incarnation identity, because sequences
restart at 1 in a new incarnation.

**Every retirement rotates it, inside `recordGenRetirement`.** That is the single site,
which makes it structurally impossible to record a retirement without rotating the
identity; both callers (`clear()` and `finishAdoptedTeardown`) inherit it, as does any
future writer. The placement has two load-bearing edges: **after** the function's own
validation of the watermark it is about to write, or a malformed target would rotate the
identity and then fail without deleting anything; and **before** the watermark decision
loop, whose "a standing fence is already at or above ours" early return still goes on to
delete the record, so a rotation past it is skipped exactly where identity reuse would
result. A rotation that cannot be confirmed **aborts the teardown with nothing deleted**,
the same self-healing state the watermark write's own failure leaves.

**The rotation is unconditional: one plain confirmed publish, no read, no CAS, no
equality guard.** Duplicate finishers are ordinary — `finishAdoptedTeardown` exists so
the original owner and adopters can finish one marker concurrently and repeatedly — so
an unguarded rotation moves the register more than once per retirement, and two creates
straddling the second rotation derive different identities and both concede. Three
guards were designed and each was worse than that: `MarkerSeq` (one retirement occupies
several `$MQTT_sess` sequences, so it double-rotates anyway), the claim nonce (it proves
duplicate claim *bytes*, not a duplicate *boundary*, so a marker replayed after its
release declines the rotation and one identity spans two intervals), and the retired
record's `WID` (commutative within one retirement, but it does not *order* different
ones, so a paused finisher resuming after a later retirement released **regresses** the
register). The pattern is general: any guard that can **decline** a write can leave an
earlier interval's identity standing, which is fatal, while an unguarded rotation can
only mint a fresh sequence, which no earlier interval holds. The divergence is therefore
taken and stated as a residual rather than fixed.

One property keeps that divergence narrower than it looks: **a create can never read a
rotation in progress.** The rotation runs while the marker still occupies the record
subject, and a create that finds a marker takes the adoption branch instead of reading
the register. Divergence needs a finisher that rotates *after* the release.

**The create arm**, reached only after the record subject is proven empty and the
watermark verdict is absent:

- **valid, and ours** — the identity is the sequence it stands at.
- **valid, but naming a different client sharing this hash** — concede, and do not
  establish over it. The subject is occupied, and refusing the CONNECT is not available
  because the register cannot be deleted, so a refusal would brick that client ID
  permanently. The two clients deny each other recognition.
- **absent** — establish with a create-only CAS and take the identity from our own
  confirmed sequence. A rejection is never read as "a register exists" (the server folds
  a load error into 10071, and 10164 is contention), so it is resolved by an
  authoritative re-read, bounded.
- **corrupt** — concede without establishing, for the same occupied-subject reason. The
  next rotation replaces it unconditionally. That is the deliberate asymmetry against
  the retirement watermark, which is **never** overwritten when corrupt: a watermark
  that cannot be read may name a retirement newer than ours, so replacing it could
  unfence a generation, whereas the register is not a fence — it is an identity that
  must only move forward.
- **indeterminate, or a failed establish** — concede. Nothing on this path ever fails a
  CONNECT; the register is a recognition aid, not a prerequisite of a session.

A **transient watermark read** concedes without touching `$MQTT_origin` at all: it
establishes no retirement fact, so an identity taken there could be stamped onto an
interval whose retirement state merely could not be read.

**Every concession is logged**, naming the client and the reason. The CONNECT succeeding
is the point, but a client that cannot derive an identity silently loses cross-term
recognition and takes the takeover disposition instead, and a permission fault or an
unreadable register can leave it that way indefinitely. Refusing automatic eviction
because it would degrade this feature without a diagnostic would be hollow if the runtime
paths did the same.

This is not a second mechanism stating the watermark's fact. The watermark says which
generation was retired and which record its deletion removed, on a stream an operator
can prune — and that loss is the case being closed here; the register says which
interval the subject is in, on a stream nothing ever deletes from and whose sequences
only move forward. They are read by disjoint arms, which is also why `RWID` is kept:
it is free at its consult point, since the valid arm reads it out of a load that arm
already performs, while unifying would add a read to every reconnect after a teardown.
`docs/ROADMAP.md` records
the condition under which that retention should be reconsidered.

What this leaves open is stated in `docs/CONFORMANCE.md`: the transient read, a late or
duplicate finisher splitting one interval into two identities, the cross-term
double-apply on `$MQTT_origin`'s own Raft group, the widened R1 and R4 shapes, a restore
or live recreation that regresses or replays the register's sequences, administrative
pruning losing recognition for the pruned client, and two client IDs sharing a hash
denying each other recognition.

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

**Then retired survivors are condemned.** The pass walks `$MQTT_teardown` — the
watermarks, one per client, bounded by the client-ID population — and for each valid
fence loads the client's record. An unmarked record carrying the retired generation is
the hollow survivor of the claim-won deletion window, and with no live winner and no
CONNECT this is the only reader left for it. Eligibility is a stable observation of
**both** identities, the record's sequence and the watermark's, so a resave, a peer's
teardown or a newer retirement restarts the wait; the record is then validated with
`mqttValidateTeardownTargets` and re-read at those exact sequences, and the sweep
writes a teardown claim.

It **marks only**, and the *finish* is left to the marker-adoption class on a later
pass. That ordering is deliberate: releasing the record in the same pass would remove
it moments after the marker landed, and a survivor broker whose lossy record watch
dropped that brief observation would face an absent record rather than a moved one —
which is the F-036 shape, where a bound Keep Alive 0 client strands. Deferring narrows
that route to the case where the survivor's committed-read backstop does not reach the
record in time; it does not close it, and a CONNECT that adopts the marker finishes
immediately by design (see `docs/findings/ha-arbitration.md#f-036`). Nothing here
reaps a watermark, so there is no reap step and no GC choice that could widen the
escape the fence covers.

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

**PUBREL durables are reaped too, and nothing else ever revisited one.**
`sweepOrphanedConsumers` lists `$MQTT_msgs` only, this pass's affine class drops every
name without the affine prefix, and the reference snapshot had no PUBREL class at all —
so a `$MQTT_PUBREL_<idHash>_<gen>` consumer created just before the record save that
names it (`ensurePubRelConsumerSubscription` provisions it first) was unreferenced
forever if its broker died in between, holding its interest-retained PUBRELs with it.
The class mirrors the affine one: the durable is parsed back to `(idHash, generation)`,
its config must match all three names that identity derives — durable, filter subject
and delivery subject — at discovery *and* again at the pre-delete lookup, since a
consumer that disagrees with its own name cannot be attributed at all. A record is a
reference through the `PubRel` durable it names verbatim, and a *local* session through
the name derived from its `(idHash, generation)` unconditionally, which is what covers
the create-before-save window on this broker. The class shares nothing with the affine
inventory and is deliberately not gated on it: an unreadable assignment claim aborts
the affine phases, and only those, because one persistent corrupt claim must not
disable PUBREL reclamation account-wide on evidence about a different resource class.
Deletion is authorized by a final
authoritative record re-read — a save landing after the reference walk aborts it, and
does not change `Created`, so the instance check alone could not catch it — and then by
re-looking-up the same instance. Deleting the consumer drops the PUBRELs it held, which
is correct in every state those checks can observe: no observable session could deliver
them and the identifiers died with the session. Both windows are narrowed rather than
closed, because a consumer delete is by name and unconditioned; that residual, and the
resume-side verify-and-recreate that bounds it, are in `docs/CONFORMANCE.md`.

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

## Connection liveness bounds

Three bounds cover a connection's life, in this order. Each covers a distinct
window, and none of them substitutes for another:

1. **TLS handshake deadline** — a socket read deadline set before the
   server-side handshake and cleared on success (`mqttTLSTimeout`,
   `shim_engine.go`). TLS listeners only.
2. **CONNECT deadline** — from the accepted (and, on a TLS listener,
   handshaked) connection until its CONNECT is admitted
   (`internal/mqtt/connect_deadline.go`).
3. **Keep-alive read deadline** — armed from the negotiated Keep Alive after
   CONNECT, and re-armed per read only while that value is nonzero
   (`mqttParse`). A client that negotiates Keep Alive 0 is bounded by nothing
   after CONNECT; see `docs/CONFORMANCE.md`.

The CONNECT deadline exists because a connection that has not sent CONNECT has
no client id, no session and no subscriptions, so nothing else in the broker can
name it, evict it or bound it. Until it existed such a connection held its
socket, its `s.clients` registration, its `clientWG` token, a readLoop goroutine
and a writeLoop goroutine for as long as the peer kept the TCP connection open.
MQTT 3.1.1 / 5.0 [3.1.4] says the Server SHOULD close a Network Connection that
does not deliver a CONNECT in reasonable time.

**Unconditional and fail-closed.** It is armed for every accepted connection,
not only when an authentication mechanism is configured: the resources a silent
connection holds are the same either way, and `-auth-passthrough=false` is a
supported configuration. `mqttConnectTimeout` resolves the window per server from
`*Options` at the point of use (MQTT-specific value, else top-level, else
`mqttDefaultConnectTimeout` = 5s), and a zero or negative value falls back to the
default rather than reaching `time.AfterFunc`, where it would close every
connection immediately. `-connect-timeout` is the operator surface; `Validate`
refuses `<= 0`, so "disabled" is not expressible.

**The arbitration is a state under `c.mu`, not `Timer.Stop()`.** Exactly one of
two racing facts wins for a connection: the CONNECT arrived
(`mqttAdmitConnectLocked`), or the deadline expired (`mqttExpireConnectLocked`).
`Stop` reports that the timer had already fired; it cannot order itself against a
callback that is about to decide, so it is not an arbitration. The loser does
nothing: a late callback that finds the CONNECT admitted leaves the socket alone,
and a CONNECT that finds the deadline expired is refused with
`errMQTTConnectDeadlineExpired` **before** `isClientAuthorized` — so a connection
that lost dials no per-client NATS connection and binds no session state.

**The callback closes `c.rawConn` only**, never `closeConnection`. The ordered
teardown belongs to `readLoop`'s `disconnect()`, which runs
`mqttHandleClosedClient` (nil-safe pre-CONNECT) before `closeConnection`; closing
the accepted socket also tears down any TLS layer above it. `shutdownDisconnect`
has the same shape for the same reason. The timer is stopped in
`closeConnection`, which is the teardown every connection that can have an armed
timer reaches — a malformed first packet, a non-CONNECT first packet, a peer
hang-up. On those paths the admission state stays `pending`, which is correct:
neither racer won.

**Not tracked by a WaitGroup**, unlike the delayed-Will timers on `willTimerWG`.
A Will timer publishes through JetStream and must finish before shutdown stops
the service connection; this callback closes a socket and returns. It depends on
nothing shutdown tears down, and every armed deadline belongs to a client that
shutdown Phase 1 already waits for — registered (drained, readLoop exits,
`closeConnection` stops the timer) or still in setup (`connSetupWG`).

**What it is not.** It makes an idle connection's lifetime finite; it is not a
DoS defense. A peer can still hold roughly `accept rate x window` connections
open. `MaxConn` remains the absolute cap and `admitConnSetup` the shutdown gate.
