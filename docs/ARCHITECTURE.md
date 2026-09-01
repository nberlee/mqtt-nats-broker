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

**Every sequence identity is scoped to one stream incarnation.** A `$MQTT_*` stream whose
sequence space restarts breaks every mechanism in this design that treats a stream
sequence as an identity: the per-subject CAS expectations, each exact-sequence claim
delete, `releaseQoS2Record`, the orphan sweep's confirmation re-read, and the fence a
`discarded` marker records (F-062). Recreation is therefore an **all-brokers-stopped**
operation, which is the same conclusion the paragraph above reaches for a nonconforming
stream shape.

**And it is never a SINGLE stream.** The nine broker-owned streams are one recreation
unit: startup refuses an account holding a proper subset, and refuses a complete bundle
whose members carry different recovery epochs, so the supported operation is a reset of
all nine together (F-066; see "The nine streams are one recreation unit"). Recreating one
stream is not a smaller version of that procedure — it is the procedure the enforcement
exists to refuse, and an operator who worked around the refusal by copying the surviving
epoch onto a replacement would reinstate exactly the stale-identity hazard the epoch is
there to detect.

Stated here once so the mechanisms can rely on it uniformly: a design must not add a
per-site diagnostic that reads like a fence without being one, and must not describe a
mid-flight recreation as a residual it handles.

### Trusting the backend

This bridge is built **on** NATS and JetStream. They are a dependency we trust, not
a system we audit into submission. The list below states the behaviours we rely on
so that a version bump has something concrete to re-check, and so a reader can see
which properties are load-bearing. It is not a claim that we have independently
proved any of them, and it is not a licence to rebuild them.

Some upstream guarantees cannot be proved from outside the server, and a few cannot
be proved at all without reimplementing the mechanism that provides them. That is a
normal property of depending on software, not a defect in our design. The correct
response is to name the contract we depend on and move on. Reaching instead for
"we cannot prove this, therefore we must implement it ourselves" is the wrong
conclusion: it duplicates a dependency, transfers a distributed-systems
problem from a mature project onto our own permanent maintenance surface, and buys
no assurance that the thing we wrote is more correct than the thing we replaced.
An unprovable upstream claim is a reason to write down what we assume. It is not a
reason to write a second consensus layer, a second dedup layer, or a second
arbitration layer beside the one we already get.

Two bounded exceptions, and neither reopens the general question. Where a contract
is cheaply checkable and a regression in it would otherwise be **silent**, a canary
keeps it executable across dependency bumps (`test/depproof/`, `make depproof`);
that is a small fixed cost proportional to the risk of not noticing, not a re-proof
of the dependency's fundamentals. And where a **startup-checkable, initialization-time
stream prerequisite** would withdraw a behaviour we depend on, we refuse it at
initialization rather than working around it (see "Stream prerequisites"). That
refusal is a snapshot, not a standing guarantee: a configuration mutated after a
manager initialized leaves that manager serving on its original decision, which is
the configuration-drift residual in `docs/CONFORMANCE.md`.

The distinction that actually matters when a race turns up is not whether NATS can
be trusted. It is **whose gap it is**:

- A gap in **our independent evidence** for a guarantee JetStream asserts is a
  dependency question. Name what we assume, and if a silent regression is plausible,
  add a canary row. This is not licence to record an assumption in place of a
  guarantee that does not exist: where JetStream genuinely does **not** guarantee a
  property we require, the design must constrain what it depends on, replace the
  mechanism, or stop depending on it.
- A gap in what *we asked* JetStream to enforce is ours, and it is ordinary local
  work. If an operation carries no condition, the server has nothing to enforce and
  cannot be faulted for not enforcing it. A subject purge that supplies only a
  filter is the worked example: it removes whatever occupies the subject when it
  applies, because that is precisely what we asked for.

Conflating the second case with the first is how a local protocol defect gets
mistaken for an unprovable upstream property and then documented as a residual
instead of being fixed. Trust the backend for what it guarantees; own what we
requested of it.

### JetStream behaviors this design depends on

Correctness rests on these specific semantics, each stated as the contract the
code assumes. This is the list a version bump has to re-verify — it is what makes
the "no fallbacks" stance reviewable rather than implicit.

The pinned dependency is **nats-server v2.15.0-preview.1**. It was raised from v2.14.5
for the fix to an upstream `CONSUMER.CREATE` versus `CONSUMER.LIST` data race
([`nats-io/nats-server#8478`](https://github.com/nats-io/nats-server/pull/8478)):
`getPublicConsumers` read `o.cfg.Direct` while `updateConfig` wrote `o.cfg` under a
different lock, and it now reads an immutable `o.direct` instead. The measured rows of
this list are re-verified on every bump by `make depproof`, which is what turns the
paragraph above into a gate rather than an aspiration; that run also had to be re-pointed
for this bump, because upstream moved the atomic-batch apply state off the stream and into
a monitor-goroutine local (see `test/depproof/README.md`).

The rows `depproof` does **not** measure were audited against the two source trees for that
bump, row by row. Unchanged: the purge request still carries filter and sequence together
and `PurgeEx`'s `sequence == 1` no-op is byte-identical in both stores; `STREAM.INFO` still
fills `State` and, separately and only when subjects are requested, `SubjectsTotals`;
`PubAck.Duplicate` is the same field on the same struct; per-message TTL
(`AllowMsgTTL`) and the double-ack path (`processAckMsg`) are unchanged; and
`enforceMsgPerSubjectLimit`, the leader-only message-get guards, the dedup map's
independence from message removal, the `ConsumerLimits.InactiveThreshold` application and
the `JSExpectedLastSubjSeq` handling all carry over.

**Three deltas were found, and the third is the one that mattered.** One `InterestPolicy`
guard was dropped from the stream **update** path, which governs a stream whose retention
is *changed* after creation — something this bridge never does, since it creates each stream
with its intended retention and refuses an unsupported persisted shape — while the
store-side `interestRetention` check the QoS 2 affine path depends on is intact. The dedup
map gained a reference rather than losing any. And **`recalculateClusteredSeq` dropped its
`+ batchCount` term**, computing `mset.clseq = lseq + mset.clfs` where v2.14.5 computed
`lseq + mset.clfs + batchCount`.

That last one is a real change to how a new leader picks the next sequence, and it is safe
for the same reason the batch state moved: a buffered batch's members are now folded into
`clfs` when the batch is abandoned (`rejectBatchState` does `mset.clfs += batch.count`), so
the count `recalculateClusteredSeq` used to add separately is already inside `clfs` by the
time a new leader recalculates. Adding it again would now **skip** sequences rather than
protect against reuse. The bridge does not take this on source reading alone: the
accept-batch fixture's post-`StepDown` assertion is exactly that every replica's CLFS equals
the abandoned prefix length, so the folding is measured on every `make depproof` run.

- **Per-subject CAS** (`Nats-Expected-Last-Subject-Sequence`). A store carrying it
  is accepted only if the subject's last sequence matches. The check is made before
  the proposal and is not repeated at clustered apply (`canConsistencyCheck :=
  !isClustered`, `server/stream.go`), which for years made "two writes from one base
  across a leader transition" the premise every write-lineage mechanism here was
  designed against. **That premise is now retired rather than assumed** (F-030), and it
  did not hold up: both routes by which such a write could reach a proposal are closed.
  An operation whose
  captured proposing term outlives that term is refused by Raft's comparison of the
  captured term against the current one (`nats-io/nats-server#8370`, 2.14.4); a
  current-term write, which that comparison cannot refuse, is refused by a readiness
  envelope under which a node behind on applies neither leads nor serves, and then by
  the per-subject expectation a serving leader reconstructs from its own APPLIED
  state. `test/depproof/` carries the route inventory, the fixtures, written refutations
  for seven other shapes, the scoped post-start mirror/source drift exclusion, and one
  discriminating mutation per live mechanism; `make depproof` runs them against isolated
  nats-server checkouts, and a
  version bump must. The harness does not carry the closure alone: the pinned source
  identifies the same boundaries, and a NATS maintainer explicitly confirmed that
  leader-local in-flight ownership plus applied-before-serving guarantee correct header
  enforcement across elections
  ([`nats-server#8469`](https://github.com/nats-io/nats-server/issues/8469#issuecomment-5304075553)).
  What this does NOT license is deleting the lineage machinery wholesale: those
  mechanisms also cover ambiguous acknowledgements, out-of-band deletion and stream
  recreation, which are not proposal-ordering questions, and each remaining per-stream
  finding owns its own consequence audit. `$MQTT_qos2in`'s audit ran (F-032, closed):
  every write there carries its expectation, so it needed no lineage — and what it did
  find was a hold purge carrying no condition at all, which is F-062 (closed: the purge
  now carries one, see the next bullet) and F-036 rather than anything this contract
  speaks to. `$MQTT_qos2assign`'s ran too (F-033, closed): every claim write is funnelled
  through one CAS helper that always emits its expectation, so it needed no lineage
  either — and what it found instead was a source acknowledgement bound to a copy store
  that never proved retention, a reference skip that leaves claims to owner resume, and a
  reap on a stale copy reading, and a claim key that outlives the `$MQTT_msgs`
  incarnation it indexes — F-063, F-064, F-065 and F-066 rather than anything this
  contract speaks to. `$MQTT_inflight`'s audit ran on 2026-08-22 (F-034) and reached the
  same verdict on lineage — but it is the one of the three that found a REACHABLE defect,
  and the remedy is identity conditioning plus bounded destruction rather than lineage.
  A rejection is reported structurally
  (`JSStreamWrongLastSequenceErrF` 10071, or 10164 under clustered inflight
  contention). 10164 means *transient contention*, not "the subject moved", so
  every rejection is resolved by re-reading rather than classified directly.
- **A stream purge takes a subject filter and a sequence CUTOFF together**
  (`JSApiStreamPurgeRequest`). `Sequence` means "purge up to but **not including** this
  sequence" and combines with `Subject`, so one request can remove a subject's records at
  or below a chosen watermark and leave everything above it
  (`server/jetstream_api.go:495-502`, v2.14.5). Three properties this design relies on:
  `Sequence: 1` is an explicit no-op in both stores (`server/filestore.go:9933`,
  `server/memstore.go:1447`), which is why a zero cutoff is refused rather than sent — it
  would report success for a purge that removed nothing; the two stores agree on the
  cutoff's **semantics** while differing in mechanics (`fileStore.PurgeEx` clamps a block's
  upper bound only when the cutoff falls inside that block's filtered range,
  `filestore.go:10048`, where `memStore.PurgeEx` sets it unconditionally,
  `memstore.go:1481`); and the clustered path proposes the whole request through Raft
  (`jetstream_cluster.go:8816`) substituting a leader-stamped `LastSeq+1` **only when
  `Sequence == 0`** (`:4308-4317`), so a non-zero cutoff travels verbatim and upstream
  itself treats `Sequence: 0` as the shape needing care. The corollary that matters: an
  unconditioned purge on a clustered stream is bounded only at PROPOSAL time, which is
  after the caller's own enumeration, and on `R1` it is not bounded at all. This is what
  `purgeQoS2Hold` depends on (F-062); `StreamState.LastSeq` arrives with the enumeration
  that supplies the cutoff, though see the next bullet for what it is not.
- **`STREAM.INFO`'s `State` and `State.Subjects` are two separate store reads**, not one
  snapshot: the response's `State` is built by `mset.stateWithDetail` and `State.Subjects`
  is filled afterwards by `mset.store.SubjectsTotals` (`server/jetstream_api.go:2018-2045`,
  v2.14.5). So a subject can appear in the returned map while its record sits ABOVE the
  returned `LastSeq`, and a walk begun against an empty stream can be handed a non-empty
  map with `LastSeq == 0`. The map carries per-subject **counts**, not sequences, and is the
  later sample, so the pair licenses **no inference at all** about where the returned
  occupants sit: they may lie on either side of `LastSeq`. `LastSeq` is usable for exactly
  one thing — as a destructive cutoff — and never as an inventory of what is at or below it.
  Both directions are handled where they are depended on (F-062): a record above the cutoff
  is spared, and an unusable watermark is re-sampled rather than acted on.
- **An atomic batch is indivisible at APPLY, not at Raft** (`AllowAtomicPublish`).
  Raft preserves nothing about a batch: `ProposeMulti` pushes the entries
  individually, `runAsLeader` pops whatever prefix exists, and each regrouped batch
  rechecks leadership on its own — so a committed FIFO prefix without its commit
  entry is a real outcome. It applies **nothing**. Batch identity is carried in the
  stream entry encoding instead (`encodeStreamMsgAllowCompressAndBatch` stamps every
  entry with its batch id and sequence and marks the last one a commit op), and
  `applyStreamEntries` enforces it: a committed non-commit member is counted and its
  entry buffered in memory, never stored, and only a gap-free commit op replays every
  buffered member into the store under a single `mset.mu` hold. An abandoned prefix is
  accounted for through `clfs` and stores nothing on any replica. That is what makes
  the accepted-CONNECT pair below indivisible, and it is **measured** rather than
  argued: `test/depproof/`'s R-batch row, its `m-batch-apply-eager` mutation, and a
  bridge sentinel over a real R3 `$MQTT_sess`.

  Two limits belong with it. The guarantee is a **historical commitment at the moment
  of the write**, not an invariant over time — the session-record subject is a
  `MaxMsgsPer: 1` register, so a later legitimate save replaces what the batch
  committed. And a hard kill *between* two members' store writes **can** leave partial
  durable residue on a replica: each member goes separately to `StoreRawMsg`. Recovery
  skips the stored members and applies the remainder, so the post-recovery state is the
  whole batch — convergence, not the absence of residue.
- **`MaxMsgsPer: 1`** turns a subject into a single-slot register: a new record
  evicts the old one, so a stream sequence names a record, never a subject.
- **Every store receives a fresh sequence, assigned before the proposal and applied
  in log order.** The leader stamps the sequence into the replicated entry and every
  replica applies entries in that order (`server/stream.go`), so no two accepted
  writes to a stream ever share a sequence and store state is always a prefix of the
  log. That is what makes a sequence usable as an **advancing identity** where a body
  cannot be: identical bytes are exactly what a re-sent claim produces, so a body
  field can be replayed while a sequence cannot.
- **A named consumer create ADOPTS, unless it says `ActionCreate`.**
  `CreateConsumerRequest.Action` 0 is create-or-update, so re-issuing a create for a
  name that already exists returns the EXISTING consumer at its current position and
  replays nothing — measured against the pinned server, standalone and clustered. With
  `ActionCreate` the server compares the submitted config against the existing one,
  **including custom metadata**, adopting only a byte-identical config and refusing
  anything else with `consumer already exists` (10148). Two consequences the ephemeral
  consumers depend on: a fresh delivery position can only be obtained by DELETING the
  name first, never by inspecting the create response; and an independent per-run nonce
  in the metadata is what makes a colliding run's config unequal. Custom metadata is
  also **not sticky** — a create-or-update that omits it wipes every custom key,
  leaving only the server's own `_nats.*` — so every create sends the full map. See
  "Per-run ephemeral consumer identity".

- **A structured create error does not prove nothing was created.** Both the standalone
  and clustered paths validate and can fail BEFORE they look up an existing consumer,
  and clustered creation can establish the consumer assignment and then fail on
  `setLeader`. So no error code partitions "our create landed" from "someone else's
  consumer is at that name", which is why only a CONFIRMED create licenses a cleanup
  delete.

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
| `$MQTT_sess` | per-subject record CAS, `loadLastMsgFor`, filtered scans, exact-sequence confirmed deletes (teardown release, expiry); no purge; **one ephemeral `AckNone` session-record watch consumer per broker**, created under a name derived from the server run and DELETED by name when a failed attempt rolls back what it confirmably created (see "The session-record watch" and "Per-run ephemeral consumer identity"); and, on `$MQTT.accept.>`, **create-only CAS writes of permanent per-CONNECT acceptance facts** committed in ONE atomic batch with the record, plus bounded filtered walks of them. Every record carries `WID`/`PWID`, the write's own identity and its parent's | **exactly two subjects** — `$MQTT.sess.<domainTk>>` and `$MQTT.accept.<domainTk>>` — compared as a SET (order-independent, duplicates refused); **`AllowAtomicPublish` required** (the accept batch carries `Nats-Batch-*` headers, which a stream without it rejects, failing every CONNECT — and the batch's apply-layer indivisibility is what makes the fact and the record one commit; see the atomic-batch contract above); `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; **`DenyDelete` fatal** (the teardown delete is a record's only exit; nobody resumes a marked record, so a denied delete locks the client ID out permanently); no `Sources`; no `SubjectDeleteMarkerTTL`; a **well-formed account recovery epoch** in `Metadata` (validated on the same snapshot; unrelated keys accepted) — this stream MINTS it, so the requirement here is well-formedness rather than equality, and every other broker-owned stream must then carry that exact value; **`MaxConsumers` unlimited** (one watch consumer per live broker, so a finite cap eventually rejects a legitimate broker start — note a configured `0` normalizes to unlimited) | `DenyPurge` (nothing purges); `ConsumerLimits.*` (a stream `InactiveThreshold` below the watch's requested 5 minutes, or any positive `MaxAckPending` against `AckNone`, already fails the consumer create at initialization, naming the consumer — as on `$MQTT_rmsgs`); `AllowMsgTTL` (no `Nats-TTL` header sent) |
| `$MQTT_willq` | create-only CAS enqueue per WillID, state-advance CAS, filtered scans, exact-sequence deletes (confirmed terminal, no-wait scan cleanup); no purge, no consumers | exactly ONE subject (`$MQTT.willq.<domainTk>>`); `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1` (each per-Will subject is a single-record CAS slot); no global limits; no `Sources`; no `MaxMsgSize` cap; **`DenyDelete` fatal** with the accumulation rationale: per-WillID subjects mean deletion is the only removal, so denying it grows the outbox monotonically and every scan re-walks every Will ever fired (`done` precedes the delete, so nothing re-fires); the **account recovery epoch** in `Metadata`, required to match this account's (unrelated keys accepted) — every broker-owned stream carries it, which is what makes the nine ONE recreation unit (F-066) | `DenyPurge`; `MaxConsumers`/`ConsumerLimits`; `AllowMsgTTL`; **`AllowAtomicPublish`** — audited and NOT required here: only the accepting session save emits batch headers, and it writes to `$MQTT_sess`. This row states its own prerequisites rather than deriving them from `$MQTT_sess`, which would now import that stream's two-subject set and its atomic-publish requirement, neither of which applies |
| `$MQTT_teardown` | per-client retirement watermark CAS and reads, one filtered walk by the orphan sweep — **nothing else, ever** | `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; no `Sources`; plus the **account recovery epoch** in `Metadata`, required to match this account's (unrelated keys accepted) — the same prerequisite every broker-owned stream now carries, and this stream's own reason for it is the sharpest: an `mseq` is a session-stream sequence, so watermarks are comparable only within the incarnation they were written under | **`DenyDelete`/`DenyPurge`** and `SubjectDeleteMarkerTTL` (no operation deletes or purges — a fence is permanent by design, so no delete marker can ever be created); `MaxConsumers`/`ConsumerLimits` (no consumers). Both denial waivers are load-bearing here, since reaping a fence would unfence a generation whose resources are already deleted |
| `$MQTT_msgs` | plain stores with per-message TTL; durable, drained and shared queue consumers, each with an explicit `MaxAckPending`; consumer deletes; `loadMsg` by sequence; **no message delete, no purge** | **`InterestPolicy`**; `MaxMsgsPer` unlimited; no global limits; no `MaxMsgSize` cap (a review round proposed a threshold at the broker's max payload, since the PUBLISH admission preflight bounds the complete stored form; disproven — QoS 1/2 Will stores bypass that preflight, so no positive cap is provably inert); **`AllowMsgTTL` required when MQTT 5.0 is accepted** (the default; a `max_protocol_version: 4` broker never emits `Nats-TTL`, so it waives the field); no `Sources`; no `SubjectDeleteMarkerTTL`; `MaxConsumers` unlimited (one durable per subscription/group/drain); `ConsumerLimits.MaxAckPending` fatal when non-zero and **below** the broker's effective cap; the **account recovery epoch** in `Metadata`, required to match this account's (unrelated keys accepted) — every broker-owned stream carries it, which is what makes the nine ONE recreation unit (F-066) | `DenyDelete`/`DenyPurge` (no delete or purge exists); `Discard` (inert with every limit unlimited); **`ConsumerLimits.InactiveThreshold`** (runtime refusal by the drain and shared/affine guards — see below); `Duplicates` (best-effort narrowing only) |
| `$MQTT_rmsgs` | per-topic store with TTL incl. empty-body tombstones (the durable deletion authority); batched `loadLastMsgForMulti` reads; **no explicit message-delete or purge API calls**; one ephemeral `AckNone` consumer per broker, which this owner both CREATES and DELETES by name — the delete is what gives a retried initialization a fresh `DeliverAll` position (see "Per-run ephemeral consumer identity") | `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; **`AllowMsgTTL` required when MQTT 5.0 is accepted** (as on `$MQTT_msgs`); no `SubjectDeleteMarkerTTL`; `MaxConsumers` unlimited (one ephemeral consumer per broker); the **account recovery epoch** in `Metadata`, required to match this account's (unrelated keys accepted) — every broker-owned stream carries it, which is what makes the nine ONE recreation unit (F-066) | **`DenyDelete`** (the owner issues no MESSAGE delete — the tombstone plus `MaxMsgsPer: 1` store-time replacement carry correctness, and both that eviction and TTL expiry are store-internal reaping `DenyDelete` does not gate — and the CONSUMER deletes it does issue are `CONSUMER.DELETE` requests, which `DenyDelete` does not gate either); `DenyPurge`; **`Sources`** (supported cross-account retained topology); `ConsumerLimits.*` (a hostile limit already fails the consumer create at initialization, naming the consumer) |
| `$MQTT_qos2in` | hold/claim CAS at `<idHash>.<gen>.<PI>` (+ `.rel` sibling), exact-sequence deletes, confirmed exact-subject hold purge (discard + orphan sweep), bounded walk | `LimitsPolicy`; `DiscardOld`; `MaxMsgsPer: 1`; no global limits; no `MaxMsgSize` cap; `DenyDelete` fatal; **`DenyPurge` fatal** (blocks the confirmed exact-subject hold purge); no `Sources`; no `SubjectDeleteMarkerTTL`; the **account recovery epoch** in `Metadata`, required to match this account's (unrelated keys accepted) — every broker-owned stream carries it, which is what makes the nine ONE recreation unit (F-066) | `MaxConsumers`/`ConsumerLimits` (no consumers); `AllowMsgTTL`; `Duplicates` |
| `$MQTT_qos2assign` | create-only claim CAS, transition CAS, exact-sequence reap; no purge, no consumers | as `$MQTT_qos2in` but `DenyPurge` waived; the **account recovery epoch** in `Metadata`, required to match this account's (unrelated keys accepted) — every broker-owned stream carries it, which is what makes the nine ONE recreation unit (F-066) | `DenyPurge` (nothing purges by subject); `MaxConsumers`/`ConsumerLimits`; `AllowMsgTTL` |
| `$MQTT_inflight` | create-only register CAS, exact-sequence deletes, bounded enumerate-load-classify reap per retiring incarnation (teardown), bounded walk; no consumers | as `$MQTT_qos2in` but **`DenyPurge` waived since F-034** (`DenyDelete` fatal; the per-incarnation FILTER PURGE was replaced by `reapIncarnationRegisters`, which deletes at exact sequences, so nothing here purges by subject); the **account recovery epoch** in `Metadata`, required to match this account's (unrelated keys accepted) — every broker-owned stream carries it, which is what makes the nine ONE recreation unit (F-066) | `DenyPurge`; `MaxConsumers`/`ConsumerLimits`; `AllowMsgTTL` |
| `$MQTT_out` | append-only stores (one a create-only CAS), per-session PUBREL + affine durables, interest-based drop, and **delete of an exact sequence** (a materialization attempt withdrawing a copy no claim of its incarnation can authorize); **no purge** | **`InterestPolicy`**; `MaxMsgsPer` unlimited; no global limits; no `MaxMsgSize` cap; no `Sources`; no `SubjectDeleteMarkerTTL`; `MaxConsumers` unlimited (two durables per session); **`ConsumerLimits.InactiveThreshold` fatal** (it would reap a disconnected session's affine consumer); `ConsumerLimits.MaxAckPending` fatal when non-zero and below the effective cap; the **account recovery epoch** in `Metadata`, required to match this account's (unrelated keys accepted) — every broker-owned stream carries it, which is what makes the nine ONE recreation unit (F-066) | **`DenyDelete` fatal** (a disowned copy could not be withdrawn and would NAK-loop behind the fail-closed delivery gate for the session's life); `DenyPurge` (nothing purges by subject); `Discard` (inert); `AllowMsgTTL`; `Duplicates` (best-effort narrowing only) |

Three asymmetries in that table are deliberate, and each is pinned by an
acceptance test so an over-strict "fix" fails:

- **`DenyDelete` is fatal on `$MQTT_sess`/`$MQTT_willq` and waived on
  `$MQTT_teardown`/`$MQTT_rmsgs`** — the former have deletion as a record's
  only exit; on the latter the owner issues no explicit MESSAGE delete. That is
  not the same as "nothing is deleted": on `$MQTT_rmsgs` the `MaxMsgsPer: 1`
  store-time eviction IS the replacement semantics, and TTL reaping drops
  expired records — both store-internal, neither gated by `DenyDelete`. Nor is
  it the same as "this owner issues no delete API request": it issues
  `CONSUMER.DELETE` for its own retained observer, and correctness DEPENDS on
  that one (it is what gives a retried initialization a fresh `DeliverAll`
  position). `DenyDelete` gates message deletion only, so no stream setting can
  refuse it.
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

### The nine streams are one recreation unit

**Every broker-owned `$MQTT_*` stream is recreated, reset, backed up and restored
as ONE unit. There is no supported procedure that recreates a proper subset**, and
startup enforces that rather than merely advising it (F-066).

The hazard the rule removes is not an empty stream, it is a stale reference that
matches new unrelated state. Records on each of the nine persist identities the
others issue — sequences, consumer durable names, subjects — so recreating one
strands every surviving reference to it: an assignment claim naming a `$MQTT_msgs`
sequence a recreated `$MQTT_msgs` has since reissued to a different message, a
persisted drain naming a durable on a stream that no longer holds it. Per-subject
CAS cannot help, because the semantic identity is what fails.

Two mechanisms enforce it, and they are deliberately different in kind.

**A COUNT rule, in an account-wide preflight that creates nothing.**
`mqttPreflightBrokerStreamBundle` runs before the provisioning loop, over the same
spec table the loop iterates, and takes a lookup-only interface so it cannot create
what it is deciding about. It must be a preflight rather than a check inside
`ensureBrokerStream`: that helper sees one stream and CREATES it when the lookup
says absent, so by the time any validator ran, the missing member would already be
provisioned. Each lookup's `(info, error)` PAIR is dispositioned together — present
(positive evidence outranks a contradictory error), absent (not-found), or
**undecided**, which fails startup rather than being read as absence. Folding
undecided into absence is the fail-open that matters: a transient lookup outage
would turn an eight-present account into a zero-present one and the loop would
silently reprovision the missing stream. Then: **zero present admits** (initial
creation, or a completed reset), **nine present admits**, and **one through eight
REFUSES**, naming what is present and what is absent.

**An EPOCH stamp, per stream, checked by every validator.** `$MQTT_sess` mints a
recovery epoch into its own stream metadata at creation — the linearization point,
so whichever replica wins the create sets the authoritative value — and every
broker adopts it from `ensureBrokerStream`'s authoritative re-read. The other eight
are stamped with the adopted value before their own create, and every validator
compares it through `validateBundleEpochStamp`. A malformed or empty *expected*
epoch is itself a refusal, never a wildcard. This is the `$MQTT_teardown`
stamp-and-refuse mechanism generalised: that stream no longer carries a private
check, because two mechanisms stating one fact is a redundancy, not a belt.

The two catch different things and neither subsumes the other, and it is worth
being exact about what each one can and cannot see.

- **The count rule catches a member that is ABSENT when a broker starts** — a
  deletion nobody has replaced, or a first provisioning that stopped part-way.
  What it buys is that the broker does not silently recreate it, which is the
  actual defect: before this, `ensureBrokerStream` created the missing stream and
  validated it against its own owner's list alone.
- **The epoch catches a member that is PRESENT but from another incarnation** —
  a partial restore, or a replacement created without the account's stamp.

Neither catches an operator who deletes a member, recreates it by hand, AND
copies the surviving epoch onto the replacement: the count sees nine and the
epoch sees agreement. That is the forgery case, outside what this enforcement
claims (see "What the enforcement does not buy"), and doing it reinstates exactly
the stale-identity hazard the epoch exists to detect.

**What the preflight does NOT promise, because it is nine independent lookups
rather than an atomic snapshot.** Under create-only concurrency — every other actor
a broker running this same ordered provisioning — an admission cannot wrongfully
succeed: an all-absent observation may race a peer's creation but then adopts that
peer's epoch through create-or-lose, and an all-present observation is truthful
because presence is monotone when nothing deletes. Concurrent DELETION breaks the
second arm, and `docs/CONFORMANCE.md` records the interleaving; it requires
violating the all-brokers-stopped precondition the recovery procedure states.

**The recovery, for every nonconforming or partial state alike:** stop ALL brokers
for the account, delete all nine streams, and restart, which provisions a fresh
bundle under a fresh epoch. Recovery is therefore a complete account-level state
reset with **no partial recovery**, and every startup refusal says so. An operator
running a supported administrative variation — a `Sources` topology on
`$MQTT_rmsgs` — reapplies it as part of the reset; nothing here reconstructs it.

A tenth stream, `$MQTT_origin`, is not a member: it is retired state, refused by its
own lookup-only preflight with delete guidance, for the same reason the bundle
preflight is a preflight at all.

Resetting the bundle costs things that are not recoverable, and they are worth
stating separately because an operator may reach for the reset casually: the
permanent CONNECT **acceptance facts live on `$MQTT_sess`**, so a reset discards
every client's acceptance history, and the accepted messages, retained messages and
pending delayed Wills go with the streams that hold them.

**It does not, however, make a pending delayed Will FIRE** — a consequence that WAS
true of recreating `$MQTT_sess` alone and is not true of the reset, because the reset
takes `$MQTT_willq` as well, so the Will is discarded along with the evidence that
would have suppressed it. Firing requires `$MQTT_sess` lost while `$MQTT_willq`
survives: a partial operation, which is now refused, and which an administrative
deletion or a partial restore can still produce.

Where the facts live is a deliberate consequence of putting them with the record —
the atomicity that removes the split-acceptance interval is only available within one
stream — and there is no honest mitigation short of a second write nobody could make
atomic with the first.

**What the enforcement does not buy.** The hazard becomes unreachable through a
SUPPORTED operation, not eliminated: every stale-identity path still exists in the
code. Startup validation and the preflight are both snapshots, so a live operator
deleting a stream behind initialized managers still reaches those paths, and the
all-brokers-stopped precondition stays load-bearing. Accepted-data loss when
resetting a bundle holding accepted data is unchanged. Completeness is demonstrable
at this HEAD and GUARDED rather than proven: the nine-entry provisioning table is
hand-maintained, and what guards it is that the preflight derives its names from
that table, that `TestBrokerStreamSpecTable` pins the table, and that
`TestStreamValidatorFieldCoverage` requires every validator to reject a foreign
stamp — a validator that forgot the check fails that bijection.

### What a persisted record references that another stream issues

Which durable identities cross a stream boundary is enumerated in
`docs/findings/persisted-identity-census.md`. A new FIELD of a persisted artifact fails by
name, over a type universe derived by a recursive walk from the persisted roots. The
INTERPRETATION-SITE half is a drift guard rather than a closure proof: its root set is
hand-maintained and it keys a site by unqualified function name, so a new branch inside an
already-named function, a new wrapper around the transport primitives, or a same-named
method on another receiver can still pass. That is why the census could not discharge
F-066's completeness deferral — and why F-066 was closed at stream granularity, where
completeness is demonstrable, instead.

Two facts from it belong here, because they bear directly on the table above.

**No validator asserts a single-stream recovery procedure any more.** They used to:
seven of the nine named themselves alone, and that was operator guidance rather than
proof, because records on those seven persist identities other streams issue —
sequences, consumer durable names, subjects — so recreating one of them strands every
surviving reference to it. F-066 owned that hazard and closed it by **WITHDRAWING the
single-stream procedure rather than by generalising the coupling**: all nine
broker-owned streams are now one recreation unit, enforced as described above, and
every validator's recovery text names the bundle. What the census adds is that the
population is now enumerated rather than sampled — which is why the enforcement did
not need it: the enumerable thing is the STREAMS, one provisioning table with one
loop over it, not the identities four separate attempts failed to enumerate.

**Scope over that population is anchored to the bridge's INTERPRETATION SURFACE**, not to
a list of JetStream transitions. A transition is in scope if and only if it can change the
outcome of an enumerated interpretation site — 161 of them, derived from the source.
Delivery, timer-driven redelivery, pause, expiry and snapshot restoration therefore enter
through the reads and callbacks that observe them, rather than as classes of their own,
and a new row is required only for a new site, observable, outcome partition or authority.
That is what makes the enumeration bounded at all: "every durable-state transition" is
not, because it enumerates the server's implementation. It is a better-founded scope rule,
**not a proof of closure** — see the three drift classes above.

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

  That indivisibility is measured, not assumed. The apply-layer contract it rests on is
  stated under "JetStream behaviors this design depends on"; the accepted CONNECT's own
  use of it is asserted at the store boundary by
  `TestMQTTAcceptBatchReplicatedIndivisible`, over a real R3 `$MQTT_sess`, and killed by
  a mutation that splits the batch into two independent conditional writes and
  suppresses the second. The assertion is made at that boundary rather than later
  because it is a claim about **this write**: the record subject is a `MaxMsgsPer: 1`
  register, and a repair save, a lifetime grant or a SUBSCRIBE legitimately replaces the
  record the batch committed while the fact — which is permanent — stays. So
  `mqttAcceptFactLanded` means "the batch committed and the record committed with it",
  never "the record the fact was committed beside is still the one on the subject".

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
  a bare sequence is uncomparable and a bare epoch says nothing. `$MQTT_willq` USED TO
  sit outside the coupled recreation unit, so a pending Will survived a `$MQTT_sess`
  recreation carrying a floor recorded against a stream that no longer existed, while
  the new one restarted at sequence 1 — reachable through an operation the documentation
  then supported. F-066 closed that route: all nine streams are one recreation unit, so
  no supported operation replaces `$MQTT_sess` while `$MQTT_willq` survives. The scoping
  stays, and is now defence in depth against administrative deletion or a partial
  restore, which are unsupported rather than impossible.

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
Two former classes — *cross-term claimant divergence* and *terminal regression* — both
turned on two bridge-authored conditional writes taken from one base across a leader
transition. **Both are retired on a conforming `$MQTT_willq`** (F-030, 2026-08-16):
leader-local in-flight ownership, election readiness, and the serving leader's
applied-state check close every route. An unconditional source added after startup is
the separate live-configuration-drift residual; startup validation is a snapshot.
`docs/CONFORMANCE.md` records both boundaries.

The reconnect-suppression miss is closed WITHIN a `$MQTT_willq` leader term by the
permanent facts and the persisted decision. Whether the terminal state that ends the
protocol — `done` — is durable against the write that would take it back is what F-030
closed: the supported server refuses that write by every route, which makes executing a
persisted decision on recovery safe.

This is a first-release storage format: pre-Layer-2 in-record Wills, outbox records
without an exact client identity or a legal decision shape, and a `$MQTT_sess` stream
without a valid generated epoch are
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
deleting that message from `$MQTT_willq` — recreating the stream is NOT an
alternative, since the nine broker-owned streams are one recreation unit and the only
other route is a whole-bundle reset. A refused record
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

- **Back up, restore, delete and recreate ALL NINE broker-owned streams as one unit,
  from one point in time.** There is no supported single-stream or partial procedure:
  startup refuses an account holding one to eight of the nine, and refuses a complete
  bundle whose members carry different recovery epochs (see "The nine streams are one
  recreation unit"). A reset discards every client's permanent acceptance history with
  `$MQTT_sess`, every open generation-retirement fence with `$MQTT_teardown`, and every
  pending delayed Will with `$MQTT_willq`. Note what that means for the Will: losing the
  acceptance facts does NOT make a pending Will fire, because the reset takes the Will
  too. The fire outcome needs `$MQTT_sess` lost while `$MQTT_willq` survives, which is a
  partial operation this enforcement refuses.
  A common epoch detects a bundle assembled across incarnations; it does NOT prove nine
  independently restored snapshots came from one coherent instant, so restore skew
  within one incarnation is undetectable and the "one point in time" is the operator's
  to guarantee.
- **A reset requires restarting every bridge serving the account before MQTT traffic
  resumes.** The recovery epoch is captured once per account manager
  (`as.sessStreamEpoch`); a live manager that outlives the reset retains the stale
  epoch and with it stale session-CAS state. A process restart recreates the manager
  and adopts the new epoch. The all-brokers-stopped precondition is load-bearing for
  the enforcement too: the preflight is a snapshot, so an operator deleting streams
  behind a running broker can still reach the stale-identity paths.
- **A first start interrupted part-way leaves a partial bundle, and the recovery is the
  same reset**: stop every broker, delete whichever of the nine exist, restart. This is
  also what a concurrent first start can produce — two brokers starting together
  against an unprovisioned account can leave one refusing, and it converges on a
  restart once some peer has completed all nine.
- **The reset recreates streams with broker defaults.** A supported administrative
  variation — a `Sources` topology on `$MQTT_rmsgs` — is reapplied by the operator
  afterwards; nothing here reconstructs it.
- **`$MQTT_origin` must be DELETED, not kept.** The session-interval register it held is
  no longer defined, and startup refuses an account that still has the stream rather than
  leaving it to accumulate.
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

The session record is the sole arbitration. Two BRIDGE-AUTHORED CONDITIONAL writes taken
from one base cannot both apply — see the same-base boundary in `docs/CONFORMANCE.md`,
which is authoritative and which every statement in this section is scoped by. What no
CAS can exclude is an **unconditional** write, which carries no expectation to compare;
that route stays open and is why the lineage machinery below exists. What the design
delivers either way is that every interleaving converges on one owner and each client is
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
had no consequence beyond the record. F-029b gave it one — the next create into the
emptied subject inherited the deleted record's identity, so a delete that recorded no
retirement left two records in DIFFERENT intervals carrying the same parent, a FALSE fork
and the one direction that suppresses a delayed Will's cancellation. **The write-lineage
audit deleted that inheritance**, precisely because every route to that verdict was a
false fork, so the lineage consequence is gone. The reversal STANDS on its remaining
grounds, which never depended on it: a bare delete records no retirement, leaves no fence
for any later reader, and destroys a persistent session's record on a rejected resume that
merely REQUESTED expiry 0. Four consequences, each load-bearing:

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
  claim does NOT bound is an unconditional foreign write regressing the single slot: this
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
first-creates racing across a leader term apart from two unrelated ones — **remains open,
deliberately.** F-029b closed it by having a create inherit the retired record's identity;
the write-lineage audit deleted that inheritance and every other one, because each route
by which such a verdict was reached was equally a FALSE fork. Every create is parentless
again, the pair CONCEDES, and that is residual R-create in `docs/CONFORMANCE.md` —
what conceding says being `0x89` ownership-unsettled since F-084, not `0x8E` —
the principal safety cost of this design, not a gap awaiting a fix. See "Recognising a
forked session record". The other — the window in which a teardown that WON its claim has
already deleted the consumers a surviving foreign record still names — is closed by
the generation-retirement watermark, which is durable state off the record subject and
therefore outside this design's reach (see "The generation-retirement watermark").

### Per-run ephemeral consumer identity

The broker owns exactly two ephemeral push consumers per account: the **retained
observer** on `$MQTT_rmsgs` and the **session-record watch** on `$MQTT_sess`. Both are
named from the server run rather than freshly per attempt, and the two are provisioned
by deliberately DIFFERENT protocols.

**Two tokens, drawn independently.** `ephemeralID` NAMES both consumers and both
delivery subjects (`$MQTT_rmsgs_obs_<id>` on `$MQTT.sub.<id>.rmsgs`;
`$MQTT_sess_watch_<id>` on `$MQTT.sub.<id>.sessw`). `ephemeralOwner` is a SECOND,
independent draw carried in consumer metadata. Both are per **`Server`**, minted
lazily, never the node name — which the caller of `NewServer` supplies and two brokers
can share. The nonce is independent because under `ActionCreate` the server compares
the submitted config with the existing one, and every other field a colliding run would
submit derives from the colliding token; the nonce is the only field left to differ, so
it is what turns a name collision into a refusal instead of a silent adoption. It
protects **create time only**: it cannot condition the observer's unconditional delete,
and it cannot discriminate a byte-identical copy of our config, which needs this
account's credentials and is out of scope. Name safety rests on the run's own nuid
draw, as the per-attempt delivery subjects always did.

**The retained observer deletes before it creates.** A repeated named create ADOPTS the
existing consumer at its current position and replays nothing (see "JetStream behaviors
this design depends on"), and the retained index is written by nothing but this
consumer's callback and the publishing broker's local seed — a WILDCARD subscription
has nothing else to consult. So a retry that adopted an observer already drained by a
discarded manager would serve no retained message for the life of the process,
silently. The observer therefore establishes ABSENCE first: delete the derived name and
require a DEFINITE outcome (success, or consumer-not-found, which is success too), with
one re-attempt, and on a second ambiguity fail the initialization closed rather than
create over a consumer whose position is unknown. Freshness comes from the protocol
order, never from inspecting the create response, which cannot distinguish a fresh
consumer that has already delivered from an adopted one.

**The watch creates without deleting.** It is `DeliverNew`, so it has no historical
position to lose by adopting its own consumer, and its two gaps are both covered: an
observation missed while no manager exists concerns no resident session (residency
requires a manager), and one missed while the manager is live is recovered by
`reconcileResidentOwnership`. Adding a destructive window would buy nothing.

**What a rollback may delete: a CONFIRMED create, and nothing else.** No error can
distinguish "our create landed" from "a foreign consumer now occupies that name",
because both the standalone and clustered paths return structured errors BEFORE they
look the existing consumer up — so deleting on the strength of an error could remove a
live peer's consumer. A create whose RESPONSE was lost is therefore left alone; the
next attempt of the same run reconciles it (the observer deletes it definitely, the
watch adopts it as config-identical), and failing that `InactiveThreshold` does. The
rollback is BEST-EFFORT and is not part of the cardinality guarantee: what it buys is
promptness for the attempt no retry follows — `Start`'s own, after which the process
exits. That "no retry follows" is the common case rather than a guarantee, because a
CONNECT can be queued on the manager mutex while the eager initialization fails;
`getOrCreateMQTTAccountSessionManager` refuses to create a manager once the server is
marked not-running, and what remains is a stated residual.

**What this guarantees, and what it does not.** Where an initialization attempt is
repeated within one server run, cardinality stays at AT MOST ONE consumer per (server
run, account, purpose) whatever the attempt count. It is not a claim about strays in
general: see `docs/CONFORMANCE.md` for the five residual statements, including that a
consumer stranded by a crash, a replacement, a lost rollback delete, an unreconciled
ambiguous create, or the normal shutdown of a successfully started broker remains
`InactiveThreshold`'s business, and that across rapid restarts cardinality is bounded by
restart rate × inactivity window rather than by one consumer per purpose globally.

**Consumer metadata** (`mqtt_nats_bridge_owner`, `_purpose`, `_run`, `_schema`) is sent
on every create, because a create-or-update that omits custom keys WIPES them. Its only
machine reader is the server's own equality check; no broker code branches on it, and
an operator or a future sweep is the human reader. A `_schema` mismatch REFUSES, so an
unsupported shape is never adopted or rewritten.

### The session-record watch

Ownership of a session changes only by a write to its record subject on `$MQTT_sess`,
so **observing that stream is observing every ownership change** — and a session
nobody writes about needs no revalidation at all. What one observation establishes is
that the record MOVED, and so is no longer ours; it identifies no successor, and no
route downstream of it may claim one (see "Only a held successor licenses a takeover
claim"). That is what lets convergence cost
nothing per quiet session, which the rejected alternatives could not: a periodic READ
per resident session proves nothing when it finds the record unmoved (10164 is
returned while a write is in process upstream), and a periodic CAS would be a write
per session per interval.

What this replaced was an at-most-once core-NATS notification on the JetStream reply
subject of every other broker's session store (`$MQTT.JSA.*.SP.*.*`). Lost meant lost:
a never-expiring session — **every** MQTT 3.1.1 persistent session — has no expiry
timer, the expiry sweep skips it, and nothing else re-validated a resident object, so
a displaced client kept being served against a session whose record had moved, which is
the split-brain [MQTT-3.1.4-3] forbids.

**The watch is not a durable channel either, and the design does not claim it is.** It
is one ephemeral push consumer per broker per account (`AckNone`, `ReplayInstant`,
`InactiveThreshold` 5 minutes, delivered to a `$MQTT.sub.<ephemeralID>.sessw` subject
on the **service** connection so the recommended client ACL still holds), created only
once EVERY stream in the provisioning table has been created and validated.

That ordering is a leak fix, not a preference, and so is the consumer's **name**. Both
per-broker ephemeral consumers — this one and the retained observer — are named from
the server run's own identity rather than freshly minted per attempt, because
initialization really is retried: `startMQTT` opens the listener before `Start`
initializes the manager, so a CONNECT arriving in that window drives initialization
itself, and a failed attempt is retried by the next CONNECT or by `Start`'s own call
until one succeeds or `Start`'s failure tears the server down. A per-attempt name left
a consumer that no later attempt could compute again — reapable by nothing, bounded in
lifetime by `InactiveThreshold` and in cardinality by nothing. A derived name means the
next attempt asks for the SAME consumer. See "Per-run ephemeral consumer identity".
The pair is still provisioned in order (`mqttProvisionEphemeralConsumers`): the watch
is created last, and a failure there rolls back what this attempt confirmably created.
Its delivery state is not
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
  outcome was **0x8E** when this was written; **F-084 made it `0x89`
  ownership-unsettled**, because record absence is an ownership fact and not a successor
  fact. `docs/CONFORMANCE.md` records what that leaves.

**Observing our own writes is new, and one shape of it is dangerous.** The old channel
never delivered this broker's own stores; a stream watch delivers everything. A save
that LANDED with its response lost arms `seqUncertain` and leaves the acknowledged
lineage pair stale, so the delivered record is strictly past `sess.seq` and lineage
would read our own bytes as unrelated — evicting the rightful client over
its own write. Two layers answer it: before the write settles, the writer's open
record transaction refuses the worker's fence; after it arms, the worker's **digest
arm** (`evictOwnLanding`) recognises the record as the attempt's own bytes, using the
resolver's identity test verbatim, and defers to the resolver without deciding
anything. The same refusal covers the case with NO bytes to compare: while an attempt
is unresolved, an unusable load (absent, unreadable, undecodable) may not concede at
all, because the trigger can be that very write — the backstop
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
so the two cannot drift) classifies WHAT is there — never by whom, which no record can say
(F-084): ours if the original landed in the window, not-ours
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
foreign FAIL-SAFE rather than proven, because an unconditional foreign write carries no
expectation for any check to compare and so is excluded by nothing (the same-base
boundary in `docs/CONFORMANCE.md`).

The pass is single-shot: the periodic pass is the retry cadence, and a retry loop would
hold the record fence across an unbounded number of round trips. It costs at most three
JetStream requests (read, re-attempt, classifying re-read) of record-fence occupation,
plus whatever the landed continuations do — which is the pre-existing landed-arm cost made
more frequent, not a new hold class.

**The invariant does not rest on "only one of them commits", and that reasoning is now
RETIRED rather than load-bearing.** For two bridge-authored conditional writes the shared
CAS base makes the second impossible — the same-base boundary in `docs/CONFORMANCE.md`.
It is NOT transferable to the unconditional foreign write that remains reachable: that
route's reachable order is the re-attempt admitted first and the foreign body landing
afterwards, which supplies no acknowledgement for this broker to adopt, so the
surviving-sequence argument below does not describe it. Kept only as a record of the
retired reasoning and as generic defensive depth. Historically it ran: a leader
transition clears the in-process
map while a replicated proposal survives it, and a clustered apply never re-checks — so
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
before the enqueue is a check-then-act and an acknowledgment appended behind the
displacement's DISCONNECT would violate
[MQTT-3.14.4-1]. The tail also samples ownership (`sess.c == c` under
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

The two shapes are genuinely different events. A write that is **not** conditioned on the
subject at all — carrying no `Nats-Expected-Last-Subject-Sequence`, so no check compares
it — can land on our base and discard our record under `MaxMsgsPer: 1`. The survivor then
CLAIMS our own parent rather than DEMONSTRATING succession. That is all the record
establishes: it does not settle whether a takeover occurred, and a live competing owner
adopting such a write produces this same shape
(`TestMQTTForkedRecordE2EWithALiveCompetingOwnerPublishesTheWill`). What the verdict
withholds is the ASSERTION of a takeover, not the possibility of one. Two brokers can
reach the takeover conclusion about each other from that one event.

This used to be described as a cross-term race between two conditional writes. That route
is closed — see the same-base boundary in `docs/CONFORMANCE.md` — and the recognition is
retained for the unconditional one, which a CAS cannot exclude by construction.

A sequence cannot distinguish them, and neither can a counter: a per-write epoch is a
branch-local Lamport counter, and a competing branch that never read our record can advance
above us and then look like our successor. What distinguishes them is **ancestry**, so
every `$MQTT_sess` record carries a parent pointer — `WID`, the write's own identity, and
`PWID`, the identity of the record it was derived from. Two records carrying the same
`PWID` CLAIM one base. "Sibling" is shorthand for that claim throughout, never a proof
of descent: restore adopts `WID`/`PWID` without validating provenance, and every
conclusion drawn is negative — such a body does not establish a takeover.

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
identities are globally unique, so a parent match names one actual record whatever
generation wrote it — a CLAIM of descent from it, never a proof, since restore adopts
the pair unvalidated; a generation mismatch adds nothing to that and only suppressed it.

**A CREATE IS PARENTLESS**, and that is the write-lineage audit's result rather than an
omission. `parentForNextWriteLocked()` returns the record we are acknowledged for and
nothing else, so a session holding none writes `{c, ""}`.

The chain used to cross the teardown re-mint. `clear()` zeroes `wid`/`pwid` — the session
is acknowledged for nothing once its record is deleted, and six sites read `wid` as
exactly that — and it also recorded the WID of the record its claim held in a third
field, `retiredBaseWID`, which the accessor fell back to. Three further sources fed the
same field or stood beside it: the generation-retirement watermark's `RWID` (a field since
removed outright, see "The generation-retirement watermark"), an adopted marker's own
`PWID`, and the `$MQTT_origin` interval register's identity. Together they
made the sibling of base `a` written after tearing `a` down `{c, a}` rather than `{c, ""}`,
so a resume child `{b, a}` recognised it, and they made a **cross-term create pair**
recognisable from either side.

**All four were deleted, with the register stream and the field.** The audit classified
every route by which such a verdict could be reached and found each one documented in
`docs/CONFORMANCE.md` as a FALSE fork — a replayed or regressed register (R0c), a
create's watermark read separated from its admission by a complete remote lifecycle (R1),
a regressed `$MQTT_teardown` slot (R2), a delayed same-base proposal outliving an interval
(R3), and an out-of-band deletion of a live record (R4). Each needs a create that
INHERITED an identity which outlived the interval it named, and each fires the same
verdict a true fork would. Removing the inheritance removes all five; nothing else in the
recognition changes.

What that concedes is the PARENTLESS CREATE PAIR: two records both carrying no parent, so
arm 2's `ackPWID != ""` conjunct concedes from either orientation and the erased side
cannot recognise the pair as siblings. **Since F-084 the erased side answers `0x89`
ownership-unsettled** rather than asserting a takeover nothing arbitrated: the
unrecognisability is still the residual, but the claim it produces is now truthful.

Its stated route used to be a cross-term one — both brokers find the subject empty,
both are admitted at base 0, both apply. **That route is closed**: a create is
conditional too (base 0 means *the subject must currently be empty*), so it is covered
by the same-base boundary in `docs/CONFORMANCE.md` like every other bridge-authored
conditional write. The
CONCESSION is unchanged, because the shape is still reachable without it — an
unconditional foreign write, or an out-of-band deletion followed by a create, produces a
parentless record beside ours just as well. That is the pre-F-029b behavior, restated
as a residual in
`docs/CONFORMANCE.md` and pinned end to end by
`TestMQTTCreateForkE2EAnswersUnsettledToTheDisplacedClient`. It is a residual conditioned
on EVIDENCE, not on a rollout: it stands unless a route is found that produces the verdict
without also producing the false ones.

Nothing was lost on the repair side, because a create-parented fork was **repaired by
nothing** already: a create had a parent POINTER and no parent CONTENT, so
`buildForkedGrantLocked` returned nil and no capsule was minted.

`RWID` survived this deletion for a time, with no reader, and has since been **REMOVED
outright** — see "The generation-retirement watermark" for the reasoning and for what an
exact reader would need instead. Two candidates were weighed and both failed: F-036's
2026-08-27 attempt disproved the predicate because the watermark holds one slot per client,
so requiring `ackWID == rwid` recreates the superseded-watermark false negative that attempt
existed to close (the licence it found is `MarkerSeq`-only); and `docs/ROADMAP.md`'s
vanished-record bullet, which decided last and decided against, because a write identity
names a record's bytes rather than one occurrence of them.

**`$MQTT_origin` is REFUSED, not merely unused.** An account still holding the register is
an unsupported persisted shape under "Compatibility boundary", so a lookup-only preflight
runs before the provisioning loop and fails startup with delete guidance
(`mqttRefuseRetiredOriginStream`). It cannot be a provisioning-table row, because
`ensureBrokerStream` CREATES an absent stream before it validates one. Absent proceeds;
present is refused; any other lookup failure fails startup rather than being folded into
absence.

Sibling-hood is defined against the **acknowledged** pair, never the in-flight attempt:
with acknowledged `{a,p}` and attempt `{c,a}`, survivor `{b,p}` is the fork, `{d,a}` is
child-shaped, and `{c,a}` is our own late landing (recognised by digest before lineage is
reached). Child-shaped is a lineage verdict and nothing more: it says our record was read
before that write, not that its author holds the session now.

All four conjuncts of arm 2 are load-bearing, and the last one is the least obvious:
`storeFramedSessionMsg` replays our acknowledged bytes verbatim and a clustered
duplicate proposal can land them at a higher sequence, so without `found.WID != ackWID`
that replay is reported as a fork of itself.

**The lineage never decides that a write landed.** `mqttAttemptOurs` and
`mqttRepairSaveLanded` credit void-register verdicts and run landed continuations, and an
identity match is not proof that an attempt's *bytes* are durable — the digest comparison
remains the only thing that licenses them.

**Five classified paths** consult it, and every one of them keeps the disposition it
already had, changing only what is reported:

| Path | On a body claiming our parent |
|---|---|
| definite rejection, **bound** (`sess_rejected_write.go`) | `mqttBoundUnsettled` → **0x89** and the ORDINARY close bookkeeping. Deliberately not the foreign arm's shape — but the difference is the DISPOSAL, not the Will: since F-084 that arm consumes the Will too, suppressing one on a takeover nothing demonstrated having been the defect on either arm |
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
CAS off the same base — only that the survivor is not demonstrably a successor of
ours. An absent `WID`
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
  those arms concede the UNSETTLED vocabulary and decide at the observation level
  (evict only if `target > sess.seq`). They used to concede the takeover one, which was
  the strongest claim available on the weakest evidence there is; F-084 corrected it. A
  record older than the write that licensed the eviction is never classified — classifying
  it could answer *forked* for a write it cannot speak for, and mint a grant with it.
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
  the unadopted attempt concedes as UNSETTLED (the one-hop recognition miss — failing to
  recognise sibling-hood leaves the broker knowing LESS, and less knowledge cannot license
  a stronger claim), a sibling of the acknowledged record still classifies
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

The worker cancels no delayed-Will outbox record, on any verdict. It used to run a
generation-and-origin-scoped cancel on the takeover verdict; that was removed when the
displaced incarnation's Will became the claimant's business, decided when it becomes due
against the permanent acceptance facts. What the worker does owe is the displaced
connection's OWN Will, which F-084 added: before the notification and the close, the
eviction tail discharges a parked action or hands a live Will to ordinary handling. Records
already pending stay pending, and their fate at `FireAt` belongs to the fire-time fences.

### Repairing a recognised fork

Recognition earns the FORK vocabulary and the repair grant that goes with it — stopping a
false takeover claim is no longer what it buys, since F-084 withdrew that claim from every
record-derived route. It does not give the client back the
subscriptions it was SUBACKed for, nor the UNSUBSCRIBEs it was UNSUBACKed for. MQTT
requires the Session State to carry the client's subscriptions ([MQTT-4.1.0-1], Spec5
[4.1.1]), so that loss is a conformance break. `sess_fork_repair.go` closes the
recoverable subset of it, and everything below is memory-only: no persisted field, no
encoding change, no new stream or subject.

**What is recoverable is the record DELTA**, as a well-defined reconciliation against
the RETAINED BODY — deliberately not as a three-way merge over a proven common ancestor.
The anchor condition (`found.PWID == ackPWID`) means the survivor CLAIMS the parent our
erased record claimed, and a claim is all it is: restore adopts `WID`/`PWID` without
validating provenance. The baseline is concrete because this broker actually HELD that
body, so the computation is deterministic without trusting the survivor's pointer; it
establishes neither that the survivor descended from the baseline nor that the baseline
is a genuine common ancestor, and it needs neither — what is re-asserted is state this
broker's own client was acknowledged for.

**A union is wrong, and this is the trap the design exists to avoid.** The verdict
proves that the survivor CLAIMS our parent — explicitly *not* that it erased us. With
base `P = {a}`, our child `X = {a,b}` and a survivor `Y = {}` carrying an **acknowledged
UNSUBSCRIBE of `a`**, a union yields `{a,b}` and resurrects a subscription the client
was UNSUBACKed for. "Absent from the survivor" is ambiguous between "our addition was
erased" and "the survivor removed a base field"; only the parent's content disambiguates
it.

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
conflict invisible and invert survivor-wins: filters the survivor RECORDED would read
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
parent, so nothing about it is repairable. `clear()` drops both, alongside the lineage
itself: nothing of the retired incarnation may be RE-ASSERTED into the fresh one, and
since the write-lineage audit nothing of it is RECOGNISED either — the reused object's
first write is a parentless create. **The invariant is not "`grantBodyParent` always
names `pwid`"; it is "a nil body means nothing is repairable".**

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
`SharedSubs` is excluded. **The ordering that forced that exclusion is gone**: F-050
moved subscription restoration before the success CONNACK, so a per-filter failure
now refuses the CONNECT instead of leaving a durable membership with no join and no
consumer. Widening the repair set to shared membership is F-027's own work and has
its own roadmap bullet; until it lands the exclusion stands as a choice rather than
a consequence.

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
nothing was established (`Unresolved`), and a CHILD-SHAPED record descends from ours by
its own claim, so our change is in the ancestry it asserts. `evictDecideOnce` holds
`as.mu` for its whole
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
  deliberately ignores the generation. It no longer has the clean-start sibling to catch —
  that write is parentless since the write-lineage audit, so the pair concedes before it
  reaches here — but the conjunct is what stops any future lineage match across a re-mint
  from re-asserting state into an incarnation a teardown destroyed.
- otherwise → **drop**. Any write since the verdict makes the current record a CHILD of
  the survivor.

The anchor does **not** arbitrate ownership of the re-asserted state. Only the accept
save's expected-sequence CAS does that, and a CAS cannot exclude an UNCONDITIONAL
foreign write at all (the same-base boundary in `docs/CONFORMANCE.md`) — which is the
route this repair exists for. Note also that this consult has NO sequence gate, unlike
the other call sites: it compares the in-memory pair after its WID and generation gates.

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

**The apply rule has two halves from different sides.** This is the MERGE, so the
survivor has already been compared and the candidate has already survived that comparison
— the mint site never read it. The GRANT becomes ours — membership, the options, and each
sid's drain presence — because that is the acknowledged decision the survivor was found
not to have decided differently. The CONSUMER IDENTITY stays the survivor's wherever the
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
object), and a **bound** one keeps serving a client against a session whose record this
broker may no longer write — which [MQTT-3.1.4-3] requires to be closed.

**What settles it is a write, not a read.** JetStream returns 10164 while a write for the
subject is *in process* upstream, so a load that finds the record still at our base
sequence proves nothing — the contender's proposal can commit a moment later. The
settlement is therefore a **probe**: re-attempt the save of CURRENT memory so a fresh CAS
arbitrates ownership, and let the readback distinguish only contention from a record that
is not ours — not from an OWNER, which nothing on this path identifies. The probe
re-marshals memory rather than replaying the rejected bytes, because a
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
*Foreign* → the Will is **consumed normally**, the session state is
disposed under the gate, and then — off the gate, on the readLoop — the
notification is ATTEMPTED through the shared tail (suppressed if this
connection never saw its CONNACK, or if another displacement path already claimed its one
DISCONNECT) and the connection closed synchronously before `mqttParse` returns, which is the
graceful DISCONNECT branch's own shape: no re-entry, no second DISCONNECT, and no socket I/O
under the gate. *No verdict with the gate held* — contended, unreadable, probe ambiguous,
suspended by an unresolved write — establishes nothing and closes the connection with
`errMQTTSessionOwnershipUnsettled`, a parse error mapped to **0x89 "server busy"** — the
same code the *Foreign* verdict now reports, because announcing a takeover nobody proved
would tell a client to stop
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
  side observes the displacement first while holding that lock — and applied **only** where
  a successor is held in hand; every record-derived displacement consumes instead. See
  "Only a held successor licenses a takeover claim".

Every displacement notification shares one tail, `mqttNotifyDisplaced`, which is also the
only home of the [MQTT-3.2.0-1] CONNACK gate for them: a connection displaced before its own
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

| writer | readable shape | when |
|---|---|---|
| the releasing PUBREL | `releasing` + the exchange generation, and **no fence** | before the onward store |
| a session discard | `discarded` + a **mandatory usable `Fence`**, and no generation | before it destroys the hold |

Both pairings are enforced, not conventional: `mqttQoS2RelClaimIdentityOK` refuses a
`releasing` claim carrying a fence and a `discarded` marker whose fence is absent or
unusable, and a refused shape classifies **unreadable** rather than being read leniently.
A usable fence is the same predicate the purge accepts, so a marker can never carry a bound
its own purge would reject (F-062).

Exactly one create can succeed while a record occupies the claim subject, so for the
exchange a discard **enumerated**, exactly one of "release it" and "discard it" wins and
the loser does nothing irreversible. The claim is **discard-exclusive, not
releaser-exclusive**: the generation identifies the exchange rather than the broker,
so a second releaser of the same exchange reads its own generation back and
proceeds.

Two limits belong with that sentence. The enumeration is a **one-shot snapshot**, so a
hold on a subject it never saw is never competed for at all — still an open blocker
(F-036). And the claim subject is **reused by successive exchanges**, so exclusivity is
per-occupancy rather than per identifier for all time: a `discarded` marker outlives its
own exchange, and the next one on that identifier reads it. That second limit is resolved
rather than open (F-062), and not by making the claim exchange-exclusive — it is not, and
must not be described as though it were. Instead the marker **records the fence its writer
was entitled to destroy**, so a later exchange whose hold sits above that fence proves the
marker is not about it, reaps it at its exact sequence and retries. The purge that
accompanies the marker carries the same bound: it names a subject, but destroys only up to
the sequence its caller could prove. Neither limit was ever a JetStream limitation — the
purge simply supplied no condition, and now it does.

Because the claim is a sibling record, the hold record is never
rewritten — its payload, properties and store time (which ages the Message Expiry
Interval) stay as the PUBLISH left them — and a discard can enumerate a session's
inbound QoS 2 state through `STREAM.INFO` without reading a single payload.

The claim is paired with an authoritative **re-read** of the hold record, and
neither replaces the other: the claim fences a *discard*, which a re-read cannot,
because read-then-fan-out is check-then-act; the re-read fences an
*already-completed* exchange, which the claim cannot, because a peer that committed
and reaped leaves nothing to contend with, so a stalled releaser's create-only claim
would simply succeed. A discard that finds a `releasing` claim waits briefly; if the
subject frees it **competes for the claim again** rather than reading the emptiness as its
own licence — the claim it waited on may have belonged to an earlier exchange and been
reaped by the current one's releaser, which frees the subject without anything having
terminated — and if the budget expires it leaves both records rather than destroying a
payload a crashed releaser's retry still needs; on the same broker an in-process fence
refuses to admit a release at all. A `discarded` marker is never reaped by the DISCARD
itself — it is the only durable proof that a late releaser must lose — and one that
legitimately fences an exchange is left to be aged out. A RELEASER may reap one, under a
single condition it can prove: when its own hold sits above the fence the marker records,
the marker was never about its exchange, so it is exact-sequence deleted and the claim
retried (F-062, and `claimQoS2ReleaseInner`). Removing that exception would restore F-062
mechanism 2 — a later PUBREC-acknowledged exchange answered `0x92` and its hold released —
so it is load-bearing rather than an optimisation.

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

### What the write audit established, and the gap it found

*`docs/findings/ha-arbitration.md` F-062 and F-036 are authoritative for the gap's
mechanisms, arms, directions and remedies. This section summarises and references them, as
the compatibility boundary's precedent prescribes; it is not a second authority.*

F-032 audited every mutation this protocol makes against
`test/depproof/`'s route inventory. **All four writes carry their per-subject
expectation** — the hold store, the releaser's claim and the discard's claim are
create-only (expectation `0`), and the terminal commit CASes at the exact sequence its
caller loaded, under a generation comparison. The server-side contract for those writes
is **inherited from F-030 rather than re-proved here**, per "Trusting the backend"; no
lineage is carried on this stream and none is needed, because a create has no base to
descend from and the terminal commit is already generation-guarded.

**The gap was the hold purge, and it was ours rather than JetStream's.** `purgeQoS2Hold`
supplied only a subject filter, so it removed whatever occupied the subject when it
applied; the server had no condition to enforce because the bridge never sent one. Its two
callers are arbitrated differently and **neither arbitration makes it exchange-bound**:

- The **teardown** caller runs only after `claimTeardown` is won, which fences a broker
  that lost the claim but not a peer already serving the same incarnation — a resume
  preserves the generation. That is **F-062**, together with the `discarded` marker the
  teardown leaves behind, which outlives its own exchange. **Closed** by three changes, and
  the third is not a detail of the first two. No arbitration could make a subject-named
  removal exchange-bound, so instead every call carries a cutoff it can prove, and the marker
  records the cutoff its writer was entitled to purge through — an entitlement, not evidence
  the hold is gone, since the marker is written before the purge — so a later exchange above
  that fence reaps it and proceeds. **Third: the AWAITED arm gained arbitration it previously
  lacked.** It had purged on observing the claim subject go empty, which establishes nothing
  about the exchange it enumerated — a releaser also deletes a foreign generation's claim
  before retrying its own create — so it now RE-COMPETES for the claim and destroys only under
  a marker it won. Of the four analysis arms, three purge; the awaited one does not. The three
  do **not** all use the same cutoff — the arm that adopts a pre-existing marker uses that
  marker's fence, which may exceed the adopting walk's own watermark — and F-062 carries the
  authoritative mapping.
  `purgeQoS2Hold` has **no unbounded
  form**, so no caller can opt out, and the two cutoffs that cannot bound anything (`0`,
  which purges nothing while reporting success, and `MaxUint64`, which wraps to the
  unbounded request) are refused rather than sent.
- The **orphan sweep** caller runs under no teardown claim at all, and infers liveness
  from this broker's residents plus persisted records, so a live remote generation whose
  record is absent is invisible to it. With the one-shot enumeration that misses a hold on
  a subject it never saw, that is **F-036**, whose scope was widened for both — and widened
  again by the cutoff, which spares an enumerated hold that landed above the watermark
  rather than destroying it. Sparing an acknowledged payload is a leak where destroying it
  was a loss, so that is the right direction, but it is a larger population for F-036 to
  answer.

What JetStream does **not** offer is an expected-sequence (CAS) purge, so no design here
may assume one; the cutoff is a watermark, not an identity. What the cutoff therefore
does not do is make the teardown own exactly the set it enumerated — see
"JetStream behaviors this design depends on" for why `State` and `State.Subjects` are not
one snapshot, and `docs/CONFORMANCE.md` for the residuals that follow.

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
| assignment claim, one per `(group durable, source sequence)` | `$MQTT_qos2assign`, `$MQTT.qos2.assign.<group-durable>.<seq>` | a **create-only** CAS elects the winner; every later transition is a CAS at the record's exact sequence. Seven of the ten transitions also assert the winner's identity, and **four assert the claim's INCARNATION** (below); the transitions that assert neither record owner-independent facts about the source message or the session and rest on the per-subject CAS alone |
| affine copy of the message | `$MQTT_out`, `$MQTT.out.q2.<client-id-hash>.<generation>.<claim-hash>.<incarnation>` | a **create-only** CAS, so exactly one of several legitimate materializers wins |

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

**Claim incarnation, and why the subject is not an identity.** The election is a
create-only CAS, so a claim subject can be won **again** after its record is reaped —
by a group delivery callback that was already dispatched when the reap happened, which
`DeliverNew` on the group durable cannot cancel. `Winner`, `Gen` and `ClaimHash` are
identical across those attempts, and a stream sequence is not an identity — a
`$MQTT_qos2assign` whose sequence space restarts reissues them, which no supported
operation now produces on its own (F-066 made the nine streams one recreation unit, and
a supported reset takes the referenced state with the claims), but administrative
deletion and partial restore still do. So the record carries a `Nonce`, minted
once at the election and never rewritten, and it does three things:

- It scopes the **copy subject**, so a replacement can neither read, adopt nor lose
  the copy of the incarnation it replaced. Without it both incarnations derive one
  subject, and a superseded materializer's withdrawal destroys the replacement's copy.
- It scopes the copy's **`Nats-Msg-Id`**, because a dedupe entry outlives its message:
  an incarnation-blind key would make a replacement's store onto its own fresh subject
  a silent no-op for the whole `Duplicates` window, so that claim would never
  materialize. This is not the attempt-scoped identity F-063 refuted — one incarnation
  has many delivery attempts and every one produces the same key, which is what keeps
  a retry idempotent — so the value is taken from the PERSISTED claim, never from an
  election candidate that may have lost.
- It **fences the transitions that state something about one incarnation's own copy**:
  the `claimed → materialized` phase, `AffineDone`, `Lost`, and the source-gone
  `Discarded`. `advanceQ2AffClaim` checks it after loading the record and **before**
  mutating — deciding afterwards is too late, because the write has already moved the
  successor's record. `SourceAcked` is a fact about the source MESSAGE and the teardown
  discards are facts about the SESSION, so those opt out and apply to whichever
  incarnation holds the subject.

**What the carriers hold is the copy SUBJECT, not the incarnation.** An actor holding a
delivered copy derives the incarnation from that subject. Where no delivered subject is at
hand — the pending entry and the in-flight register, which the PUBREC, expiry and
oversize-discard paths reach holding a packet identifier and nothing else — the pair
`(claim subject, copy subject)` is carried instead, and every use binds the two. A bare
incarnation was the earlier form and is not enough: it says which incarnation a claim
reference belongs to but never WHICH COPY, so a carrier naming another live claim's
subject together with that claim's own valid incarnation is internally consistent and
still about a different delivery. Crediting it would record a completion on a claim that
never delivered anything, and the delivery gate's terminal branch then disposes of that
claim's copy — the message is lost, which is F-065's failure in a different disguise.

Three checks make a carried pair usable, and none of them follows from the others:

- **the record's own shape** (`mqttInflightRecordIdentityOK`): a register naming a claim
  whose carried copy yields no incarnation is REFUSED rather than interpreted, because
  `_EMPTY_` is `advanceQ2AffClaim`'s deliberate opt-out and would silently disable the
  fence;
- **the claim must name THIS copy** (`q2AffClaimForCarriedCopy`, the single place a
  carried pair becomes a claim), which is what refuses a reference to a foreign claim.
  A mismatch is treated as a SUPERSEDED INCARNATION — the one mismatch that licenses
  disposing of the copy — only when the mismatching copy is a copy of THAT assignment under
  another incarnation: same owner, same generation, same claim hash, differing in the
  incarnation token and nothing else, which is the only shape a reap-and-re-election
  produces. That is one predicate
  (`q2AffCopyIsSupersededIncarnationOf`) rather than a comparison repeated per site, and it
  is one because **both** disposing paths were caught, a review round apart, disposing on
  bare subject inequality: the retirement route, and the delivery gate, which takes its
  claim subject from the copy's own HEADER and so meets a copy persisted on one
  assignment's subject under another assignment's header. Bare inequality is a weaker
  statement — the assignment whose copy subject it actually is may be perfectly live — so
  acknowledging it away loses that message and strands its claim without `AffineDone`. A
  cross-assignment mismatch is a persisted shape this broker does not write, so it is
  refused, paced, and left for an actor with evidence.

  On the retirement route the distinction is load-bearing rather than cosmetic: the
  transition refuses either way, but the retirement it leaves behind outlives it and settles
  on the copy in hand at the next redelivery;
- **on recovery, the register's own SOURCE must PROVE the pair.** For an affine delivery
  the source IS the copy, so a pair is installed only where the source is LIVE and the
  stored message at the register's `Dur`/`Seq` is on the carried copy subject — the one
  fact neither of the other two checks can see. The predicate is positive, and that is a
  reversal of the first form built here: refusing only a live source that names a
  *different* copy left an indeterminate consumer lookup, an indeterminate message read,
  and an authoritatively absent consumer all admitting an internally consistent forgery
  whole. "An absence is not evidence of a forgery" is true and beside the point — nothing
  licenses installing an authoritative pair on the strength of a source that was never
  read. A COMPLETE source is outside the predicate, because no pair is installed for one:
  that register is dropped as a delivery whose exchange finished. The source is read anyway
  to classify the register, so the check costs no extra request.

  An unproven register is then reserved rather than interpreted, and it joins the
  **duplicate-source grouping as an unowned member** rather than being set aside on its
  own. That difference is the whole protection: an unproven pair does not prove the
  identifier was never transmitted — the client may hold THAT identifier rather than a
  sibling's — so setting it aside would turn a two-member group into a one-member one and
  install the sibling as an authoritative singleton *with* a `cpending` mapping, which the
  source's redelivery then follows to re-send the payload under a second identifier.
  Reserving both identifiers does not prevent that; only withholding the mapping does. As
  an unowned member it poisons every sibling of its source, exactly as a register whose
  claim could not be established does: every identifier reserved, no record installed, no
  source mapping, and new outbound QoS 1/2 allocation withheld for the session. The
  reservation is also marked so no terminal path settles the record — the client's own next
  acknowledgement must not delete the evidence an operator has to look at.

**A refused copy is handed back on an exponential backoff, never on a plain NAK.** Every
refusal on the affine delivery path — an unreadable claim record, an absent one, an
unattributable one, a phase not yet advanced, and the retired settlement's own refusals —
goes through one helper that NAKs *with a delay*. A plain NAK is re-offered immediately, so
a refusal state that PERSISTS becomes a request storm: one copy under an absent claim was
measured at 9214 deliveries in ten seconds, each a claim load and a log line. Those states
are not exotic — `docs/CONFORMANCE.md` states three windows in which a copy legitimately
outlives the claim that would authorize it, and an unavailable backend produces the same
shape for as long as it lasts — so the storm is reachable in ordinary degraded operation.

The delay doubles from 100 ms to a 30-second cap, and it is derived from **JetStream's own
delivery count** for that message, so it needs no per-copy state: nothing to grow, nothing
to evict, nothing a restart loses. The first refusal is paced at the base delay, which
keeps the common interleaving fast — a copy delivered before its claim's phase advance is
normal, since the copy is stored before the record is advanced — while a sustained refusal
settles at the cap, which paces its diagnostics with it. A payload nats-server cannot parse
is treated as a plain NAK (`consumer.go`), so the wire form has its own test.

**Claim state.** A claim carries a monotonic phase (`claimed` → `materialized`,
"was a copy provably created at some earlier instant?") plus three independent
completion facts:
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
retention took its pending copies with it, so delivery of a QoS 2 message the
broker accepted can no longer be PROVEN. `Lost` is terminal (no contender waits
on it, and the group delivery is released) but never reapable, because the record
is the only durable evidence of that unproven outcome and a clean reap would
erase it. `Lost` and `AffineDone` are mutually exclusive and are not
independently merged: whichever the durable record already proved stands, unless
`PubRecProven` is present, which outranks both (below).

**`Lost` is not proof that the message failed to reach the client**, and the
distinction is operator-facing. The write order that justifies the inference
holds for the JetStream copy acknowledgement, not for the client's PUBREC, which
is answered before `AffineDone` is recorded — see the write-order paragraph
below. A client may therefore have received and acknowledged the PUBLISH while
the assignment looks lost. Nothing is redistributed and the client is not
deprived, so this was a reporting defect rather than a deviation.

**A sixth field, `PubRecProven`, is what corrects it, and it exists as its own
field rather than reusing `AffineDone` for a reason that is not stylistic.** The
stored PUBREL is the durable, account-wide record that the client answered
PUBREC, so it now NAMES the assignment it completes — as a bound
`(claim subject, copy subject)` pair, never the claim subject alone, because a
claim subject is reused by a later incarnation and a bare reference would credit
a successor claim with a delivery it never made (the F-065 loss, committed by the
remedy for it). `PubRecProven` is set only by the two routes that hold such a
resolved carrier: the owner's resume pass and the PUBCOMP arm below.

`PubRecProven` is the ONLY fact in the record that outranks `Lost`. `Lost` is an
inference from an absent copy — "could not prove", not "never delivered" — and an
inference must never outrank a proof, so `mergeProven` clears `Lost` when
`PubRecProven` is present, in either arrival order. Before that rule a
cross-broker race made a mislabelling permanent: whichever landed first won, and
nothing could ever correct a `Lost` that got there first.

**Generic `AffineDone` is deliberately NOT given that power.** It means *terminal
copy disposition* and is written by seven routes — the session discarded, a
redelivery under an already-terminal claim, a subscribe denial, expiry, an
oversize discard, retired-source settlement, and the ordinary PUBREC. One of
those is the delivery gate's own `AffineDone || Lost` arm, so if `AffineDone`
outranked `Lost` then "this claim is already `Lost`" would become the reason to
clear `Lost`, confirm the copy acknowledgement and reap — laundering an unproven
outcome into a clean deletion. That path is reachable, not theoretical: a delayed
materializer holding a `claimed` snapshot can store a copy while recovery is
committing `Lost`, and the restored consumer delivers it.

A record asserting `PubRecProven ∧ Lost`, or `PubRecProven ∧ ¬AffineDone`, is one
this broker does not write. Both are REFUSED by the shape validator rather than
normalised by the next merge, and the refusal is checked before the
already-proven short circuit on both proof routes.

**The reference is resolved on resume, before any loss can be inferred.**
`reconcileQ2AffPubRelProofs` runs as the first step of the affine half of
`resendPendingRedelivery`: it walks this incarnation's stored PUBRELs, resolves
each bound reference through the single helper `q2AffClaimForCarriedCopy`, records
`PubRecProven` through the ordinary fenced transition, and **performs the reap
itself**. The reap cannot be discharged by ordering, because the
recovery-and-reap route is skipped whenever affine consumer restoration fails —
which is also why the pass runs before that restoration rather than beside
`recoverQ2AffClaims`. It needs no consumer: it walks `$MQTT_out` by subject and
writes to `$MQTT_qos2assign`. It is *not* serialized against affine deliveries —
the restore gate opens first — so a restored callback may be proving the same
claim concurrently; that is a fenced race, and the fence is the transition's
nonce-and-copy CAS, which both parties take.

The pass reports whether its evidence is COMPLETE, and `recoverQ2AffClaims`
refuses to infer a loss on anything less. An absent copy is only evidence of a
loss once the thing that would refute it has actually been looked for; concluding
one from a search that failed is a guess, not a fail-closed reading. The next
resume simply tries again. The loss diagnostic is also emitted only once its CAS
has landed, so a write that lost its race no longer announces a loss that was
never recorded.

**PUBCOMP will not acknowledge a PUBREL away while the proof it carries is
unrecorded**, and it holds the reservation while it refuses. The obligation is
resolved BEFORE the pending entry is removed, and the entry is untracked only on
the path that will acknowledge: `qos12InflightForCap` counts `pendingPubRel`
against Receive Maximum, so an entry removed and then withheld frees its packet
identifier, a later exchange re-mints it after a wrap, and when the original
redelivers the duplicate branch acknowledges it away — destroying the evidence
the withholding existed to protect. The final re-lock re-validates four things
before removing anything, because the resolution ran without the session lock:
the connection still owns the session, the entry is still present, it still names
the same `(durable, sequence)`, and it still carries an acknowledgement subject.
An AckWait redelivery of the same record is allowed through, which is why the
acknowledgement subject is taken from the untrack rather than from the earlier
copy. A claim that is authoritatively ABSENT acknowledges rather than wedging —
with no record there is nothing for a later resume to report untruthfully, and
withholding would hold the exchange on something that can never reappear — while
an unreadable one, an unsupported shape and a malformed carrier all withhold. A refused
carrier is reported ONCE per RECORD per connection: AckWait re-offers a withheld record
forever, so warning on every attempt is unbounded output about one unchanging fact — and
keying that suppression by anything coarser than the record silences a genuinely different
one, since several PUBREL records legitimately share a packet identifier. The same
discharge guards the **duplicate-record branch** of the PUBREL delivery callback: several
PUBREL records under one packet identifier is a supported stale-broker shape, and the
duplicate may carry the only reference to a *different* assignment, so it is discharged
before it is acknowledged away rather than simply discarded.

The reference's own validation is exact rather than lenient, because a lenient reading
fails OPEN: a value that parses but is not one the broker emits resolves to an absent
claim, and absence is an acknowledging outcome. So each header must appear **exactly
once** — reading the first of two would silently pick one of two assignments — and the
claim subject's sequence must be canonical, the value `strconv.FormatUint` produces, not
merely something `ParseUint` accepts.

The loss report is worded as **PROVISIONAL**, and when a proof CLEARS a `Lost` that was
already recorded the report is RETRACTED in the log. There is deliberately **no ordering
rule** between the two, because the code could not keep one: the record is the authority
and the log is a derived view of it, the two cannot be made transactional, and a concurrent
proof can clear the verdict and reap the claim between recovery's CAS and its own log line.
Wording the report provisionally removes that requirement instead of adding machinery to
meet it, so either interleaving reads truthfully.

A superseded broker's PUBREL store is not CAS-conditioned, so it can
land after this incarnation's scan completed and its verdict was written and logged; the
proof then clears `Lost` and the claim is reaped, so without the retraction the operator is
left with an erroneous report and nothing durable contradicting it.

The loss diagnostic is likewise emitted only for a loss that is ON the record the
transition left behind. A nil error is not the verdict: the transition reports success for
a write `mergeProven` turned into a no-op, which is exactly what a concurrent
`PubRecProven` does, and for a record that vanished between the reading and the write.

**Teardown records the proofs before it destroys them.** Deleting the PUBREL consumer
drops its records with it, and the discard that follows preserves any `Lost` already on the
claim — which `reapable` then refuses forever, leaving an authoritative record calling a
delivered message lost. So `deleteTeardownTargets` reconciles every stored PUBREL's
reference into its assignment BEFORE that delete, and fails closed on a walk it could not
complete or a claim it could not read, since the teardown is re-driven and the cost of
failing closed is a retry. It runs for an ADOPTED teardown too — the broker that deletes
the consumer is the one that owes the reconciliation — and every step is idempotent. The
one case that proceeds rather than failing closed is a reference this broker does not
write: it identifies no assignment, so no retry could succeed and failing closed would
wedge the teardown forever.

What that reconciliation does NOT do is fence the writer: it is not atomic with the
delete, so a broker still serving the retiring generation can store a proof-bearing PUBREL
inside the interval between the scan and the deletion. Closing that needs a durable
mechanism — writer quiescence, a retention handoff, or a claim-side intent. A
store-then-verify read of the retirement watermark is REFUTED on three independent counts,
recorded together so a superficially revised version cannot repeat the other two: that
watermark is one replaceable record per CLIENT rather than one per generation, and
`recordGenRetirement` advances on `MarkerSeq` alone, so a teardown of one generation can
run while the slot names another; the verifying read is not crash-persistent, so a writer
that stops between its store and its read reopens the window; and the arm such a read would
take does not clear a `Lost` already on the claim, so it would not have corrected the
record even when it fired.

The residuals this leaves are stated in `docs/CONFORMANCE.md`.

**Releasing the group source is authorized by evidence, never by the phase.** The
phase is a record of the past: `$MQTT_out` is interest-retained, so the copy a
`materialized` claim names can be dropped with its consumer afterwards, while the
source ack is irreversible and releases the message from the shared group forever.
So `materializeQ2AffCopy` proves the copy with a PRESENCE CHECK on the copy
subject, and `sourceAckAuthorized` admits the ack on
`copyPresent ∨ AffineDone ∨ Discarded ∨ Lost ∨ SourceAcked` and on nothing else.
`PubRecProven` adds no disjunct, because it implies `AffineDone`.

Three consequences, all deliberate:

- **A store RESPONSE is not evidence, in either direction.** A success can be a
  no-interest store that kept nothing; a rejected create-only CAS means "the
  subject is not empty", not "a copy is there now". Both outcomes therefore
  converge on one presence check rather than only the rejected one.
- **An already-`materialized` claim is re-verified and never re-stored.** An absent
  copy under it is ambiguous — consumed by its own delivery, or dropped with its
  consumer — and a replacement would be a copy no delivery may publish.
- **A failed materialization does not end the attempt.** The claim is re-read and a
  terminal fact still authorizes the release, because `reapable` refuses without
  `SourceAcked`: withholding it would make every discarded claim permanently
  unreapable. `Discarded` is required here, not merely permitted.

Where nothing is authorized the move proves nothing, writes nothing and acks
nothing, leaving the source for AckWait. For a claim that reached `materialized`
that refusal still does not resolve anything, and the delivery waits for its owner
— F-064's residual 8. For a claim that never did, the contender now RESTORES the
winner's destroyed consumer and materializes onto it (see "On-behalf restoration"
below), so the stall is closed without the owner. The residual it keeps —
a consumer deleted between the final proof and the ack — is stated in
`docs/CONFORMANCE.md`; `CONSUMER.DELETE` is unconditioned, so no finite check
closes it.

**The retry bound is conditional, and it was measured rather than assumed**
(`TestQ2AffCopyStoreWithNoInterestSequenceAndDedupe`). The create-only expectation
is checked against the current last message on the SUBJECT, so an absent subject
accepts expected `0` however many no-interest stores preceded it — a retry is never
rejected as a CAS conflict. Deduplication is checked afterwards, and its entry
outlives the message it was recorded for, so a retry carrying the copy's stable
`Nats-Msg-Id` stores nothing even with interest present. Once retaining interest
exists and remains, suppression therefore lasts at most the stream's **configured**
duplicate window — the validator WAIVES `Duplicates` rather than fixing it, so an
accepted `$MQTT_out` may carry any window. Where interest never returns there is no
bound at all, and that indefinite stall is F-064's. Varying the msg-id per attempt
is **refused**: expect-0 succeeds on a now-empty subject, so a stale materializer
would store a second copy that NAKs forever once its claim is reaped.

Telling the two apart is a matter of WRITE ORDER, not of observation. `AffineDone`
is recorded **before** the copy is acked, so an absent copy under a claim that
never reached `AffineDone` was never acked **by the JetStream delivery** — it was
dropped with its consumer. Every broker reads that identically, which an "I saw the
consumer disappear" flag could not: the broker that observed it may be gone, or the
consumer may have been recreated, by the time anyone reconciles.

**That inference is about the copy ack, and it does NOT extend to the client's
PUBREC.** `mqttProcessPublishReceived` calls `trackAsPubRel` and stores the
correlated PUBREL *before* it reaches `mqttFinishQ2AffineDelivery`, which is what
records `AffineDone`. So PUBREC, then a broker stop, then an out-of-band consumer
destruction leaves `materialized ∧ ¬AffineDone ∧ copy absent` for a message the
client has already answered. An earlier revision of this paragraph said such a copy
"was never acked by a delivery" without that qualification; it is corrected here
because two decisions rest on the distinction:

- Re-materializing a `materialized` claim is **refused**, precisely because a
  replacement copy there can become a second PUBLISH for an exchange already past
  PUBREC.
- The `Lost` that owner-side recovery would record for this shape was therefore
  sometimes a **false loss report**. It never was a conformance defect — `Lost`
  releases the group delivery without re-offering the message, so nothing is
  redistributed — but it was untruthful. It is **corrected**: the stored PUBREL
  now names the assignment it completes, and `reconcileQ2AffPubRelProofs` resolves
  that reference before any loss can be inferred, recording `PubRecProven`, which
  outranks `Lost`. See the `PubRecProven` paragraphs above.

That inference is authoritative **while the owning session record still exists**.
Once it is gone, a legitimate teardown deleting the consumer and an out-of-band
destruction of it are indistinguishable — both leave a materialized claim with no
copy and no `AffineDone` — so orphan reconciliation records `Discarded` rather
than `Lost`. A `Lost` flag already committed is never reaped and is corrected only
by a `PubRecProven` that a surviving PUBREL still licenses; what cannot always be
recovered is a loss that happened before the session record disappeared and was
never observed by the owner. See "Orphan reconciliation". The inverted
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
redistribution [MQTT-4.8.2-5] forbids. `materializeQ2AffOnBehalf` therefore
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
consumer and outstanding claims. The consequence is **exactly one pass**, and stating it
more broadly would be false: the reference
walk reports the affine half indeterminate and the caller suppresses
`reconcileOrphanQ2AffClaims` for the whole account, permanently, since nothing rewrites
the record and expiry will not remove it either. It is not a gate over "every affine
resource" — the retired affine **register** arm sits outside it and can still mutate
claims, and affine consumer deletion is gone entirely (F-036 course E). Its identity
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
that would stop it. See `docs/CONFORMANCE.md` for this and for the residual that does
remain: a consumer deleted between the copy proof and the source ack (F-063).
Packet-identifier continuity is **not** among them — the in-flight register closed it on
every outbound path.

### On-behalf restoration of a destroyed affine consumer

`$MQTT_out` is interest-retained and `CONSUMER.DELETE` carries no precondition, so
a winner's affine durable can be destroyed out of band while it is away — taking
every copy it held. Nothing owner-independent used to RESOLVE such an assignment:
the dispatcher rereads the claim on every redelivery, the sweep skips any claim of
a referenced generation, and `recoverQ2AffClaims` runs only from the owner's own
resume. Since MQTT permits a session that never expires, "the owner comes back" is
not a reconciler, and the group consumer sets no `MaxDeliver`, so the source is
re-offered forever while holding a `MaxAckPending` slot.

A contender taking such a redelivery now restores the durable from the winner's
record and materializes onto it. **Presence and authoritative absence are not two
paths**: the same create-or-update runs either way, differing only in which
`MemoryStorage` it carries.

- **Replayability is whole-struct equality, not a field list.** The canonical
  configuration has a single constructor, `buildQ2AffineConsumerConfig`, and the
  record's `Q2Aff` is accepted only when it `reflect.DeepEqual`s what that
  constructor returns for the same names plus the record's own three variable
  fields (`AckWait`, `MaxAckPending`, `MemoryStorage`). A list cannot close a
  struct a dependency bump can extend, and `MaxDeliver` alone — outside every list
  an earlier design wrote — reopens the stall. This is not defensive padding:
  `mqttValidateRecordAffinePair` permits `Q2AffGen` with a nil `Q2Aff`, and
  `restoreQ2AffineUnwind` produces a real names-only record whose zero values are
  actively wrong (`AckPolicy` zero is `AckNone`, `DeliverPolicy` zero is
  `DeliverAll`). The comparison is against the persisted REQUEST, which is what
  the record holds.
- **The three variable fields are bounded** at the provisionable domain: `AckWait`
  positive, `MaxAckPending` within `1..mqttMaxAckTotalLimit` and within
  `$MQTT_out`'s live `ConsumerLimits.MaxAckPending` when that is positive.
- **`ConsumerLimits.InactiveThreshold` is read BEFORE the create and refused when
  it is nonzero**, with no create attempted. nats-server substitutes the stream's
  limit whenever the request field is zero, and this configuration deliberately
  sends zero — so for an already-present durable a create-or-update would apply
  the substitution and restart the deletion timer, and refusing afterwards would
  refuse a durable the refusal itself had just made reapable. The predicate is
  `!= 0`, not `> 0`: the field is a signed duration, and the server's own limit
  rejection fires only for a positive limit, so a negative one would slip past and
  mutate the durable anyway. The returned-config check stays, because the
  pre-check is not atomic with the create.
- **A storage type is immutable, so the contender adopts rather than imposes.** It
  builds with the actual durable's `MemoryStorage` where one exists and the
  record's where none does, never its own local option — the owner will meet this
  durable later. On a create refused for that exact cause it looks up **once** and
  retries; a second conflict refuses. Where the relookup finds the durable
  **authoritatively gone**, the retry is issued even though it carries the same
  value the refused request did: the obstacle has been removed, so the identical
  request is exactly what now succeeds, and stopping on that equality would defer
  restoration to another redelivery for no reason. One attempt is a bounded
  fail-closed policy, not a proof the conflict cannot recur, and it is safe
  because no copy has been stored and no source acknowledged at that point.
- **An indeterminate lookup is not an absence.** Only `JSConsumerNotFoundErr`
  authorizes the absent branch; a transport error, a `nil` `ConsumerInfo` and a
  non-nil one carrying a `nil` `Config` all refuse with no create attempted.

**Every refusal above is on the CONTENDER's path only, and that asymmetry is
deliberate.** A refusal costs different things on the two paths. On the contender
it is free: nothing is stored, no source is acked, and no client is connected to
notice. On the owner it costs a live connection — a refusal while a saved shared
filter is being restored at CONNECT is silently skipped, because there is no SUBACK
on that path, and the only remedy that removes the silence closes the connection,
publishing the client's Will and, at a zero Session Expiry Interval, deleting the
session. `ensureQ2AffineConsumerSubscription` therefore gains **no** new refusal:
its initial create is unchanged and it performs no lookup and no stream preflight.

It gains exactly one thing, and it can only turn a current failure into a success:
**a post-failure recovery on a storage-type conflict**. `MemoryStorage` is
immutable, and the owner rebuilds its request from the CURRENT local option while
the durable keeps whatever it was made with — so an operator who reloads
`consumer_memory_storage` breaks affine resume for every session whose durable
predates the reload, permanently, with no contender involved. On that exact
flattened cause the owner looks the durable up once and retries with the storage it
actually has. On a failed lookup or a failed retry the caller-visible outcome and
the reported error are unchanged from today; the lookup's own error is never
substituted for the create's.

**Why this path has that defect and its siblings do not** — which is also the
route that would remove the residual below, if it is ever worth taking.
`ensurePubRelConsumerSubscription` verifies before it creates: it looks the
durable up, marks it verified and RETURNS when it is there, and reaches its
current-option create only when the durable is confirmed absent. An absent
durable cannot conflict on an immutable field, so a reload never reaches one. The
per-subscription consumers escape for the SAME reason, not because of their
names: a persisted `sess.cons[sid]` is verified by the resume reconciliation
phase and then adopted IN PLACE at CONNECT, with no create-or-update carrying
current options. Their `nuid`-bearing identities are minted only where there is
no present consumer to conflict with — a genuinely new subscription, or repair
after the old durable was found authoritatively absent — so the `nuid` protects
those cases rather than the reload. The affine path is the
deviant: it issues an unconditional create-or-update, deliberately, because that
create doubles as the verification that a persisted config still describes a
consumer which exists and still looks like this. That is what puts the current
local option in front of an existing durable, and it is the whole origin of the
conflict this recovery absorbs.

It first reads the live stream and **declines to retry at all** while
`ConsumerLimits.InactiveThreshold` is nonzero. The refused create mutated nothing,
because JetStream checks convergence before it applies anything; the retry is
accepted and would therefore have the stream's limit substituted into it, arming a
deletion timer on a durable the failure had left alone. A retry whose response is
then lost would report failure while leaving that durable due to be reaped with the
session's interest-retained copies — an unresumable session turned into a losing
one. Monotonicity is about state as well as about the caller's outcome, and this is
where the two could have diverged.

The read is **not atomic** with the retry, so a limit landing between them still
reaches it. That window is stated in `docs/CONFORMANCE.md` with the three
conditions it needs, rather than narrowed a third time: consumer create carries no
precondition, so nothing here can fence the update against a concurrent stream
change, and the debris it leaves is the same one an ordinary owner create already
produces under the same mutation.

### What the write audit established, and the gaps it found

*`docs/findings/ha-arbitration.md` F-033, F-063, F-064, F-065 and F-066 are
authoritative for these mechanisms, arms, directions and remedies. This section
summarises and references them, as the compatibility boundary's precedent prescribes; it
is not a second authority.*

F-033 audited every mutation this protocol makes against `test/depproof/`'s route
inventory. **Every claim write carries its per-subject expectation**, because they are all
funnelled through one CAS helper that emits `JSExpectedLastSubjSeq` unconditionally: the
election is create-only (expectation `0`), every later transition CASes at the sequence its
own attempt loaded, and the reap deletes an exact sequence. Seven of the ten transitions
additionally assert the winner's identity. Of the three that do not, the **two
`SourceAcked` writes** rest on the per-subject CAS alone — a fact about the source MESSAGE
applies to whichever incarnation holds the subject — while **`AffineDone` is
owner-independent but incarnation- and copy-fenced**: it states something about ONE
incarnation's own copy, so it asserts the claim's `Nonce` and requires the record to still
name the copy the delivery carried (F-065). Owner-independent is therefore not the same as
unfenced, and removing that fence would not be consistent with this audit. The server-side
contract for those writes is **inherited from F-030 rather than
re-proved here**, per "Trusting the backend"; no lineage is carried on this stream and none
is needed, because a create has no base to descend from and a transition that CASes at a
base it read already answers "is this record still mine?" more strongly than a parent
pointer would. Nothing purges this stream, and the startup validators refuse every
configuration that would evict a claim with no writer at all.

**The gaps are on the other side of the protocol — the copies, not the claims.** The one
operation that carries no condition is `CONSUMER.DELETE` on `$MQTT_out`: the API has no
precondition field, so it removes whatever consumer holds that name when it applies, and
because the stream is interest-retained that destroys every copy the consumer still held.
Three defects follow, each resting on a premise of its own:

- **F-063 — fixed 2026-08-19, guarantee re-scoped.** Nothing bound evidence that a copy was
  RETAINED to the irreversible source acknowledgement that follows it: a store onto a
  deleted consumer succeeds and keeps nothing, and the only re-read was on the
  CAS-rejection path, so the claim reached `materialized` and the group source was released
  for a message that no longer existed. Every route now proves the copy with a presence
  check and releases the source only on
  `copyPresent ∨ AffineDone ∨ Discarded ∨ Lost ∨ SourceAcked`. Because `CONSUMER.DELETE`
  carries no precondition, the guarantee is conditioned on a fresh but **non-atomic**
  observation. The residual spans the final copy lookup through any intervening claim
  load or CAS retry to the confirmed source ack — no bound narrower than that is
  claimed — and is stated in `docs/CONFORMANCE.md`.
- **F-064** — a live generation reference makes the sweep skip every claim of that
  generation. The sweep still does, deliberately: it carries no payload, so it could only
  resolve such a claim by asserting a fact it cannot prove. What has changed is that the
  STALL no longer waits for the owner — a group redelivery does carry the payload, and
  restores the winner's consumer on its behalf. Two things remain: settled claims of a live
  generation are still not reaped (**F-065**'s fence), and a `materialized` claim whose copy
  is gone is still owner-dependent, which is why the `Lost` narrative in
  `docs/CONFORMANCE.md` still holds only for a session that comes back.
- **F-065 — fixed.** The reaper evaluated its two-sided predicate on a copy-absence
  reading taken BEFORE its own `SourceAcked` transition, so a copy stored during that
  round trip was invisible and the claim was deleted out from under it — leaving a copy
  the delivery gate can only NAK, forever, holding a `MaxAckPending` slot. Three routes
  reached that state and all three are closed: the reaper now takes its copy reading
  AFTER its own last write and immediately before the delete; a materialization attempt
  WITHDRAWS the exact copy sequence it proved present once it proves no claim of that
  copy's incarnation remains; and the claim now carries an INCARNATION, so a replacement
  election can neither adopt a superseded copy nor be written to by a superseded actor.
  A PUBREC that leaves the affine side unproven additionally RETIRES its source, so the
  register it retains is read as "past PUBREC" rather than as a live PUBLISH — gated on a
  SUCCESSFUL PUBREC, because a rejected delivery stores no PUBREL and therefore has no
  PUBCOMP at which an unlanded durable mark could be retried.
  The stated guarantee is **re-scoped**, as F-063's was: the
  withdrawal is a compensation rather than an arbitration — claim and copy are on
  separate streams and JetStream cannot commit to both together — so a copy stranded by
  an attempt that crashed, whose delete was unconfirmed, or whose final claim reading was
  indeterminate is bounded by ordinary teardown, or by the gate disposing it once a later
  incarnation holds the subject, and is otherwise retained. (A later orphan-sweep consumer
  deletion used to be a third bound; F-036 course E removed it.) See `docs/CONFORMANCE.md`.
- **F-066** — a `stopAll`-instructed recreation of ONE stream is treated as a
  self-contained procedure, but durable records persist identities other streams issue. A
  verified sample of such identities — explicitly not a census, since four enumeration
  attempts were falsified and the census was deferred to F-034, which landed it on
  2026-08-22 — shows **three** epoch
  couplings, all against `$MQTT_sess` and all ad hoc, with every confirmed reference to
  `$MQTT_msgs`, `$MQTT_out`, `$MQTT_qos2assign` and `$MQTT_inflight` uncoupled, and one
  `$MQTT_sess` reference uncoupled too. On this
  path the VERIFIED AFFECTED SUBSET — not a proven closed unit; F-034's census enumerates
  the population but is a drift GUARD over the interpretation surface rather than a closure
  proof, so it does not settle that either — is those four
  streams; recreating a proper subset loses accepted messages, delivers an OLD payload for
  a new message, or NAK-loops orphaned copies forever.

Two consequences on this path are **F-036's** rather than ours. The contender's discard arm
firing on an absent winner record rests on F-036's own falsified premise; the
different-generation reading is settled on the clean-start route, where teardown marks every
assignment of the retiring generation `Discarded` before deleting its consumer. And the
orphan sweep's affine consumer delete — by name, past a `Created` fingerprint that a
resuming session's ADOPTION left unchanged — reached its window only when a session was
resident with that generation while its record was absent, which is F-036's premise
exactly. **It was REMOVED by F-036 course E**: every positive authorization for it was
sought and refused, so the route is gone rather than guarded, and the accepted price is
the correlated leak in `docs/CONFORMANCE.md`.

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
   minted here (left alone, restoration would create its consumer later in the
   resume and never persist it: the restore path runs no final subscription
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
   kind: **10071** (the single-writer check) means the subject MOVED after our
   accept save — which is weaker than a takeover, and F-084 owns the gap — and is disposed
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
   is served meanwhile: no SUCCESS CONNACK was sent — a failure CONNACK carrying
   0x83 and the phase's own diagnostic is, which opens nothing — and every phase
   subscription is closed.
5. **Adopt in place.** Subscription restoration — which runs BEFORE the CONNACK
   (F-050) and after this phase — reaches each
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
6. **Sweep what restoration did not adopt** — and note that F-050 emptied this
   step's population. It was written for a filter restoration skipped *after* the
   CONNACK while the connection stayed live, which nothing else would ever
   reclaim: the subscription would sit closed forever holding consumer interest,
   and a later re-SUBSCRIBE's metadata refresh does not clear `closed`. **That
   state is no longer reachable for a supported record.** Restoration now runs
   before the CONNACK and a filter it cannot install refuses the CONNECT, so an
   unadopted collected filter always ends in a refusal and a teardown rather than
   a live connection. The sweep is retained as the reclaimer of last resort — it
   still runs immediately after `processSubs`, before its error is tested, with an
   idempotent deferred fallback for the exits that never reach it — but it is now
   belt-and-braces rather than the only thing standing between a skipped filter
   and permanent dead interest. The phase's own `undo` is unaffected and remains
   live. Every reclamation path releases the gate **before** unsubscribing, so a
   parked delivery wakes, finds the subscription still closed, and skips instead
   of waiting out the bound.

## Shared group durables are verified on resume, not assumed

The plain reconciliation above deliberately covers only plain filters. Its collect step
says shared subscriptions "live in `SharedSubs` and their group durables have their own
machinery" — and until F-076 that machinery did not exist. This section is it.

`processSharedJSConsumer` has three arms. A re-SUBSCRIBE with a live delivery subscription
refreshes the mutable per-subscription fields. A filter with **no resident `cons` entry**
joins: it creates the group durable, which proves it. Between them sat a fall-through for
a resident entry with no live subscription — a session resume on a new connection — which
subscribed to the recorded deliver subject on the strength of a configuration remembered
from an earlier connection, and proved nothing.

That is the ordinary same-broker reconnect, not an edge case. Shared consumer configs are
**never persisted** (`buildPersistedSessionLocked` skips every entry whose `DeliverGroup`
is non-empty, because `SharedSubs` is the sole authority), so a session rebuilt from its
record has no shared `cons` entry at all and takes the join path. A resident `cons` entry
therefore means the session object survived in this process. If the group durable had been
deleted meanwhile — a concurrent leaver's last-holder race, the orphan sweep's grace
window, an operator — the client was told Session Present = 1 and handed a live
subscription on a subject nothing publishes to, silently.

**Two batched passes, around `processSubs`.** A pre-pass collects exactly the shared sids
with a resident entry — derivable before restoration runs, since the sid is the filter's
session key and the level-up companion follows the same rule `processSubs` applies — and
resolves them through `lookupConsumersMulti` in **bounded chunks** of
`mqttInflightLoadBatch`, the same cap every other resume-time batch here uses. The bound is
a correctness property, not tuning: a session's shared filter count is client-controlled,
each lookup carries its own timeout, and an indeterminate verdict refuses — so one
unbounded request would let a single lost response fail a large session closed on every
reconnect. For the same reason the **first** chunk-level failure stops the pass and marks
every unresolved target indeterminate rather than paying a timeout per remaining chunk:
one indeterminate filter already refuses the CONNECT, so continuing cannot rescue it, and
in the post-pass the stall would run while this connection's queue interest is already
installed and dropping QoS 0 deliveries. The resume arm then acts on its verdict
**before** subscribing, which keeps the join path's own rule that a group durable must not
deliver to a member until the join is proven.

A post-pass runs after `processSubs` returns, after its phase-sub sweep, and immediately
before the per-filter aggregation. It repeats the **full** classification rather than a
presence check, because a consumer can be deleted *or altered* between the two readings,
and it adopts the second one — a member-variable field may legitimately have moved. A
filter that regresses is marked failed and refused by the restore gate's existing
aggregation, so there is one refusal path rather than two.

**Four outcomes, and the distinctions are the mechanism.** *Present and canonical* adopts
a clone of the live configuration in place of the remembered one. *Present and not canonical*
refuses, and neither updates nor deletes: the durable may be serving other members, and a
configuration this broker would not have built is not one it may normalize on their behalf.
*Authoritatively absent* is **repairable** — the durable provably is not there, so the next
section recreates it and arbitrates the membership it is recreated into. *Indeterminate* —
a failed read, a reply carrying no consumer, a consumer carrying no configuration — refuses,
and is kept distinct from absence precisely because recreating on an unproven absence turns
one broker's transient error into a group-wide change. That distinction was the reason to
keep the two verdicts apart before anything acted on either, and it is what now decides
which one may be acted on.

**Validation is whole-struct and closed by construction.** Every `ConsumerConfig` field
carries exactly one disposition, enforced by a reflection bijection, because a list of
fields worth checking cannot close a struct a dependency bump can extend — the same
reasoning the affine path records. Three fields are legitimately variable across members
and are bounded rather than ignored; two are server-normalized and compared against an
**expected value** rather than accepted from the live consumer, since copying `MaxDeliver`
would admit a durable that discards a delivery after one attempt; one, `Metadata`, is
server-**owned**, its content moving with the dependency, and is constrained instead by
requiring it to carry nothing but the three named server keys. A test measures the
normalized set against a real create, so the expected values are observed rather than
remembered.

**What this establishes is CONFIGURATION evidence, never OWNERSHIP evidence.** A present
canonical durable does not prove this broker still owns the membership the restore is keyed
on. That is a separate fact needing a separate instrument, and the next section is it: on
the CONNECT path every restoration of shared interest — whether it created, repaired or
merely adopted — now proves ownership with a classified record write before the CONNACK. On
the **live SUBSCRIBE** path is arbitrated by its own packet-scoped gate and the same
classified proof (F-078, below).

The window is narrowed, not closed: `CONSUMER.DELETE` and `CONSUMER.CREATE` are
unconditioned, so a durable deleted or altered after the post-pass gets no further check on
that CONNECT. Self-repair now exists for exactly one verdict — an authoritative absence is
recreated — while a non-canonical or unreadable durable still goes on refusing until the
condition clears by any means. All of it is recorded in `docs/CONFORMANCE.md`.

A record carrying a shared group consumer configuration in `Cons` is a shape this broker
never writes, and is **refused** at every ingress that reads one rather than accommodated:
the CONNECT restore prologue, record-driven expiry, the session-expiry sweep, the
orphan-consumer sweep, the shared record validator, and the shared last-holder scan. The
refusals divide by what each one protects. The two **expiry** paths would claim, rewrite
and delete the record, normalizing a format nothing here understands, so they refuse to act
on it — the session-expiry sweep only once the record is otherwise eligible for expiry
processing, since a zero or never-expiring interval ends that pass before the shape is
reached. The two **delete-by-absence** readers decide what to destroy from the absence
of a
reference across the whole record set, so for them a partial view must never be reported as
a complete one: the orphan-consumer sweep **aborts** its pass rather than skipping, and the
last-holder scan **reports every requested durable held** rather than skipping. "The
last-holder scan fails closed on a record it cannot read", below, is that scan's own
section.

## A restored shared membership is arbitrated against the record

The verification above establishes what a group durable *is*. It establishes nothing about
whether this broker still *owns* the membership being restored into it, and those are
different facts needing different instruments. This section is the second one, for the
CONNECT/resume path.

**The finding (F-077).** Broker A holds a resident session with a shared membership. B
accepts a newer CONNECT for the same client ID, its path removes the membership — a 3.1.1
forced leave, an UNSUBSCRIBE, or `clear()` — and B's last-holder scan deletes the group
durable. A restores that membership anyway and serves the group. "Serves" includes
**acknowledging**: the QoS 1/2 delivery callback acks and drops on a reserved-subject
violation, on No Local suppression, and delivers-and-acks a shared QoS 0 member. (A
subscribe deny used to belong on that list and no longer does — on the ordinary
shared group consumer it now hands the message back rather than acking it, for the
reason given under the delivery guards below. It stays terminal on the affine
shared QoS 2 route, where the copy is already one session's and no other member
can take it. The remaining entries still destroy a group message for the members
that legitimately hold the membership, which is what this section arbitrates.)

**Three routes reach it, and only two of them create anything.** A resume whose durables
all verify present-and-canonical adopts and creates nothing, yet installs delivery interest
under a membership it never revalidated. A resume whose durable is authoritatively absent
now recreates it (below). A record-sourced filter — every shared filter of a session
rebuilt from its record, since shared configs are never persisted — takes the join path and
creates. All three are arbitrated; a create-scoped rule would have covered only the last
two.

**The instrument is a classified record write, and it has to be a write.** A read cannot
arbitrate: JetStream returns 10164 from the clustered pre-proposal check while a write for
the subject is in process, so "the record did not move" is not provable from a load — the
contender's proposal can commit a moment later. `proveSharedMembershipOwnership` performs
the ordinary record write under its per-subject CAS at `sess.seq`, inside the CONNECT's
still-open accept transaction, and classifies the outcome. On a resume the body is normally
the same membership that was read, so it looks like a no-op rewrite and is not one: every
write stamps a fresh `WID` and parents it to the current lineage, and the CAS is what no
read could be.

Five outcomes, and the two rejection kinds are deliberately not equivalent:

- **landed** — this broker owned the record at the write's linearization point. The only
  outcome that permits the CONNACK.
- **10071**, the single-writer check, means the subject moved after the accept save — not
  that another broker owns the session, which is the stronger claim F-084 owns. It
  disposes *without* consulting the contention resolver, so this is the only place a
  sibling lineage can be recognised on that arm: the record is read once and classified,
  which distinguishes a proven sibling from an unclassifiable stranger and mints the fork
  grant for the first only. Both classes now report `0x89`; what separates them is the
  Reason String and, decisively, the mint.
- **10164** is clustered contention rather than a lost race, and gets the bounded
  re-read/retry treatment, whose verdicts map onto the same five.
- **ambiguous** — the mutation is KEPT, no further write is permitted, and the resolver
  settles it. No SUCCESS CONNACK is written, so nothing is served meanwhile; the
  refusal itself carries 0x83 and the step's own diagnostic.
- **definite local failure** — nothing was stored; refuse.

Every refusal reuses the pre-CONNACK abort discipline, which already discards the Will and
leaves the lifetime ungranted. **No `0x8E` reaches the client**: the displacement's
DISCONNECT is gated on the CONNACK having been written, which is false for every refusal
inside the CONNECT, so no DISCONNECT is sent at all. The refusal is not silent, though —
it goes through `refuseDisposing` and carries a **failure CONNACK** whose reason code and
Reason String say what the evidence supports and no more. See "The pre-CONNACK refusal
signal", and "Only a held successor licenses a takeover claim".

**Why this path needs no transmission gate.** The restore gate is armed before the first
subscription the CONNECT installs and opens only after a confirmed success CONNACK, and
every delivery callback consults it — the QoS 0 raw subscription, the QoS 1/2 deliver
subject, and the PUBREL and affine subscriptions a shared QoS 2 filter provisions before
the group consumer exists. So nothing this connection could transmit or acknowledge escapes
the window. One caveat, which is a claim about this connection's outputs rather than about
the group: the gate stops transmission and acknowledgement but cannot undo *queue
selection*, so a shared QoS 0 message selected for this member while the gate is closed is
dropped and reaches no member at all. That is the restore gate's existing residual.

**The trigger is restored interest, and it is stack-local.** The fact that a restore
installed, repaired or adopted shared interest is raised at the sites that do it and
aggregated on the caller's stack — committed per filter only once the whole filter succeeds,
its level-up companion included. It is deliberately not a field on the session: a fact
parked there outlives the operation that raised it and would govern the next one. It is also
kept apart from two accounting facts it used to be inferred from, because each inference
was wrong in a different way. The repair creates a durable this session already holds the
`MaxAckPending` charge for, so it creates while charging nothing; a verify create gated on
the charge would skip exactly the population that just recreated a durable, and a refund
keyed on anything but this invocation's own charge would erase an earlier connection's.

**Absence is repairable; nothing else is.** The verification's four outcomes are re-cut into
one predicate — *terminal unless repairable* — with the authoritative absence as the only
repairable verdict. That is stated as a class rather than as a special case at each decision
site, because the batching seam it touches closed only when the special cases were deleted.
Repairability has to move three decisions together, and moving fewer leaves the repair
unreachable: the batch's termination, the pre-pass's filter-failure, and the resume arm's
own return. A chunk-level transport failure is not a per-target verdict and stays terminal
whatever sits around it.

The asymmetry between the two passes is the mechanism. The **pre-pass** treats absence as
repairable, so the join path recreates the durable and the ownership write then proves the
membership is still ours. The **post-pass** treats absence as terminal: a durable absent
again *after* the repair is not one this broker may create, it is one something is deleting
underneath this CONNECT.

**A durable this broker created and then lost the arbitration for is left in place**, and
that is a decision rather than an omission. Every available deletion is refuted — the
last-holder scan skips same-client-ID records, so after a takeover a blind last-holder
delete destroys the *new* owner's durable, and the shared release helper refunds a historic
charge and purges through an already-rejected transaction. If the new owner does hold the
membership, the durable is what it needs. It is not inert, though: the messages stream is
interest-retained so the durable keeps interest, it can accumulate pending across the
orphan sweep's grace window, and a later joiner may receive messages published while no
authoritative membership existed. `docs/CONFORMANCE.md` states that rather than implying it
away.

**What this establishes, and what it does not.** Ownership of the *record*, at the write's
linearization point — not continuously, and not the presence of the *consumer*.
`CONSUMER.CREATE` and `CONSUMER.DELETE` carry no precondition field at all, so no finite
check binds the durable's existence to the record's ownership: the durable may vanish at any
point after the last verification or write, this write included, and only the next resume
detects it. A takeover *after* a landed write is the session-record watch's and its
committed-read backstop's to converge.

**The live SUBSCRIBE path is arbitrated by the section below** (F-078). It reaches the same
interest by different routes and needs a packet-scoped transmission gate this path does not,
because the restore gate is already open on a live connection.

## A live SUBSCRIBE arbitrates its shared interest before it serves

The section above proves ownership for the CONNECT/resume path. A live SUBSCRIBE reaches the
same interest and, until F-078, revalidated nothing — so the consequence above was reachable
on a connection that was never restoring at all. Three routes get there: the `!exists`
create and its post-subscribe verify `CreateOrUpdate`; the resume fall-through, whose
resident consumer entry survives a rollback that reset the membership map, so a later
SUBSCRIBE subscribes on a remembered config having verified nothing; and the **refresh** arm,
which revives delivery interest on an existing subscription by updating its mutable fields.
All three are the same fact — *interest was touched* — which is why the trigger is that
rather than "a durable was created".

**The instrument is the same classified record write**, reused unchanged, under the packet's
own record transaction and placed after the packet's final save and its optional affine
unwind, so it is the last record write of the packet and its CAS linearizes where the packet
left the record.

**When it runs is a wider question than the trigger.** The gate opens without a proof only
for a packet that committed no trigger **and** none of whose record writes was rejected by
its per-subject CAS. The second half is load-bearing and not obvious: the early per-filter
membership save converts a definite rejection into a per-filter failure code and
continues, so a record that demonstrably *moved* leaves both the trigger and the returned
error empty. Three further writes lose the same evidence — the affine provisioning save,
the affine unwind, and the void-register verdict store, the last reached through two
chains and through the failed-filter cleanup as well. A packet-local fact records them;
the rejection marker they arm is only consulted before the *next* inbound packet, which is
far too late for a delivery callback already running.

**A CAS rejection, and nothing else, raises the question.** The distinction is evidential,
and not cosmetic: a CAS rejection means the expected-sequence write was *refused*, which
is positive evidence the subject moved; a definite local failure moved nothing and
re-authorised nothing; and an ambiguous write is this broker's own bytes maybe-landing,
which if they landed landed under its own CAS. Treating any failure as evidence turns an
ordinary per-filter failure into a connection-level refusal, which is a contract the
affine paths own rather than this one. The fact is read from the store-boundary detail,
because the outward error is reformatted and loses its identity, and because the
definite-failure classification deliberately merges a CAS rejection with a local failure
and a refused transaction.

**An AMBIGUOUS write does not withhold the gate either, and that ordering is deliberate.**
The quiet check precedes the unresolved-write check, so a packet whose ancillary save went
ambiguous opens the gate while the session still carries that write — and an unresolved
write is an outcome nobody *read*, so it includes "CAS-rejected, response lost". It stands
because a quiet packet installed **nothing**: with no trigger, no shared interest was
created or revived, so the open releases only traffic backed by interest that *predates* the
packet.
Withholding it would give one packet type a stronger delivery guarantee than this broker
provides for any other ambiguous record write, and it would turn an ambiguous affine
provisioning failure into a connection-level refusal — the contract the affine paths own,
which the wider trigger predicate already broke once. The read loop does attempt resolution
synchronously before the failure SUBACK is enqueued, but that attempt can itself return
unresolved, so the exposure is stated as a residual rather than as a bounded window.

**The transmission gate.** Armed before the packet installs anything — before the PUBREL and
affine subscriptions a shared QoS 2 filter provisions — and resolved exactly once through
a defer, so an error path or an abnormal unwind cannot leave it pending. It is scoped to the
connection that armed it *and* to a per-session generation: the first stops an abandoned
verdict crossing into the next connection, the second stops a waiter that watched one packet's
gate from escaping the next packet's — **up to that waiter's final read**, which is the
honest boundary: a gate published before the read is observed by it, and a waiter past it
is the final-check residual the generation does not reach. Its terminal state is a single
CAS, unlike the restore
gate's store-then-CAS pair, because this gate has more terminal paths and a losing opener
must not be able to flip a withheld verdict to open.

All four delivery callbacks consult **both** gates, and the three that can wait — QoS 1/2
delivery, PUBREL and the affine copy — do it through **one** wait under **one absolute
deadline**, re-reading the current generation on each pass; two sequential waits would permit
nearly twice the intended cap. The fourth, shared QoS 0, consults the same two verdicts
without waiting on either and **drops**, as it does for the restore gate. Plain QoS 0
stays ungated: it acknowledges nothing on the group's behalf.

**The disposition follows the two existing precedents rather than inventing a third.** The
FOREIGN outcome disposes — the record is not ours, so this connection may not serve the
group — then notifies and closes, and the packet returns *nothing* to the parser, because
speaking again would race the displacer for the reason the client is left with. Every other
non-owning outcome — a sibling that erased our write, a definite local failure, an
unresolvable ambiguity — does not dispose: the session is left intact.

**Since F-084 the Will is treated the same on ALL of them**, Foreign included, and the
client is told `0x89` on all of them too. This section used to call the Foreign outcome a
"demonstrated takeover" that discarded the Will and notified `0x8E`; it demonstrates no
such thing — a rejected ownership CAS establishes that the subject moved and nothing about
who moved it — and suppressing a Will on it was silent loss. What still separates Foreign
from the rest is the DISPOSAL, not the claim and not the Will.

**Every close here consumes the Will while the ownership gate is still held**, rather
than leaving it to the ordinary close. A record-observation eviction can detach the session
in between, and the detached path publishes only an already-owed *immediate* Will — so a
delayed one would never reach the durable outbox. The Will disposition is consumed exactly
once, so the later close's own attempt is inert; nothing else about that close changes.

**An ambiguous proof is resolved by the packet itself, under the gate it already holds.**
Releasing first would let the periodic pass — which resolves *bound* uncertain sessions —
classify it first and apply the generic policy, which disposes on a forked verdict. A forked
resolution therefore retires the uncertainty *without* disposing: it clears the pending write
and discards the continuations that were owed only if those bytes had landed, leaves the
void-register revision uncredited, and leaves the sequence, lineage and record body at the
acknowledged base, because at forked those bytes demonstrably are not the record. Without
that retirement the close would park a teardown that a later generic disposal consumes,
losing the Will.

## The last-holder scan fails closed on a record it cannot read

The two sections above establish that a group durable is *present and canonical* and that
this broker still *owns* the membership. Neither says when a durable may be **destroyed**,
and that decision has its own instrument: `sharedDurablesHeldByOthers`, the scan five
release paths consult before deleting a group consumer — teardown, a failed shared
SUBSCRIBE's rollback, the 3.1.1 forced leave, the per-consumer release, and UNSUBSCRIBE.

**Its answer authorizes an irreversible deletion**, and `$MQTT_msgs` is interest-retained,
so deleting a group durable destroys the backlog of every member of that group with it.
That is why "no other holder" may only be reported out of a record set the walk read in
full. Three dispositions therefore report **every requested durable held**, which withholds
the deletion: an indeterminate walk (a JS API timeout, a meta-leader failover), a record
whose bytes do not decode, and a record carrying shared group consumers in `Cons` — the
last because holders are read out of `SharedSubs` alone, so that shape hides precisely the
holder whose durable the caller is about to delete.

**The discriminator, so a future walk is classified rather than re-argued.** A walk that
decides by the **absence of a reference across the whole record set** must fail closed on
any record it cannot interpret; a walk that decides only about the **record in front of it**
is already fail closed when it skips one. That is why `buildSessionRefs` and the
orphan-consumer sweep abort, why the last-holder scan reports held, and why
`sweepSessionExpiries` and `adoptAbandonedTeardowns` correctly *skip* — for those two,
skipping performs no action and the record goes on protecting everything it names.

**What this scan deliberately does NOT refuse**, because the temptation is to assume it
inherits the orphan pass's whole set. It does not validate that a record's client ID hashes
back to its subject token, and it does not validate a generation. Its own-record skip is
keyed on the record's `ps.ID`, which is the documented takeover residual recorded below and
in `docs/CONFORMANCE.md`; tightening attribution here would change that residual rather than
this scan's readability defect. The shape check is placed **above** the own-record skip for
the matching reason: that skip presumes an interpretable record, and a same-ID one may
be a
takeover winner's.

**The cost is stated rather than implied away.** A durable spared by a refusal is not
reclaimed by anything else — the orphan-consumer sweep aborts on the very same record — and
the retention is unbounded in count, since one such record spares the durables of every
later group as well. `docs/CONFORMANCE.md` carries it, with the diagnostic naming the
stream, the subject and the sequence so an operator can act.

**And this scan is a read, not an arbitration.** Membership and record ownership are
arbitrated by the session subject's CAS; this walk is a non-snapshot read that merely
authorizes a deletion, and `CONSUMER.DELETE` carries no precondition binding the consumer's
existence to the record. A joiner persists its membership *before* creating the group
durable, which covers the principal leave race but not all of it: a leaver can complete its
scan before the join record lands and delete after the joiner's final verification. Failing
closed on an unreadable record does not close that, and does not claim to.

## The scan answers a DESTRUCTION question, and its answer merges two facts

`sharedDurablesHeldByOthers` authorizes one act: destroying a group consumer. It says nothing
about what the CALLER still owes for its own session, and its single `held` bit answers two
different questions — "some other session holds this durable, or the walk refused" and "THIS
session still reaches it through another of its own shared keys", because the scan marks the
session's own remaining `sharedSubs` before it walks any record. A caller releasing its own
state has to separate them, and a caller deciding a delete does not.

**Three dispositions per group consumer, therefore, not two.** Own-held: discharge nothing
and
delete nothing — the pending purge, the ack-budget refund and the unsubscribe are all
whole-durable, so acting would silently stop a live subscription delivering. A foreign holder,
or a refusal: discharge this session's own state, withhold the delete. Unheld: both. The
overlap that makes this real is deterministic rather than exotic — a multi-level key joins
a
second group consumer for its level-up parent, and that parent IS the consumer of the plain
filter one level up, so `$share/g/foo.>` and `$share/g/foo` share a durable.

**`sess.cons` is not the authority for what a failed filter must release.** `addJSConsToSess`
runs only once a whole filter has succeeded, so the join a FAILING filter made has no resident
config — while its `MaxAckPending` charge, its pending deliveries and its deliver-subject
subscription are all already real. Everything a shared key joins is derived instead from the
key itself, through the one derivation `mqttSharedGroupNamesForKey`, which is also what
`mqttSharedDurablesForKey` and `mqttSharedKeyOwnsDurable` project rather than re-deriving.
The
release then takes `(durable, deliverSubject)`; the resident-config form is a thin wrapper
over
it, and the `held`-gated delete lives in exactly one place, so no refusal can reach a
`CONSUMER.DELETE` by a second route.

**The same keying applies to the pre-save delivery barrier**, and there the ownership test
must
EXCLUDE the key being released: the barrier is lowered before the membership removal, so the
doomed key is still present and counting it would make every closure suppress itself. Two
independent fences protect a live membership from a release it never owed — that
exclusion, and
the rule that a rollback which RESTORES a membership registers no landed release at all.

**The fence is a PROPERTY over the membership that survives the operation, not a rule per
caller.** F-082 fenced two sites and F-083 then found two more by enumerating the
callers, so
the enumeration was the defect rather than the sites. The question is always the same — *does
a membership that SURVIVES this operation still reach this durable?* — and only the membership
it is asked of differs. `mqttSharedSurvivors` is that membership, and every operation supplies
its own as an explicit immutable set of keys:

| Operation | Its surviving membership |
|---|---|
| UNSUBSCRIBE, staging half | the live map **minus the packet's whole doomed set** — the removals have not been applied yet, and one packet can leave BOTH overlapping filters, so excluding only the key in hand lets each protect the other |
| UNSUBSCRIBE, commit half | the live map as it stands: `sess.update` has already applied the removals, so it IS the surviving membership |
| a failed shared filter's release, both halves | the live map; the compensating removal is already durable |
| a SUBSCRIBE packet's rollback, both halves | the **pre-packet snapshot** — a historical value, because the live map still names the very key being rolled back |

Passing the membership is what keeps the last row honest. An exclusion-list API would encode
the UNSUBSCRIBE's ordering rather than the invariant, and would misdescribe a rollback whose
authority is a snapshot. The set is built under `sess.mu` and immutable thereafter; what keeps
it TRUE once the lock is released is the session's **ownership gate**, which every production
route holds. That truth is LOCAL to this session — it says nothing about another broker, and
the non-snapshot join/delete race and the same-client-ID skip are untouched by it.

**Two sites are vacuous rather than exempt**, and the difference matters because only
one of
them owes anything. The 3.1.1 forced leave removes EVERY shared membership and teardown runs
after `sharedSubs` is niled, so no key survives at either and the whole-durable release is
always owed. Teardown carries nothing further.

**The forced leave's own target derivation.** It used to purge over `es.cons` while
voiding over
the membership keys, so the two halves of one operation disagreed about what they were acting
on. It now uses **two authorities, for two different obligations**:

| Obligation | Authority | What only it can see |
|---|---|---|
| the doomed configs, and the CANDIDATES for a consumer release | the membership keys **plus** the `cons` entries | a membership with no config (every record-restored resume), and a config no surviving key names (the `readd` shapes) |
| the pending-state purge | the **pending maps** — `cpending`, `deferred`, `srcAckOwed`, filtered by the shared durable prefix | pending state named by *neither* structural view |

For this caller the two questions F-083 separates — is this config doomed, is this
durable owed
a release — **collapse**, because every membership is going, so nothing local survives
to hold
anything. That is what licenses one candidate set here where UNSUBSCRIBE needs a
surviving-key fence. But a target found **only** through the pending maps is purged and
**never deleted**: pending state is not evidence that this session was the group
durable's last
holder, and only the last-holder scan authorizes a `CONSUMER.DELETE`.

The pending derivation runs at **both** halves, not just the commit. The write that
removes the
membership is the write that must also say "these allocations were never transmitted",
so a
pending-only durable's verdict has to be staged with the rest; left to the commit, a
crash in
between would leave a live register with no cancellation recorded and **no membership
able to
trigger another leave**, and recovery would reserve its identifier indefinitely. `srcAckOwed`
retirements stay exempt from voiding, for the reason `voidRegistrationsLocked` already
gives: a
retirement suppresses a completed exchange rather than claiming an identifier, and
voiding it
would re-offer a completed payload as a QoS 2 duplicate.

This closes F-045's forced-leave instance. It does not alter or re-own the UNSUBSCRIBE
transaction; F-083 already closed the no-`cons` UNSUBSCRIBE instance.

**"This config is doomed" and "this durable is owed a release" are different
questions**, and
the second does not imply the first. A multi-level key's level-up companion has its own
`sess.cons` entry but shares its durable with the plain filter one level up: when the
multi-level key leaves alone, that durable is NOT owed, yet the companion's config must still
go or it outlives the membership with nothing left to reach it by. So an UNSUBSCRIBE's commit
half is three operations — remove the doomed keys' configs unconditionally, compute the owed
targets, then sweep every remaining config naming an owed durable and release it.

**Nothing may discriminate a shared sid from a plain one lexically**, and that includes a
rollback deciding whether a raw subscription predates its packet: both membership snapshots
are consulted, because the two keyspaces are disjoint by construction and a prefix test
misfiles a plain filter that merely looks shared.

`mqttToNATSSubjectConversion` encodes a literal `.` as `//`, so the ordinary plain filter
`$share.foo` has the sid `$share//foo` and satisfies a `$share/` prefix test. The authorities
are `f.shared`, set at parse time, and `cc.DeliverGroup` on a resident config. A plain
UNSUBSCRIBE therefore routes through the same deferred plain-consumer teardown a QoS downgrade
uses; the separate UNSUBSCRIBE-only teardown that used to exist was deleted, having duplicated
that machinery while mutating `sess.cons` without `sess.mu` and never unsubscribing the
deliver subject.

**An ungated `CONSUMER.DELETE` on a shared durable is prevented three times over, and the
three are REDUNDANT WITH EACH OTHER.** The commit half removes the doomed shared configs
before the plain loop runs, so a shared sid reaching that loop finds no config; `f.shared`
dispatches away from it; and the plain accessor refuses a config carrying a `DeliverGroup`.
Removing any ONE changes no observable behaviour, so none of the three is individually pinned
by a test; only removing all three together produces the failure, which is what the paired
mutation demonstrates. That establishes the conjunction and nothing finer — **it does not
license removing any one of them**, and a change that drops one would be invisible to this
suite rather than proven harmless.

## The restore gate: the CONNACK follows a complete restoration

A resuming CONNECT used to acknowledge the session with Session Present = 1 and
only **then** restore the subscriptions its record names. On that path there is no
SUBACK, `sess.update` is skipped, and a per-filter failure only sets a local
result and continues — so the failure reached nobody. The record kept the
membership, no queue subscription and no group consumer existed, and a client that
is not required to re-subscribe waited forever (F-050).

**Restoration therefore runs BEFORE the CONNACK, and a filter it cannot install
refuses the CONNECT** through the same abort discipline the section below
describes. Five exits are reachable while restoring a saved filter — the PUBREL
consumer, the session-affine QoS 2 consumer, the raw subject subscription, the
plain or group JetStream consumer, and the level-up companion of the last two —
and `processSubs` already records each outcome in the filter itself, because on a
SUBSCRIBE those bytes become the SUBACK. `mqttFirstUnrestoredFilter` reads that
value; it is **local failure aggregation and nothing more**. It does not arbitrate
ownership, does not prove this broker still owns the record, does not prove a
group durable exists, and does not prove an installed subscription yields
end-to-end delivery. What it proves is that no step of this restoration reported
failure, which is what the CONNACK is about to claim.

One unrestored filter refuses the whole CONNECT: there is no packet on this path
that could say *which* filter died, which is the finding. A v5 connection is told
the reason (0x83, with the filter named) because that refusal is the first packet
and a filter that can never be restored — a subscribe permission revoked since it
was saved is the reachable case — would otherwise be an endless silent close with
no hint that a clean start is the recovery. A 3.1.1 connection has no reason code
above 5 and gets the bare close. Every other refusal **in the post-accept-save,
pre-success-CONNACK window** now carries a reason too: that generalisation has
landed, and inside that window only the failed success-CONNACK write is still
silent. Exits earlier in the CONNECT remain silent and are a stated residual. See
"The pre-CONNACK refusal signal".

**The reorder needs one new fact, and it is a property of the CONNECTION.**
[MQTT-3.2.0-1] makes the CONNACK the first packet a client sees, and restoration
installs live delivery interest, so something must withhold transmission until the
CONNACK is written. That something cannot be per-subscription, for two structural
reasons: a per-subscription flag cannot be armed before the subscription exists —
`ensurePubRelConsumerSubscription` uses a bare `c.processSub`, whose NATS handler
goes live *before* `processSub` returns, so a stored PUBREL can enter its callback
with no metadata attached at all — and a per-subscription gate that has been
released is indistinguishable from one that never existed, so a refusal cannot
express "woken, and still not allowed out" that way.

So the fact is one atomically-published verdict on the session
(`sess_restore_gate.go`), consulted by every delivery callback **before** it
touches `sub.mqtt`:

- **armed**, unconditionally on every accepted CONNECT — clean start, cleared
  session and zero-filter resume included — before the first subscription the
  CONNECT installs, which is before the reconciliation phase rather than merely
  before restoration. **Both the earliness and the unconditionality are defence in
  depth**, and neither is a proved behaviour: the phase's own subscriptions are
  installed closed behind their own gate, and a mutation making the arm conditional
  on the record naming a filter survives, because a refusal leaves the record's
  filters exactly as it found them so the CONNECT after one always has some. They
  are kept so no future path has to re-derive either argument.
- **opened** after a confirmed success CONNACK — but **not immediately**, and the gap is
  deliberate. `[MQTT-3.2.0-1]` forbids opening it any earlier; what pushes it later is the
  3.1.1 forced leave, which can still refuse the connection after the CONNACK is
  written. An
  opened verdict is *immediate* permission for the delivery callbacks, which run on
  their own
  goroutines, and a transmission cannot be retracted by a refusal that follows it — so the
  verdict opens only once that leave has settled. `opened` is stored strictly before the
  channel closes, so a waiter released by that close reads the final value rather than racing
  it.
- **abandoned** on every refusal, before the client detaches. Two shapes, and the difference
  is the acknowledgement: a **pre-CONNACK** refusal abandons through the abort helper, which
  also discards the Will and clears acceptance; a **post-CONNACK** refusal — only the forced
  leave's settlement arms — calls `abandonRestoreGate` **alone**, because the CONNACK is
  already written and neither the Will ([MQTT-3.1.2-8]) nor the acceptance may be taken back.
  Both shapes owe the wake for the same reason. The verdict is
  **not cleared**: a delivery arriving after the abandon must still be blocked,
  and the abort helper deliberately leaves `c.mqtt.sess` in place, so `sess.c`
  still names this connection. Waking the waiters is what lets a withheld QoS 1/2
  delivery record itself as never transmitted instead of waiting out its whole
  bound — but that recording is **best-effort across a rebind, deliberately**. The
  marker is scoped to the connection that withheld the delivery, and the refusal
  returns and releases the ownership gate, so a reconnect can bind first and leave
  the woken callback stale. Two remedies were built and withdrawn (a drain, which
  cannot be an admission barrier, and a durable-scoped predicate, which removes the
  race but not the lateness); HEAD already treats this boundary the same way. The
  consequence — a lost prompt NAK, and DUP on a first sight — is stated in
  `docs/CONFORMANCE.md`, with the DUP derivation itself belonging to F-072.

Both terminal transitions are CAS-guarded. They are exclusive today; the guard is
what keeps that from being an invariant a later caller can break with a
double close, which panics the process.

The four delivery callbacks, and why they differ:

| Callback | Disposition while the verdict is armed and not opened |
|---|---|
| `mqttDeliverMsgCbQoS12` | **waits**, bounded by the same deadline the reconciliation gate uses; a withheld delivery is recorded — **when the connection that withheld it is still the session's**, which is best-effort across a rebind (above) — so the attempt that finally goes out is a first transmission ([MQTT-3.3.1-1]) and a freed slot can NAK it forward. The recording is shared with the reconciliation phase's own gate through one helper, because both owe the client the same thing, and it is written only for a delivery still OWED (`!closed \|\| phaseOwned`), never for a durable being torn down |
| `mqttDeliverMsgCbQoS0` | **drops.** QoS 0 has no redelivery, so parking a NATS delivery goroutine on an MQTT handshake would buy nothing |
| `mqttDeliverPubRelCb` | **waits**, then disposes by WHY the wait ended. A refusal returns silently, so JetStream redelivers on its own schedule and nothing spins against a session with no client. A TIMEOUT **NAKs**: the bound is min(AckWait, cap), so with AckWait above the cap the wait ends long before any redelivery would, and a PUBREL carries no withheld marker — without the NAK an owed one sits unsent for the rest of that AckWait on a resume that SUCCEEDED. PUBREL carries no DUP ([MQTT-3.6.1-1] fixed-header flags are 0010), so nothing further is owed |
| `mqttDeliverQ2AffineCb` | **waits**, then disposes by why the wait ended, exactly as PUBREL does above: silent on a refusal, **NAK** on a timeout. The NAK matters most for a copy that was materialized and never delivered — absent from the resume's pending snapshot, so nothing else re-drives it. No withheld bookkeeping: the affine path derives DUP from the JetStream delivery count, which is F-072's open defect, and a copy refused here before `trackPublish` is exactly that finding's population. Waiting is what keeps the ordinary case out of it |

Two orderings around it are load-bearing. The **grant point** moves after
restoration, because its own rule is that the requested Session Expiry Interval
and Clean flag are applied only once everything that can refuse the CONNECT has
passed — restoration is now one of those things, and a granted interval of 0 on a
CONNECT that is then refused would have the close path discard a persistent
session. And the **3.1.1 forced leave** stays *after* the CONNACK, so the
pre-CONNACK restore must exclude shared filters for a non-v5 connection
explicitly; the old code achieved that by ordering alone. Provisioning an affine
consumer, a PUBREL consumer and a group join for memberships the forced leave then
removes is what that exclusion prevents.

One thing does move *before* the CONNACK: the leave's memberships are **parsed** there.
A key
`mqttSharedDurablesForKey` cannot expand names no durable, so a leave that met one would remove
the membership and release nothing — and retaining just that key would not help, because
a key
that names no durable cannot make the last-holder scan report anything held. Corrupt or
unsupported persisted state is therefore **refused pre-CONNACK**, where the abort
discards the
Will and leaves the lifetime ungranted, rather than discovered after the acknowledgement.

**What a restore-path failure no longer destroys.** A per-filter failure used to
reach `cleanupFailedSub`, which for a plain filter whose level-up companion failed
after the main half succeeded deleted a durable the session record still names.
That was survivable while the CONNECT was accepted anyway — the next resume's
reconciliation repaired it — and it is the wrong disposition for a refused
CONNECT, whose record still references that durable and whose backlog is retained
by its interest. On the restore path the cleanup is now the connection-local
subscriptions alone. What the connection did add to `tmaxack` is refunded per
filter, from the amount each group join **reported**: the config comes back
whether a call charged it or reused a resident entry, so refunding by durable
would erase an earlier connection's charge and leave the accounting permanently
short.

The reconciliation phase below needs **no** change for any of this, which is worth
stating: its `phaseOwned`/`closed`/`phaseGate` triple keeps its own meaning, its
adoption still clears `closed` and releases its own gate, and
`sweepUnadoptedPhaseSubs` still keys on `phaseOwned` alone. A delivery that wakes
from the phase's gate simply meets the connection's verdict next.

**Pre-CONNACK abort discipline.** Every post-bind failure exit before a
confirmed success CONNACK — the register-recovery error, the reconciliation
refusals, **the restoration refusals above**, the lifetime-grant failure below,
the CONNACK size/write failure
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
snapshot; a 10071 hands the session over — to whoever holds the record, which the
rejection alone does not establish (see "Only a held successor licenses a takeover
claim") — with 10164
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

## The 3.1.1 forced leave is a convergence obligation, not a CONNECT exit

Force-leaving a resuming 3.1.1 client's shared memberships is **broker-side hygiene the client
neither asked for nor can observe**. Its failure therefore owes the client nothing, and the
connection is refused only where refusing is the right answer — never merely because a write
did not succeed. Previously both failure arms returned an error from `mqttProcessConnect`,
which closes: the client held Session Present = 1 and a dead socket, and its Will was published
because the broker could not persist its own bookkeeping.

**The definite bucket cannot contain a transient store failure**, and that fact is what shapes
the whole mechanism. `persistRecord` arms *uncertainty* for every non-CAS-rejection store
error, so an ordinary "JetStream was briefly unavailable" classifies as **ambiguous**, not
definite. What is left definite is exactly: 10071, 10164, and refusals that never reached
JetStream at all.

| Outcome | Disposition |
|---|---|
| landed | commit the leave |
| **ambiguous** | register the landed continuation **first**, then resolve **in line under the CONNECT's own transaction**; continue only on *ours* |
| **10164** (contention) | the bounded re-read/retry classifies it; continue only on *landed*. An unclassifiable outcome parks the continuation **before** returning |
| **10071** (the subject moved) | classify the lineage, dispose with the fork or the ownership-unsettled vocabulary (takeover-or-fork before F-084), refuse |
| any other definite | roll the removal back inside the still-open transaction, and refuse |

Two properties hold the design together. **Continue only on proof**: no arm proceeds
with an
ownership question outstanding, which is why the restore gate can wait for the settlement
rather than needing a second admission barrier. And **one property, not an
enumeration**: the
commit's own register cleanup performs record writes, so rather than threading classified
results back through three helpers, the caller asks the *session* whether anything it did
leaves the connection unable to serve — `recordStateBlocksServing`, one lock-safe
accessor over
"unresolved write **or** armed rejection". That covers writes this code never named, including
ones a later change adds; it does not cover a pre-store refusal, and does not need to, because
none of those is evidence about ownership.

The CONNECT may resolve inside its own transaction where a SUBSCRIBE or UNSUBSCRIBE
defers to
the readLoop, and the difference is structural: `mqttResolveAfterPacket` runs *after*
`mqttProcessConnect` returns, but the resume work inside it needs a resolved session
first. The
CONNECT already resolves once for that reason before its accept transaction is opened; this
ambiguity arises *inside* that transaction, so the in-transaction resolver body is the form
that fits. The old call at this site could never work — it took the ownership gate the CONNECT
already holds and needed a fence the frame already owned, so it reported "unresolved" without
attempting anything.

Every refusing arm owes the restore verdict its terminal, and owes it as
`abandonRestoreGate` **alone**: post-CONNACK there is no licence to discard the Will or clear
acceptance. Waking the withheld deliveries is the point — parked, they wait out their bound,
find the session detached, and can no longer record themselves as never transmitted, so their
redelivery reaches the client marked DUP.

### The pre-CONNACK refusal signal

A v5 client is told **why** its resume was refused, at every refusal in the
**post-accept-save, pre-success-CONNACK window** — which is the scope of this claim, and
exits earlier in the CONNECT are a stated residual in `docs/CONFORMANCE.md`. In that window
the sites are: register recovery, the shared-ownership disposing, unresolved
and
local arms, the **plain QoS 1/2 reconciliation verdicts**, the **lifetime grant**, and the
restore path that started this. Without it a permanently unrestorable session is an endless
silent close, and a clean start — the recovery — is a choice the client can only make if it
knows there is something to recover from.

Two limits are structural rather than chosen. It is **v5 only**: a 3.1.1 CONNACK has no reason
code above 5, so that connection gets a bare close. And the code must be one a CONNACK may
legally carry — `0x89` "Server busy" is permitted and keeps "reconnect and resolve this"
exactly, but `0x8E` "Session taken over" is DISCONNECT-only, so nothing here can tell a client
"stop reconnecting".

**What the client is told is NOT the disposal vocabulary, and that separation is the
mechanism.** The vocabulary's bundled contract — reason code, Reason String, log line, close
state — governs the **DISCONNECT**. The CONNACK is a separate projection, and it has its own
two-value vocabulary (`mqttPreConnackRefusal`):

| Outcome | CONNACK | Reason String |
|---|---|---|
| a body claiming our parent | `0x89` | "session ownership could not be established; try again" |
| any other disposing verdict | `0x83` | "session ownership could not be confirmed" |
| local, unresolved | `0x83` | the step's own diagnostic |

The reason the projection exists rather than forwarding `mqttDisposalTakenOver.reasonStr`
is
that most arms reaching a disposing verdict never established a takeover: the rejection is
also reached when the record could not be read, was absent, did not decode, was released,
or
sat at or below our own base. Sending that text would make this mechanism's whole purpose
false. `preConnackRefusalFor` is exhaustive with no `default`, because a default is exactly
how a new outcome would inherit an ownership claim it has no evidence for; an unlisted
outcome reaches a loud log and a bare close instead.

**One exit stays silent, deliberately**: the **failed success-CONNACK write** has no channel
left to explain itself on — the channel is what failed.

The two sites that used to be silent were silent for a reason about their own code, and both
are closed. `reconcileResumedConsumers` and `grantRequestedLifetime` each **disposed the
session internally before returning**, so a CONNACK the caller enqueued had already lost the
race with the `closeConnection` that disposal schedules; and each collapsed a takeover and
a
fork into one verdict, so the caller could not project the right code even with the ordering
fixed. Both now **report** an outcome and a vocabulary, and one caller-side sequence —
`refuseDisposing` — runs **abort, then signal, then dispose**, in that order, for all three
disposing pre-CONNACK sites (the shared-membership ownership write included). The order is
load-bearing at every step: the abort discards this connection's Will before the close it
schedules can publish it, the signal precedes any scheduled close, and the disposal is last.

**The asymmetry that used to sit here is gone.** The CONNACK still deliberately
**under-claims** — it says ownership could not be confirmed even where a child-shaped body
was observed — and the DISCONNECT and log paths no longer over-claim in the other
direction. F-084 closed that: see "Only a held successor licenses a takeover claim".

The classification behind those two sites is one function, `classifyPreConnackSave`,
shared by
the reconciliation phase and the lifetime grant: both split 10071 from 10164 identically,
and
written twice a new CAS kind would have to be found twice.
`proveSharedMembershipOwnership` deliberately keeps its own copy — unifying that third
consumer is what let an unclassifiable rejection reach F-078's live-SUBSCRIBE Foreign arm
and
change **Will disposition**, which is a different question and F-084's.

## Only a held successor licenses a takeover claim

`0x8E` "session taken over by a new connection" is the one DISCONNECT reason that tells a
client to **stop reconnecting under this session**. The broker used to send it wherever a
record-derived verdict concluded "not ours" — a record that could not be read, was absent,
did not decode, sat at or below our base, or moved to a body that is merely valid-shaped.
`classifyRecordLineage`'s negative verdict says of exactly those cases that *nothing is
established*, and the broker reported the strongest claim it had.

**The rule.** `mqttDisposalTakenOver` and `mqttDiscardWillOnTakeover` are licensed only by
an in-process, causal, same-client-ID replacement in which this broker holds **both** the
connecting client and a **non-nil** displaced client, under the client-ID ownership gate.
That is `discardTakenOverClient`, whose two callers are the ordinary `processConnect`
takeover and the re-drive's T1b. No persisted record shape is positive successor evidence.

The rule is structural rather than asserted: `mqttDisposalTakenOver` has one reader,
`mqttNotifyTakenOver`, which has one caller.

**Why no predicate over the record works.** The obvious candidate is a usable record above
our base whose digest differs, whose `WID` is non-empty and differs from `ackWID`, and
whose `PWID` names `ackWID`. That identifies a **child-shaped body**, not proven ownership:
`createOrRestoreSession` adopts `WID`/`PWID` without validating provenance, valid-shaped
bodies arrive through unconditional publication, and `storeFramedSessionMsg` replays
acknowledged bytes verbatim. So the claim is not narrowed by a better classifier; it is
withdrawn from every route that cannot hold the successor.

**The fourth vocabulary.** `mqttDisposalOwnershipUnsettled` carries `0x89` and
`mqttReasonStrOwnershipUnsettled`, and says what those routes actually know: this broker
may no longer write the record, and nothing established who may. It is a separate value
from `mqttDisposalForked` on the `mqttDisposalTeardownIncomplete` precedent — same code and
text, different name, because the name is read at the call site as the claim being made —
and it **mints nothing**, because a fork grant is a repair anchored to a record proven to
claim our parent.

Both `0x89` vocabularies therefore share a reason code, which makes two things load-bearing.
The fork bit travels **explicitly** from the classification to the mint rather than being
recovered from `voc.reason`; recovering it would mint a grant for every unsettled record.
And a test that asserts only the reason code can no longer tell them apart, so the claim
assertions read the Reason String too.

**The Will follows the licence, not the vocabulary.** A displacement that cannot prove a
successor may not suppress a Will either — that is the same rule the shared-membership
Forked and Local arms already stated ("neither demonstrates another owner, so neither
disposes and neither discards the Will: suppressing it here would be silent loss"), applied
to the classes that were wrongly excluded from it.

Every disposal that goes through the prepared disposition now chooses exactly one typed
`mqttWillAction` and executes it in a second phase. Two rows of the table below are reached
WITHOUT one and are marked as such: the licensed takeover discards through
`discardTakenOverClient`, and the pre-CONNACK refusal discards in
`mqttAbortAcceptedConnect`. A third route, the remote-persist eviction, takes the
record-derived row's actions from its own tail rather than from a disposition. What the
table states is the ACTION each class owes, not the mechanism it reaches it by:

| Disposal class | Live bound Will | Parked deferred action | Reached through |
|---|---|---|---|
| Licensed takeover | discard, keeping the already-owed-now exception | dropped — the takeover owns it | `discardTakenOverClient`, not a disposition |
| Record-derived: unsettled or forked | consume normally | discharge against its ORIGINAL deadline | the prepared disposition, or the eviction tail |
| Teardown-incomplete | consume normally | discharge | the prepared disposition |
| Pre-CONNACK refusal of the ARRIVING connection | discard — no session was granted | n/a | `mqttAbortAcceptedConnect`, not a disposition |

**Phase 1 / phase 2, and why.** `disposeForeignStateLocked` runs the identity check first,
then extracts the stale client and any parked action and chooses the action — all inside
one `sess.mu` critical section, so a competing disposal cannot observe a half-prepared
object and claim the same Will. Nothing irreversible happens there. Phase 2 executes the
action **outside** `sess.mu`, with the caller's record transaction already closed and the
ownership gate still held: the consume does JetStream work, and the discharge would
self-deadlock on a fence its caller holds. A disposition whose identity check refused
carries `mqttWillActionNone` and discharges nothing.

The ordering at every caller **that holds an ownership gate** is `phase 1 → end record
transaction → Will action → release the ownership gate → notify and close`. Inside
`mqttProcessConnect` that is two defers rather than one tail: the notify/close tail is
registered before the gate-release defer and the Will phase after it but before
`defer endAcceptTx()`, so LIFO produces exactly that sequence.

The remote-persist eviction is the one record-derived route where that ordering does not
apply, and its contract is the **inverse**. It decides under `as.mu`, detaches inside that
section, and runs its Will action from a tail that executes once the record token and every
lock are released — so no ownership gate is held there at all, and asserting one would
assert something false. What it shares with the gated callers is the fence: the record
transaction is CLOSED before the action, because both the discharge and the consume do
JetStream work. Two oracles pin the two contracts, and each would fail if given the other's
route.

**The five Will-loss routes this closed.** Four were real losses and one is correct:

- `disposeForeignStateLocked` took the parked deferred action and dropped it.
- A detached client's ordinary close publishes only an **immediate** Will
  (`mqttPublishOwedWill`), so an owed-delayed one was dropped.
- The remote-persist eviction detached, notified and closed while performing **no** Will
  action at all.
- The takeover discard reached classes that had demonstrated no takeover.
- `mqttAbortAcceptedConnect` discards the **arriving** connection's Will. Correct and
  unchanged: no session was granted to it. This is why the rule above is scoped to the
  *displaced* session's Will — on a pre-CONNACK disposing arm both are in play and they
  take opposite actions.

`dischargeDeferredWill` is extracted from `applyDeferredDisconnect`'s lower half and is
Will-only: no clear, no save, no purge, no re-park, and no record fence.
`applyDeferredDisconnect` calls it too, so the discharge exists once. Re-park is
deliberately absent — phase 1 has already deresidented the session, so putting the action
back would strand it where no pass can reach it.

**What this costs, and it is a real cost.** A genuine remote takeover — another broker
really did take the session, and this broker sees its record — is now reported `0x89` and
its old connection's Will is **published** rather than suppressed. The client is invited to
reconnect against permanently unclassifiable state. That is accepted deliberately: this
broker cannot tell that case apart from a forged, replayed, unrelated or malformed record,
and telling a live client to stop reconnecting on evidence that establishes nothing is
worse. Stated in `docs/CONFORMANCE.md`.

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
- **The drain's release is fenced against its own session being cleared, twice.** The
  finalizer captures its drain and sequence, unlocks, and does JetStream work; `clear()`
  meanwhile nils the drain maps, RE-MINTS the generation, and resets the packet-identifier
  allocator. So the incarnation identity is captured in the same critical section that
  proves the captured drain is still the session's — without that, a stale finalizer pairs
  the retired delivery's identifier with the fresh generation, and since the release matches
  on the subject's identifier plus `(Dur, Seq)` with group durables named deterministically,
  the FRESH incarnation's register is a full match and is deleted while its client may still
  hold that identifier. A **second** check runs after the release and after the record
  fence, because `clear()` can also finish during the release's own round trips, after which
  the fence opens legitimately against the new epoch while every local still names the
  retired drain: the token is released and the delivery withheld rather than undone, since
  the release itself is irreversible. Neither check arbitrates a genuinely LIVE owner of the
  same register — that stays F-036's predicate.
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
  subscription restoration, which runs later in the CONNECT than register recovery (and,
  since F-050, still before the CONNACK) — while register recovery has already rebuilt
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
- **A parked close un-jails its client ID only where it still owns the session, and takes
  `as.mu` to do it.** The flappers jail (`as.flappers`) is `as.mu` state end to end: CONNECT
  reads it, every displacement writes it, and the account manager's own cleanup timer sweeps
  it. A close whose record write is unresolved parks and then clears its client ID from the
  jail exactly as an ordinary teardown does — but it reaches the park by routes with
  different lock states, and only one of them can say what the entry it would delete IS.
  With the ownership gate held (the `mqttDeferDisconnectOwned` routes; the never-expiring
  one parks only after ending its own record transaction, so it holds no fence either)
  **neither producer can run**: `evictDecideOnce` refuses outright under a held gate, and
  `discardTakenOverClient` sits downstream of a CONNECT that would need the same gate. Any
  entry present is therefore a STALE one from an earlier displacement, and removing it is
  the ordinary teardown's behaviour. On the gate-less route nothing of the sort can be
  proven — `lockSessionForTeardown` releases `as.mu` between attempts and samples ownership
  in a separate critical section, so the holder it lost to can release and let
  `evictDecideOnce` jail this client ID in the interval before the park — so
  `mqttDeferDisconnectUngated` leaves the jail alone. The two entry points exist so the
  precondition is visible at the call: one boolean silently carrying both "must still be
  uncertain" and "may delete a jail entry" is what would let a future ungated caller
  compile into a wrong deletion. Deleting there would cancel a live displacement's jail;
  leaving it
  costs at most one `mqttSessJailDur` of reconnect *enforcement* on an entry that may
  have been stale — CONNECT compares the stored timestamp against that duration, and the
  cleanup timer runs on its own longer interval, so an expired entry can sit in the map
  well past the window in which it jails anything — which is the fail-closed direction.
  The lock is taken at the park rather than
  inherited (the gated callers hold the session gate, not `as.mu`) and taken BARE —
  `sess.mu` is released inside the park's own critical section — so it adds no edge to the
  `as.mu` → `sess.mu` order, and the section holds one map delete and waits on nothing, so
  it has no outward edge that could close a cycle with the gate, the record fence or `c.mu`.
  No `docs/CONFORMANCE.md` entry: the jail is an inherited anti-flap heuristic, not an MQTT
  surface.
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
  is deliberately NOT parked — a parked Will is memory-only, so a CRASH in that window drops
  it, regressing a Will the broker delivers today — so the close falls through to
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
  memory-only, so a CRASH in that window drops it. **A remote takeover no longer does**: as
  this was written, a record-observation eviction consumed a parked action and discarded it;
  since F-084 that route EXTRACTS the action and discharges its Will against the original
  deadline (`dischargeDeferredWill`), so the surviving loss is the crash window alone.
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
- **A retargeted CAS rebuilds from the record it just read.** Several writers of
  `$MQTT_inflight` build a body once and, when their CAS is rejected, re-read the subject
  only to learn the SEQUENCE — then re-target with the original body, silently erasing
  whatever a concurrent writer proved in between. The alpha-blocking instance was the
  resume claim contest re-proposing a pre-retirement snapshot over a `Retired` mark, which
  puts a completed exchange back in the live bucket and re-sends its payload after the
  client's PUBCOMP. The rule is one rule over the whole writer set: a re-targeted CAS
  starts from the reread and mutates only the fields the operation OWNS. Monotonic facts
  (`Retired`, `Cancelled`) merge forward; identity facts (nonce, identifier, source, QoS,
  the affine pair) never change under one exchange — the same-delivery takeover replaces
  them as a SET, which is a new exchange identity taking the slot rather than a mutation
  of the old one; and the arbitration fields (`Claim`/`ClaimSeq`) belong to the contest
  alone. A body proposed at the caller's OWN cached sequence is exempt and stays sparse:
  the CAS is itself the proof, because any intervening writer moved the sequence. What
  those writers may not do is re-propose that body after a reread.
- **`Retired` dominates `Cancelled`.** Rebuilding makes the two bits coexist durably for
  the first time, whenever a cancellation races a retirement. One precedence rule decides
  the shape at every reader — recovery's classification, the contest's close arm, the
  store's tombstone reap, and the affine sweep's settle gate: a retirement is backed by
  the client's own PUBREC, so the transmission provably happened, while a cancellation
  over a transmitted delivery is F-088's known-false write. Freeing an identifier the
  client holds is the catastrophic direction; holding one that was actually free costs a
  slot. So a record carrying both is never consumed as a close and never frees its
  identifier: it reinstates the suppression, exactly as a pure retirement does.
- **A register delete whose caller can name the NONCE and reach the VERDICT routes
  through one helper.**
  Three separate reviews each found one more delete site that stranded a cancellation
  verdict, which is the signal that enumerating sites is itself the defect. The boundary
  is a decision procedure rather than a list: a caller that holds or can recover the
  exchange nonce owes the verdict a proof, so its delete is confirmed and the verdict
  released against that proof, while a caller whose own semantics already require
  confirmation keeps it. THREE shapes are outside it: a site that cannot name a nonce at
  all; a site that runs the SAME discipline through a stronger ladder of its own —
  confirmed delete, else a tombstone on the register, else the nonce verdict — which is
  the full version of what the helper does in one step rather than a bypass; and a site
  that can name the nonce but cannot REACH the verdict, which lives on a session. The
  orphan sweep's affine arm is the third: with no identity-fenced session resident it
  retains a register that CARRIES a cancellation, but a verdict held only in that
  session's persisted `CancelledRegs` is invisible to a walk that reads registers rather
  than records, so such a register is still deleted and its verdict stranded — F-090,
  filed, pre-existing, and not an alpha blocker. An AST guard parses every production
  file in the package and fails on any register delete outside that allow-list, so a new
  site cannot silently rejoin the stranding class. The forget's persistence stays
  opportunistic, riding the existing
  revision machinery — the same standard, and the same one-nonce crash window, the
  committed recovery path already accepts.
- **A sequence is applied to a live entry by LINEAGE, not by base.** A resume's operation
  may legitimately re-target through several sequences, so it carries the chain it
  authoritatively held the record at, and a live entry's register sequence is advanced to
  the chain's end only when it names a link in that chain. Admitting only the base leaves
  an entry aimed at a sequence that no longer exists when memory sits at an intermediate
  link; admitting anything overwrites a fact this operation never saw. A record nothing
  contested carries a one-element chain, which makes the rule exactly "unchanged since
  the walk".
- **A same-delivery takeover disposes of the occupant by what it IS.** When an
  allocation's create-only CAS meets a register naming the same source under another
  writer's nonce, four dispositions exhaust the cases and each is the only convergent
  one. A RETIRED occupant is adopted, not replaced: it proves this identifier's exchange
  for this very source is past PUBREC, so the retirement is reinstated (behind a
  generation fence, since a discard may have re-minted the incarnation while the
  reconcile was outside the lock) and this allocation settles through the post-CAS ladder
  without transmitting. A pure CANCELLED occupant is reaped and the create retried, which
  is why that arm is ordered ABOVE the takeover: our allocation IS the fresh first
  transmission the tombstone's freeing licensed. A LIVE occupant that is ours to rewrite —
  any plain durable, or a shared one whose claim equals ours — is replaced at its exact
  sequence. A live occupant that is NOT ours is QUARANTINED in place.
- **A quarantined identifier is held, not abandoned.** Abandoning it removes the source's
  `cpending` mapping, so the next AckWait offer allocates a DIFFERENT identifier and
  transmits the same message while the rival's register stands — two identifiers for one
  message, which is the deviation this whole mechanism exists to remove. Held, the entry
  becomes a foreign reservation that no terminal path may settle, the mapping stays, and
  the contest is recorded against the SOURCE rather than the identifier — a consumer
  purge unlinks the mapping and a re-subscribe re-offers the same delivery, so a
  pi-keyed record would be invisible at exactly the moment it has to decline.
- **The quarantine has exactly two removers, and neither is live.** They are the next
  SUCCESSFUL resume's final-disposition reconciliation and session-generation retirement.
  A live release would have to settle a durable contest from inside a live session, and
  both shapes the register vocabulary offers for "this identifier is settled" are
  consumable by a rival's SUPPORTED lifecycle: an observed absence by a late create-only
  CAS, which R1's own late-write rule permits, and a cancellation tombstone by the
  rival's reap-and-retry, which confirmed-deletes a pure `Cancelled` occupant and retries
  its create. A fence any writer may lawfully consume fences nothing. The cost — one
  identifier and one Receive Maximum slot held for the rest of the connection — is a
  stated residual in `docs/CONFORMANCE.md`, and it is conservative under-utilization
  rather than an overrun.
- **The resume's release is STAGED by the walk and COMMITTED once the resume succeeds.**
  Recovery runs long before the success CONNACK, and every refusal after it — the CONNACK
  write, the lifetime grant, subscription restoration, consumer reconciliation, the 3.1.1
  forced leave — preserves the session object and its maps. Releasing at the walk would
  therefore free an identifier on a resume that never completed. So the reconciliation
  appends to a FRAME-LOCAL list and the commit runs past every refusal return, immediately
  before the restore verdict opens, re-fencing each release on the exact tuple its walk
  recorded. A fence miss keeps the quarantine, which converges: the next resume re-observes
  and re-stages. The list is frame-local rather than session state because session-resident
  staging would let an aborted resume's decision be committed by a later resume whose own
  walk never observed the absence. Two identity rules govern which disposition a
  quarantine may consume at all. The index is keyed by the SOURCE, and a source is not
  an identifier — a delivery can legitimately have been re-offered under a second
  identifier after the
  first was consumed — so evidence that does not name THIS identifier decides nothing
  about it. And an arm that leaves the identifier HELD (a live entry, or a PUBREL
  reservation on the same identifier) applies at once, while every arm that would FREE it
  stages, a retirement included: the client completed that exchange, so the identifier is
  genuinely free, but freeing it is exactly what may not happen before the resume
  succeeds. A quarantine installed while the walk was running is
  likewise kept, and that eligibility is judged BEFORE any disposition is consulted: the
  walk may well have ROUTED that source — from the earlier register it read — and that
  result describes a contest this quarantine is not part of, so applying it would dispose
  of the quarantine on evidence predating it and free the identifier while the rival
  register still stands.
- **A transmission holds its identifier until it is no longer owed.** `queueOutbound`
  is a synchronous socket write with no deadline, so between the tracking decision and
  its return a teardown, a subscribe denial, a recovery apply, a terminal
  acknowledgement or a session discard could free the identifier and the allocator could
  re-mint it — two packets under one identifier, in order, on one socket. Every
  JETSTREAM-BACKED QoS 1/2 **PUBLISH** transmission and every **PUBREL** transmission
  therefore runs inside a SEND SECTION, and a **retained** QoS 1/2 message — which has no
  section, because it allocates in `serializeRetainedMsgsForSub`, frames itself, and is
  written by a later `queueOutbound` — carries an equivalent reservation in the same
  registry instead (F-091, below).
  The authority is a session-level registry of identifier-to-hold rather than the entry:
  a successful PUBREC replaces the PUBLISH entry with a PUBREL one while the write may
  still be blocked, and a PUBCOMP untracks the PUBREL entry while a duplicate copy is
  still unanswered, so per-entry state alone would strand the identifier. The registry
  deliberately survives a session discard — `clear()` resets the identifier counter to
  zero while the displaced connection's write is still in the socket.
- **A hold has THREE release conditions, and PRESENCE IN THE REGISTRY IS NOT A
  RESERVATION.** A *write that has not returned* is the only fact available on the
  PUBLISH side, because at that point no acknowledgement has been asked for yet. On the
  PUBREL side the real event is available and it is the one [MQTT-2.3.1-4] names: the
  identifier is free once the corresponding acknowledgement has been processed, which
  for a PUBREL is its PUBCOMP. So a hold also counts the PUBRELs transmitted under that
  identifier whose PUBCOMP has not arrived, and releasing on the write's return alone
  leaves the *second-order* case open — the client answers the duplicate later, and that
  stray PUBCOMP acknowledges away a re-minted exchange's PUBREL record, destroying the
  evidence that it is past PUBREC (F-089, the same severity class as F-088).
  The two halves differ across a connection change, which is why one hold carries both:
  the write drains at `endSend` on the transmitting goroutine and survives every
  transition, while a PUBCOMP is only ever processed on the session's CURRENT connection,
  so a debt owed to any other connection can never be answered.
- **A retained send's reservation is the third condition, and it is fenced by the session
  INCARNATION rather than by the write.** A retained QoS 1/2 message has no JetStream
  backing, so it has no register (R6) and never had a send section: it allocates in
  `serializeRetainedMsgsForSub`, is buffered on `sub.mqtt.prm`, and is written by a later
  flush — the SUBACK tail's, or the first live delivery to that subscription. Across that
  span its only other reservation is the `pendingPublish` entry, and `clear()` destroys
  exactly that while resetting the counter, so the identifier was handed straight back out
  while the displaced connection's write was still in the socket. The reservation is
  therefore taken in the same `sess.mu` acquisition as the allocation — under
  `asm.sessLocked[sess.id]`, so no successor can rebind between the two — and each buffered
  frame carries the identifier it was framed under.
  **Written completion ends it only while the allocating generation is still current.**
  Then `pendingPublish` is still there to carry custody on to the PUBACK, or a PUBACK for
  this very frame removed it and the identifier is genuinely free. Past a `clear()` neither
  is true, so the write returning does NOT free it: releasing there put a second PUBLISH
  under that identifier on the same socket, because `clear()` does not rebind `sess.c` and
  the still-bound connection's own delivery callbacks pass every gate. `sess.gen` is the
  fence because `clear()` re-mints it and an acknowledgement does not, and it never errs
  toward releasing — no path nils `pendingPublish` without re-minting.
  **A subscription leaving `c.subs` must not take an owed retained frame with it**, and the
  two dispositions differ. Its frames are detached while the connection lock is held, and
  then either TRANSFERRED to a reachable replacement or ABANDONED with their reservations
  released. Abandonment is UNFENCED — no bytes reached the client, so there is nothing for
  a re-minted identifier to collide with, and holding on would reserve the identifier against
  reallocation for the session's life while nothing will ever write the frame.
  **The same-sid replacement TRANSFERS rather than abandons**, and that distinction is
  load-bearing: a v5 SUBSCRIBE may legitimately carry one filter twice with the first entry
  asking for the replay, so releasing the displaced frames would silently discard a retained
  delivery the client is owed. Moving them onto the replacement — the entry every later flush
  and every later teardown finds — also keeps the replacement out of the reservation
  vocabulary entirely: the ordinary written and abandoned paths dispose of the supplemental
  RESERVATION. Neither releases the identifier itself — the `pendingPublish` entry goes on
  reserving that until the client acknowledges, or until the session's next resume drops an
  abandoned frame's entry.
  The three direct mutation sites are `processUnsub`'s delete, `processSub`'s replacement
  and `closeConnection`'s map swap; the last is for totality rather than correctness,
  since `mqttHandleClosedClient` nils `sess.c` first.
  A structural test holds those three to a frozen predicate — the mutation must be inside
  the connection lock with an unconsumed detach between the lock and the mutation. That test
  is a bounded regression tripwire, deliberately not a proof: it recognises only direct
  mutations through a signature-declared `*client` identifier, and does not resolve local
  aliases or helper-mediated mutation. The invariant above is universal; only the test's
  reach is bounded, and that boundary is fixed rather than open — see its res-judicata note.
  Once ownership moves the term is inert, and soundly so: no additional identifier-bearing
  PUBLISH *under that identifier* can reach the orphan's socket, because every route that
  could frame one opens a reservation refused on `sess.c != cc`.
  **PUBREL admission is deliberately UNCHANGED by this mechanism.** `beginPubRelSendLocked`
  refuses an active transmission token, as it always did, and NOT a retained-only hold —
  so a PUBREL token and PUBCOMP debt may sit beside a retained term on one hold, which is
  coherent because the owner matches and both `endSend` and `consumePubRelAckLocked` ask
  the shared predicate, so each term is discharged only by its own event. Three refusals
  were tried here and all three refuted: a retained-term refusal blocks the retained QoS 2
  exchange's OWN PUBREL (the flusher holds the term live until after the triggering live
  PUBLISH is written, so the record waited out AckWait); a refusal on any live
  `pendingPublish` entry breaks the UNSUBSCRIBE drain, which legitimately recreates a
  correlated PUBREL under a recovered entry's identifier; and one on a source-less entry
  starves an abandoned frame's record for the connection's life. The stale-PUBREL
  multiplexing hazard is owned wholly by F-092, whose fence correlates the record against
  the entry holding the identifier — a decision only the delivery callback can make. A
  successful PUBREC is ordinary successor custody: it replaces the PUBLISH entry with a
  `pendingPubRel` one, and the retained term is untouched by it. That is deliberately not
  the stronger "every transmission passes a send gate" — QoS 0 has no gate — nor "no
  additional identifier-bearing PUBLISH at all", since a multi-frame retained buffer holds
  several, each under its own identifier.
- **The acknowledgement debt is OWNED by a connection identity and read through
  `sess.c`, not dropped at named transition sites.** The identity is the connection's
  `cid`, a monotonic never-reused scalar — deliberately not a client pointer, because an
  inert hold outlives its connection by design and a pointer would keep the departed
  client's whole object graph reachable from a resident session for as long as that
  session lives, which for a never-expiring one is forever. A ZERO identity is refused
  rather than recorded, so no hold can carry one: zero means uninitialized, and two
  uninitialized connections would be indistinguishable to every ownership comparison.
  The sites cannot be enumerated safely: `clear()`
  never assigns `sess.c` at all, a persistent-session disconnect nils `sess.c` and never
  calls `clear()`, and the clean-start path runs `clear()` while the displaced client is
  still bound. A debt dropped at `clear()` would therefore leak forever on a
  never-expiring session and free an identifier the client still holds on a clean start.
  Read through `sess.c` instead, a debt stops reserving at exactly the moment its
  PUBCOMP stops being processable, at any of the seven `sess.c` writers. A retransmission
  on the next connection RESETS a dead predecessor's count rather than adding to it, or
  the one PUBCOMP the client owes could never clear a count of two.
  Because presence is not a reservation, an *inert* hold — no write out, no debt of ours
  — reserves nothing, and a reader that met one and refused would strand the identifier,
  its register and a Receive Maximum slot for the session's life. Such a hold is TAKEN
  OVER, never refused, and dropping it loses nothing precisely because it reserved
  nothing. The sweep is bounded-memory hygiene on that basis and nothing rests on it; it
  runs at `clear()` and again immediately after the CONNECT rebind assigns `sess.c`, which
  is the only point a hold's owner changes and therefore the only point the predicate turns
  a predecessor's owner-scoped term inert. A live write token is owner-independent and
  survives both. That is not the "dropped at a named transition site" the debt rule
  forbids: the rule is about a LIVE debt, and the sweep deletes only what the predicate has
  already proved reserves nothing. The rule is enforced as an ownership boundary rather
  than a habit: the registry is touched only by the liveness predicate and the helpers that
  mutate a hold, and every other reader asks `sendReservedLocked`.
  Adding a term to that predicate is NOT additive — it changes what every reader and
  mutator means. The ones that ask the predicate adapt for free; the ones that re-spell the
  condition do not — and when the retained term arrived, `consumePubRelAckLocked` was the
  one that had to change, switching to the shared predicate so a PUBCOMP could no longer
  delete a hold carrying a live retained reservation. `beginPubRelSendLocked` was given a
  retained-term refusal too and it was DELETED again: see the PUBREL-admission paragraph
  above for why, and do not reinstate it.
- **A removal path that meets an active section CONDEMNS the entry** instead of deleting
  it, parking the continuation it owes; the first parked disposition wins, because every
  continuation is idempotent against another remover's durable work. Terminal
  acknowledgements park their identifier-freeing half (their protocol answer is
  unaffected), while a successful PUBREC needs no transfer machinery at all, the registry
  holding the identifier however the entries move. A PUBCOMP parks nothing and defers
  nothing: a PUBREL has no in-flight register — the identifier's durable authority moved
  to the PUBREL record at the PUBREC handoff (R2) and the register was deleted there — so
  the only durable act it owes is acknowledging that record, and doing so while a
  duplicate write is out is both safe and preferable, since the client has answered and
  the ack stops further redeliveries.

A session discard removes the retiring incarnation's registers with a **bounded reap**
rather than arbitrating each one as the inbound hold walk does: on the discarding broker
nothing is competing for a register — no claim record, no marker — though a resuming
session does read one as durable authority and rewrite it to take ownership. Its
enumeration filter names a generation `clear()` has already re-minted, so it cannot reach
the next incarnation. That is the extent of the claim. A resume preserves the generation,
so a peer may still be serving this incarnation; whether removing a register costs anything
there is `$MQTT_inflight`'s own consequence question, and **F-034 answered it on
2026-08-22**.

Until F-034 this was a blind FILTER PURGE, and that is why the remedy `$MQTT_qos2in` had
already received could not simply be copied: **F-062** bounded every hold purge to a proven
cutoff, and a filter purge enumerates nothing, so it has no watermark to be bounded by.
Giving `$MQTT_inflight` one was F-034's work and is done — `reapIncarnationRegisters`
enumerates, loads, classifies and deletes only at or below the sequence its own enumeration
returned, and only a record whose identity and subject agree. **No filter purge of this
stream remains anywhere in the broker**, which is also why `DenyPurge` moved from a fatal
prerequisite to a waiver in the table above.

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
find its own PUBRELs, QoS 2 holds and Will. The invariant this buys is exactly one
thing: a late delete from a retired incarnation cannot name a FRESH incarnation's
resources. It does not establish that no live resource exists under the retiring
generation — a resume preserves the generation, so a peer may still be serving it. Where a
late removal had to be made safe against that peer, the bound came from the removal rather
than the name: on `$MQTT_qos2in` every hold purge carries the highest sequence its remover
could prove it was entitled to destroy (F-062). What that premise costs on
`$MQTT_inflight` was audited by F-034 (2026-08-22), and the answer is the same in kind: the
teardown's register pass carries the watermark its own enumeration returned, and conditions
each delete on the identity it loaded at the sequence it loaded.

This is the same conclusion the QoS 2 move protocol and the affine delivery path
reached independently, and it is why the fix is naming rather than a lock: a nonce
re-check before each delete is a check-then-act, and no in-process lock arbitrates
anything against another broker.

**The teardown claim** (`internal/mqtt/sess_teardown.go`) covers what naming
cannot: two brokers tearing down and resuming the same record at once. Before
deleting anything, a discard CASes a `Teardown{Origin, Nonce, StartedAt}` marker
onto the record at the sequence it resolved against, preserving the record's
content so any broker can finish the job and so the orphan sweep still reads its
durables as referenced. Losing that CAS means the claim is not ours, and the loser
deletes **nothing** — a complete outcome, not a failure. Releasing is a
sequence-targeted delete whose result is classified by **re-reading**, because
`MaxMsgsPer: 1` may already have removed that sequence on a peer's behalf: absent
means done, our own nonce means retry, and a foreign nonce or no marker means the
release is not ours to make. The release is always confirmed even on the no-wait discard
path, since an unclassifiable release is not a release.

Nobody resumes a marked record. Every CONNECT that can interpret the record (and the
record-driven expiry path) **finishes** the teardown when it finds a marker —
repeating the generation-scoped deletions and releasing the record — and
only then starts a fresh session with a fresh generation and Session Present false.
Repetition is safe for names nothing can re-occupy. The inbound QoS 2 hold walk purges by
subject, so a repeat can name a subject re-occupied since: what makes it safe is the bound
each call carries, per walk, which naming alone never gave — see **F-062** for what that
bounds and what it deliberately does not. It is **not** claimed for the deletion set as a
whole on naming alone — but the other unbounded member is gone: the same teardown used to
purge `$MQTT_inflight` by filter with no bound at all, and **F-034** replaced that with an
enumerate-load-classify pass carrying its own watermark.
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
  consumers and its marker persist until an operator resets the nine-stream `$MQTT_*`
  bundle.
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
record of the retirement, and the marker lives on the record's own subject. An
UNCONDITIONAL foreign write can land after it did — a CAS excludes a competing
conditional write off that base but not one carrying no expectation (see the
same-base boundary in `docs/CONFORMANCE.md`) — and `MaxMsgsPer: 1` then **evicts**
it. What survives is a record naming
exactly the resources the winner deleted, with no marker for an adopter or the sweep
to find: a **hollow survivor** that a reconnect resumes with Session Present 1 for
consumers, holds and in-flight registers that no longer exist.

The fix keeps the retirement fact **off the replaceable subject**. Every teardown that
wins its claim first records a permanent per-client watermark on `$MQTT_teardown`
(`{id, gen, mseq, nonce, origin, started}`, one subject per client,
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

**The watermark carried an `rwid` naming the record the deletion removed, and that field
was REMOVED** by `docs/ROADMAP.md`'s vanished-record item, which is the box that decided
its fate. The reasoning is the item's product and is recorded here so it is not
re-derived:

A record's write identity names its BYTES, not one OCCURRENCE of them. `storeFramedSessionMsg`
replays acknowledged bytes verbatim and the readback adopts the new, higher sequence with
the original `WID`; restore adopts `WID`/`PWID` from record JSON without validating
provenance. So one identity can stand at two sequences, and a reader asking "did a
teardown retire the record I hold?" cannot tell which occurrence a retirement removed.
`mseq` bounds the removed occurrence below the marker but does not say WHICH occurrence it
was, so no conjunction of surviving fields is occurrence-exact either. The field therefore
never acquired the reader it was retained for, and could not.

**What an exact reader would need instead**, if one is ever wanted: the claimed
PREDECESSOR SEQUENCE, stored on the marker where `claimTeardown` knows it and copied into
the watermark, compared for equality with the reader's own `sess.seq`. That is two new
required fields on two permanent shapes; the route is analyzable — `$MQTT_teardown` is
already epoch-coupled to `$MQTT_sess` — and it was refused as DISPROPORTIONATE to what it
buys, which was a more specific DISCONNECT Reason String. Disproportionate, not blocked.

**What the field also did, and what replaced it.** Being required, it made a teardown of a
record carrying no `WID` **fail closed** with the record left marked, rather than retiring
a record on a name nothing can attribute. Nothing else in the teardown path checks `WID`
or `PWID`, so that refusal is now an explicit precondition in `recordGenRetirement`,
checked before the watermark is built and before any deletion.

Three readers consume the fence — a fourth, the record-absent create, was deleted with
the create-parent inheritance and now reads it only to refuse a corrupt one. Each applies
the same validation
(`mqttValidateGenRetirement`: decodable, the client ID hashes back to the subject
token, a valid generation, a nonzero `mseq`) with the same
fail-closed disposition — a watermark that is unreadable or malformed in any field is
never repaired in place, because it may name a retirement newer than the reader's own:

- **The winner's release-superseded arm.** The release now hands back the record it
  classified against; when that record carries the generation just retired and no
  marker, it is the hollow survivor and is condemned with a fresh teardown claim. A
  record carrying a DIFFERENT generation is left alone — outside this retirement's
  reach, which is a statement about the generation and not about who holds it.
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
- **The record-absent CREATE**, which is the only non-destructive reader and now reads
  the watermark for ONE outcome: **corrupt REFUSES the CONNECT**, because admitting a
  session over a watermark this broker cannot read leaves its own later teardown unable
  to record a retirement — `recordGenRetirement` refuses to replace a corrupt standing
  watermark — so the record would stay marked for every future finisher. **Valid, absent
  and indeterminate all ADMIT, and none of them supplies a parent**: the create is
  parentless whatever the watermark says.

  It used to take `rwid` from the valid arm as the identity its first write descended
  from, which is what the write-lineage audit deleted; the field itself has since been
  removed, so there is no longer any record identity here to inherit. A valid watermark is
  still read on this arm and deliberately draws nothing from it — an inheritance restored
  here would reintroduce the false-fork routes `docs/CONFORMANCE.md` records, not close a
  gap.
  The read's PLACEMENT after the proof of absence is likewise no longer load-bearing for
  ancestry; what remains of it is that the refusal decides on a watermark at least as new
  as the deletion that emptied the subject.

Two things a marker means constrain the three DESTRUCTIVE readers: it licenses its
adopters to delete every durable the record names, and the CONNECT discard deletes them
itself. So each
of them validates the record with `mqttValidateTeardownTargets` before it acts — the
CONNECT *refuses* rather than discarding — and a record whose names are not this
session's is condemned by nobody. The create-side reader names no target and deletes
nothing, so it has none to validate.

Nothing ever reaps a watermark. Storage is bounded by the client-ID population, and
permanence is what keeps the residual from being widened by a GC choice. Because
`mseq` is a `$MQTT_sess` sequence, `$MQTT_teardown` is only comparable within the
incarnation that issued those sequences — which is now a property of the whole
bundle rather than of this pair: every broker-owned stream carries the account's
recovery epoch and a mismatch is **refused at startup**, and no stream may be
recreated without the other eight. See "The nine streams are one recreation unit".

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

## Orphan reconciliation

A broker that dies in the wrong window leaves durable state nothing else ever
revisits: an inbound QoS 2 hold and its release claim on `$MQTT_qos2in`, a
assignment claim on
`$MQTT_qos2assign`, an orphaned `$MQTT_PUBREL_*` consumer, and the teardown marker that
pins them in place. A session-affine `$MQTT_Q2AFF_*` consumer on `$MQTT_out` accumulates
too and is deliberately **not** converged — see the affine subsection below. None of
them loses a message BY ACCUMULATING — a stalled claim is honored, never released (nor is
a stale `discarded` marker among them a counterexample any longer: it records the fence its
writer was entitled to destroy, so a later exchange above that fence reaps it and proceeds
— F-062 mechanism 2) — but all of them accumulate. A periodic per-account sweep converges
them
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

**Absence is established by a complete walk, never inferred from a partial one — and
that is a narrower claim than "proven".** Candidates are listed *before* a complete
walk of `$MQTT_sess`, so a resource created between the two is never judged
against a snapshot that predates it; local in-memory sessions count as references,
covering the create-before-save window on this broker. What the walk plus this broker's
residents cannot observe is a session live on **another** broker whose record is absent —
a resume preserves the generation, so such a session still uses these names. That is
**F-036**'s open false-absence inference; the per-resource-class consequences belong to
F-034 — whose audit closed on 2026-08-22, routing its own live-owner residue back to F-036
as that finding's fourth manifestation — and to F-036, rather than to this section.
(`$MQTT_qos2assign`'s class was F-033, whose
audit closed on 2026-08-18. What it found in this pass is one defect of ours — a live
generation reference makes this pass skip every claim of that generation, leaving them to
an
owner resume that may never come, which is F-064. (F-036 also gained an addition here —
the affine consumer delete was BY NAME and a resuming session ADOPTS the consumer it
finds, leaving the instance fingerprint unchanged so it discriminated nothing. That is
HISTORICAL: course E removed the delete, and the population it created is gone with it.)
**F-064's accumulation
half has since narrowed the skip**: under a live reference the pass now reaps claims that
are already durably TERMINAL, and collects the retired affine `$MQTT_inflight` register
class with the copy and claim each names. The skip still protects every NONTERMINAL claim
of a live generation, and that is a proof rather than caution — no fact such a pass could
write is both true and terminal for that population, and writing `Discarded` there makes
a concurrent materializer withdraw an unpublished copy for a session that never ended.
See the F-064 bullet in `docs/ROADMAP.md` for the worked interleaving.) The same
bound is stated once for
the code, in `internal/mqtt/orphan_qos2_sweep.go`'s header. Ownership is the
`(client-ID hash, generation)` pair carried in the subject or durable name, never
the hash alone. An incomplete walk, an undecodable record, a client ID that does
not hash back to its subject token, or an unvalidatable generation aborts the
whole destructive pass.

**Two grace policies, and neither is an age.** `mqttOrphanSweepGrace` (five
minutes) covers construction windows — a resource created just before the record
save that references it — and applies to teardown markers, holds, assignments and
PUBREL consumers. `mqttQoS2ReleaseOrphanGrace` (one hour) applies to release
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
confirming it gone. (A releaser reaps one only where it can prove the marker never fenced
its exchange; that condition and its evidence are F-062's.)
A releaser that preloaded the payload and resumes afterwards
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
ones are fully derived — and adoption checks all four. A **PUBREL** consumer is
additionally re-looked-up immediately before deletion and must still be the same
instance quarantine observed: one deleted and recreated under the same name is a
different consumer, and deleting it by name would destroy a peer's fresh consumer
inside the very create-before-save window the wait exists to protect. The affine
consumer had the same re-lookup, and it was not sufficient — an adoption preserves
`Created` — which is part of why course E removed that delete rather than hardening it.

**PUBREL durables are reaped, and they are the only consumer class this pass reclaims from
a CANDIDATE LISTING.** (Not the only affine-consumer delete reachable from a sweep pass:
teardown-marker adoption still reaches `deleteTeardownTargets`, as above.)
`sweepOrphanedConsumers` lists `$MQTT_msgs` only, this pass reclaims no affine consumer by
absence since course E, and the reference snapshot had no PUBREL class at all —
so a `$MQTT_PUBREL_<idHash>_<gen>` consumer created just before the record save that
names it (`ensurePubRelConsumerSubscription` provisions it first) was unreferenced
forever if its broker died in between, holding its interest-retained PUBRELs with it.
The class was built as the affine one's mirror, and now stands on its own: the durable is
parsed back to `(idHash, generation)`,
its config must match all three names that identity derives — durable, filter subject
and delivery subject — at discovery *and* again at the pre-delete lookup, since a
consumer that disagrees with its own name cannot be attributed at all. A record is a
reference through the `PubRel` durable it names verbatim, and a *local* session through
the name derived from its `(idHash, generation)` unconditionally, which is what covers
the create-before-save window on this broker. The class shares nothing with the affine
inventory and is deliberately not gated on it: an unreadable assignment claim suppresses
the affine CLAIM pass, and only that, because one persistent corrupt claim must not
disable PUBREL reclamation account-wide on evidence about a different resource class.
Deletion is authorized by a final
authoritative record re-read — a save landing after the reference walk aborts it, and
does not change `Created`, so the instance check alone could not catch it — and then by
re-looking-up the same instance. **What those checks cannot observe is a session live on
another broker whose record is absent** (see "What AUTHORITATIVE means" in
`orphan_qos2_sweep.go`): whether deletion in that state costs anything for this resource
class is not answered here, because it belongs to the finding that owns the class rather
than to this section. Deleting the consumer drops the PUBRELs it held, which
is correct in every state those checks can observe: no observable session could deliver
them and the identifiers died with the session. Both windows are narrowed rather than
closed, because a consumer delete is by name and unconditioned; that residual, and the
resume-side verify-and-recreate that bounds it, are in `docs/CONFORMANCE.md`.

**The affine side is not a mirror image: the sweep's candidate-reclamation route no longer
infers affine-consumer deletion from ABSENCE.** That is course E's invariant, and the
wording is deliberate — it is not "nothing reachable from the sweep deletes an affine
consumer", which would be false. The sweep adopts abandoned teardown markers and finishes
them, and `deleteTeardownTargets` deletes `t.q2AffDur`; see the structural guard below for
what that route is and is not licensed to claim. What was removed is the inference from
absence, and the reason is that no positive fact can authorize it: the retirement watermark
records the **session** generation (`recordGenRetirement` writes `Gen: t.gen`) while the
consumer is keyed by an **affine** generation that outlives incarnations, so
`refs.hasInbound(idHash, ps.Gen)` and `refs.hasAffine(idHash, ps.Q2AffGen)` are different
keys and neither licenses the other. Consumer metadata has the same mismatch;
`NumPending == 0` is an instantaneous observation rather than a fence; and a record
re-read cannot tell "no session" from "a live session whose record is absent", which is
F-036's own premise. `$MQTT_out` is interest-retained, so a wrong delete destroys
acknowledged QoS 2 payloads — and the guards that read as defenses were not: a resuming
session ADOPTS the consumer through a create-or-update, which leaves
`ConsumerInfo.Created` unchanged, so the instance fingerprint cannot see it.

Assignments are still reconciled: an absent winner's claim is committed `Discarded`, and
claims are deleted only through `reapQ2AffClaim`, whose predicate refuses a claim marked
`Lost` and requires **both** `SourceAcked` and the copy to be absent. That two-sided
requirement is what makes the removal a leak rather than a no-op: with the consumer
retained the copy stays present, so such a claim is revisited every pass and reaped only
once another authorized actor removes or acknowledges the copy — a completed delivery
after an adoption, `settleRetiredAffineRegister`, or a lifecycle deletion (teardown,
rollback, a successful unwind). The accepted price is stated in `docs/CONFORMANCE.md`:
an unbounded correlated leak of the consumer, its copies and those claims, with no
automatic backstop, since the affine consumer's `InactiveThreshold` is deliberately zero
and `$MQTT_out` refuses `ConsumerLimits.InactiveThreshold`.

**The acceptance condition is behavioral**:
`TestOrphanSweepDoesNotReclaimAnAffineConsumerByAbsence` drives the real pass through every
state that used to authorize the delete and asserts the consumer AND its exact retained
copies survive, so a reintroduction fails it whatever syntax carries it.

Beside it, `TestQ2AffineConsumerDeletesAreInventoried` is a **drift alarm and an enforced
licence inventory**. It holds an exact, multiplicity-checked inventory of every
`$MQTT_out` consumer delete **in the package** — five sites — each with the reason it is
allowed to exist, plus the one call that routes a delete through a stream-generic ephemeral
helper. It has no reachability phase: four successive review rounds each evaded the
reachability step rather than the inventory, so it was deleted. Anything uninventoried fails,
a duplicate fails, and a stream that cannot be resolved to a named constant, a delete taken
as a method value, or a package-level function literal all fail CLOSED.

**Its claim is bounded to match its reach**, and this is deliberate: it detects direct and
common-indirect reintroduction, and is **not** a soundness proof, because AST analysis cannot
resolve Go function values, interface dispatch or reflection. Making it sound would need
closed-world type-checked SSA, which is disproportionate for an alarm the behavioral test
above does not depend on.

Of the five inventoried sites, `deleteOrphanPubRelConsumers` judges only `$MQTT_PUBREL_*`;
`ensureQ2AffineConsumerSubscription` and `finishQ2AffineUnwind` are session-fenced lifecycle
routes whose failure has no retry anchor; and `deleteTeardownTargets` (both its PUBREL and
its affine durable) is reached through teardown-marker adoption, and is carried as a
**known pre-existing call site, outside course E's change and owned by open F-036, with no
claim that it is race-free**: a won `claimTeardown` authorizes proceeding with lifecycle
teardown relative to the claimed record, but it does not fence a stale
same-affine-generation peer's `beginQ2AffOp` and create-only claim CAS, so
`discardQ2AffClaimsFor`'s scan can be outrun. That window is F-036's fourth manifestation,
with F-063's proof-to-ack residual behind its second half.

**Two abort-capable siblings carry every refusal this pass makes about a claim.**
`advanceQ2AffClaimChecked` and `reapQ2AffClaimChecked` take a caller-supplied precheck;
`advanceQ2AffClaim` and `reapQ2AffClaim` are thin wrappers passing nil, so their existing
callers are unaffected. The precheck runs on EVERY attempt, against the record that
attempt will actually CAS, after the load and the existing identity fences and **before
`next := *cur`, before `mutate`, and before `mergeProven`** — the only placement that
vetoes. Carrying the same condition inside `mutate` cannot veto at all: `mergeProven`
runs after it unconditionally, and on an `AffineDone ∧ Lost` record `mergeProven` clears
`Lost` by itself, so the CAS lands with the closure having mutated nothing, normalising
the very shape the condition existed to refuse. The checked reaper applies its precheck
at TWO points — its own initial load, and inside the conditional `SourceAcked` advance —
and deliberately not a third after that advance returns, because the precheck is an
invariant that transition preserves and a post-return call is not a fresh load.

`reapQ2AffClaimChecked` also returns a typed outcome, `reaped` / `retained` /
`indeterminate`, because a caller that deletes an in-flight register only once the claim
it references is provably gone cannot distinguish those three from a function that
returns nothing. An authoritatively absent claim and a not-found delete are both
`reaped`; a refused precheck is `indeterminate`, never `retained`, since a refusal proves
nothing about the record and must not license a dependent deletion.

**The retired affine register class is collected with its copy and its claim.** A live
record protects such a register through the INBOUND session key, a different key from the
claim's affine one, so it needs its own arm. `settleRetiredAffineRegister` acts on what
the register's own source proves — and for an affine delivery the source IS the copy.
Where that source is `Complete` the exchange is over and the triple is resolved; a
nonterminal claim of the same assignment there is a CONTRADICTION and everything is
retained rather than trusted to the record-before-ack ordering. Where the source is
`Live` and the message at the register's own sequence sits on the copy the register
names, the register is durable evidence of a successful PUBREC, which licenses recording
`AffineDone` — deleting the copy at its exact sequence with a CONFIRMED delete, reaping,
and deleting the register LAST because it is the only proof of that licence. `Unknown`
and `Lost` settle nothing: an unreadable source is not an absent one, and a knowably gone
consumer means the delivery was lost rather than finished.

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
| Per-client authz   | Auth passthrough: one `*nats.Conn` per MQTT client, plus the identity snapshot taken on it at CONNECT. NATS enforces SUBSCRIBE admission and QoS 0; the broker enforces publication and service-owned delivery |

## Auth passthrough

On CONNECT, the MQTT username/password are used to dial a per-client
`*nats.Conn`, and the broker then asks that connection **who it is**:
`$SYS.REQ.USER.INFO` answers with the bound NATS account and the identity's
publish and subscribe permissions. That one request is what makes the rest of
this section possible. Without it the broker knew only that the credentials
authenticated, which is why publishing was unenforced, the account was unbound,
and a subscribe deny stopped at SUBSCRIBE admission.

The request is made twice: once on the service connection at startup, **before
the MQTT listener opens**, to learn the account the broker's own streams live
in; and once per CONNECT on the client's connection, inside
`isClientAuthorized`, before the CONNECT is accepted.

### The identity snapshot

What the answer yields is a **snapshot**, taken once and never refreshed:

- **the bound account**, compared for exact equality with the service account.
  A mismatch refuses the CONNECT. This is the v1 same-account constraint below,
  which until now nothing enforced.
- **the publish permissions**, which gate every MQTT-originated publication.
- **the subscribe permissions**, whose DENY side is evaluated per message on the
  service-owned delivery routes. The ALLOW side is not re-evaluated: NATS
  already applied it when it accepted the subscription.

**Every unknown refuses the CONNECT**, and the list is exhaustive rather than
illustrative: a request that times out or errors, an undecodable body, an API
error, an empty user, an empty or mismatched account, a permissions object
present but missing a direction, a permission entry that is not a valid subject
filter, or a reported expiry already elapsed. A malformed deny that merely
"matches nothing" would be a fail-open, so it is refused rather than compiled.

A permissions object that is **absent entirely** is the shape a server with no
ACLs configured emits, and it means unrestricted. That is the default
deployment, so reading it the other way would refuse every publish everywhere.

### The snapshot's validity is the connection's validity

A per-client connection reconnects indefinitely and re-authenticates each time,
potentially under changed permissions, so a snapshot outliving its connection
would authorize service-owned stores on behalf of an identity that no longer
holds. The broker therefore installs a disconnect and a closed handler on the
connection it dialled, and either firing **closes that MQTT connection's
socket** - `c.rawConn.Close()` only, leaving the ordered teardown to `readLoop`,
exactly as the CONNECT deadline does.

The cost is that an MQTT connection does not survive a transient NATS outage.
That is the fail-closed direction, and it is more truthful than what preceded
it: the client's QoS 0 traffic already stopped silently for the duration.

The service connection carries an epoch too, and it holds two things: the
**account** it is bound to, and whether that is currently known. A disconnect
marks it unknown; the reconnect handler re-asks and restores it only for the same
account. Every snapshot records the account it was discovered against, and every
publication gate, every delivery guard and every Will discharge requires that
account to still be the current one, so an identity established for one account is
not knowingly used to authorize a write into another, and a reconnect into a
*different* account invalidates and closes every auth-passthrough client.

That check is not, however, atomic with the write it guards, and this document
must not claim it is. Between the check and the socket, a write can be held by the
service connection's reconnect buffer, by the JSA send queue, or by a per-client
reconnect buffer, and flushed after the connection has come back on a different
account. Closing that absolutely needs the authorized account carried through the
transport and validated at emission, which this item does not do. It is a stated
residual - `docs/CONFORMANCE.md`, residual 7 - and it requires an operator to
repoint the service credentials while the broker runs; the remotely-reachable
failures this section otherwise describes are closed.

The binding is the ACCOUNT and deliberately not a per-reconnect incarnation. An
earlier revision used a monotonic generation, which did close the cross-account
hazard and opened a larger one: an ordinary same-account reconnect advanced the
generation, so every live client's publications were refused and its deliveries
withheld permanently, with no event that would ever restore them. What the rule
names is the account, so that is what is compared, and a reconnect into the same
account is a non-event.

Invalidating on the disconnect rather than on the reconnect is what makes this
work at all: nats.go resends subscriptions and flushes its reconnect buffer
**before** it queues the reconnect callback, so a callback that discovers an
account change discovers it after the buffered effects have already landed in the
new account.

`UserInfo.Expires` is a remaining duration truncated to seconds. A **negative**
value is an identity the server already considers expired and refuses the
CONNECT. **Zero** is refused by nothing, because it means both "no expiry" and
"under a second remains" and cannot be told apart; a positive value additionally
arms a timer that invalidates the snapshot, and for zero the connection lifecycle
above is what carries the guarantee - an identity that really expires loses its
connection.

### What a deployment must allow

Auth passthrough imposes two requirements on every MQTT identity, and the second
is new and mandatory.

**It must be able to publish `$SYS.REQ.USER.INFO`.** It does not need to subscribe
to anything: the reply is collected on the SERVICE connection, under a
`$MQTT.uinfo.` inbox, because the server decides whose identity to report from who
*published* the request. That placement is not incidental - an MQTT `#`
subscription maps to the NATS wildcard `>`, which matches `_INBOX.>`, so a default
request/reply inbox would deliver one client's identity and permission lists to
another as an ordinary MQTT message.

**It must be DENIED publish on `$MQTT.>`. This is a security prerequisite, and the
broker cannot check it.** The reply is an unsigned message on an ordinary account
subject, and a request to `$SYS.REQ.USER.INFO` is observable to an account
subscriber - measured, not assumed. So a principal able to publish into the
reserved namespace can race the server and answer for any client, and a
fabrication claiming "no permissions configured" reads as unrestricted.

**No test applied to the reply can detect this**, because every such test is one
the forger also controls. An earlier revision of this section claimed a
self-referential check was sound - accept a reply only if the identity it
describes could not have published it - and that was wrong: a reply claiming
`allow: [">"]` with `deny: ["<that exact inbox>"]` satisfies it while granting
everything else. The check was removed rather than weakened, because a test an
attacker can satisfy is worse than none.

What makes this safe is the deployment, and the requirement is not new. The deny
has been recommended here since the reserved namespace existed, for a strictly
stronger reason than identity spoofing: **a principal who can publish to
`$MQTT.>` can already forge session records and JSA traffic directly**, so
spoofing an identity gains it nothing it did not already have. An account whose
MQTT users are unrestricted has no isolation to protect, with or without this
mechanism.

The service connection is exempt and deliberately full-rights: it is the thing
that writes that namespace, and its own identity is established at `Start`, before
the listener opens, so no MQTT client exists to race it.

No system account needs configuring - nats-server supplies a default `$SYS`, and
the recommended ACL in `test/conformance/nats.conf`
(`allow: [">"], deny: ["$MQTT.>"]`) already satisfies both requirements.

A blanket `deny: ["$SYS.>"]` on publish prevents the bootstrap and **cannot be
carved back with an allow**, because NATS evaluates deny before allow. Such a
deployment refuses every CONNECT, with the violation named in the log.

With `-auth-passthrough=false` there is no per-client identity at all: every
client rides the service connection and acts as the service identity, so no
per-client enforcement is possible and none is attempted. That mode assumes the
service credential is trusted and suitably privileged.

### The security principal is account plus client ID

A session is resumable by any credentials in the same account presenting the
same client ID. MQTT 5 [4.3.3] / [5.4.2] recommends authorizing access to the
Client Identifier, since it grants access to session state; binding a session to
a stable NATS principal would require persisting that identity, which the
compatibility boundary rules out. The boundary is therefore stated rather than
enforced: **account + client ID** is the principal.

### Publishing: authorize every effective subject before the first effect

The rule is a property, not a list of call sites, because this finding's history
is that each review round produced another route:

> Before the first live or durable effect, the snapshot must authorize **every
> effective application subject** the publication will create.

The set is not always a singleton. A Sparkplug birth publication also derives,
publishes and retains a `$sparkplug/certificates/...` subject on the service
JSA, and a Will whose topic is a birth topic does both - so an allowed Will with
a denied certificate subject is one publication with two effective subjects.

Where the gate sits:

- **QoS 0 and QoS 1** - in `mqttProcessPub`, before the QoS switch, so it
  precedes the live fan-out, the retained store and the `$MQTT_msgs` store.
- **QoS 2** - inside `mqttStoreQoS2MsgOnce`, immediately before the create-only
  CAS, on the branch that has established this is a new hold.
- **Wills** - decided ONCE, at CONNECT, and only read afterwards.

The Will is the one publication whose authorizing identity is expected to be gone
by the time it happens, so it cannot be gated where it is discharged: every
discharge site runs inside `mqttHandleClosedClient`, after the snapshot has been
invalidated, and asking there would suppress an authorized Will on any NATS blip.
The decision is therefore computed while the connection still exists - over the
Will's full effective-subject set - and recorded on the connection; the delayed,
takeover, uncertainty-parked and immediate paths all READ it. A denied Will is
consequently never enqueued and never published, and a `$MQTT_willq` record
exists only because the decision passed.

Two things follow. A record on that stream carries the **account** the decision
was made for, and replay refuses - retryably - if the service connection is bound
elsewhere. And under auth passthrough an immediate Will, which writes no record at
all, **always** takes that same client-free route, carrying the same captured
account.

There is deliberately no "publish it from the originating connection if it still
looks alive" branch, because that branch cannot be made safe: any check of the
snapshot must release the lock before publishing, and a per-client connection
reconnects without limit, so a revocation in that interval could send the Will
through a REAUTHENTICATED connection or lose a QoS 0 one outright. Only
`-auth-passthrough=false`, which has no account to cross and no snapshot to
revoke, keeps the originating route.

An internally replayed publication carries **no** snapshot and is not refused for
lacking one: its authority is the decision already taken. The gate distinguishes
an internal client positively rather than treating a missing snapshot as
permission.

### A held QoS 2 exchange carries its own authorization

MQTT transfers ownership of the message to the receiver at PUBREC, and both
3.1.1 and 5.0 require a later same-identifier PUBLISH before PUBREL to be
answered with PUBREC and no second onward delivery. A valid existing hold is
therefore the durable record of a decision already taken:

- a new hold is authorized before its first durable write;
- a duplicate PUBLISH on an occupied identifier is **not** re-authorized, and
  its subject has no effect - the hold subject is keyed on the packet
  identifier, not the message subject;
- PUBREL takes no fresh check, including when a peer broker serves it. A fresh
  check there would strand an exchange the broker already owns whenever an ACL
  changed mid-exchange.

### What the publisher is told

MQTT 3.1.1 [3.3.5.2] permits either a positive acknowledgement or closing the
network connection for an unauthorized PUBLISH; MQTT 5 has reason code 0x87.

| | MQTT 5.0 | MQTT 3.1.1 |
|---|---|---|
| QoS 0 | discarded, no acknowledgement exists | connection closed |
| QoS 1 | discarded, PUBACK 0x87 | connection closed |
| QoS 2 | no hold, no Receive Maximum slot, PUBREC 0x87 | connection closed |

This deliberately **diverges** from the reserved-`$MQTT.>` refusal in the same
function, which answers a 3.1.1 client with a success-shaped acknowledgement.
Both are legal. Matching it here would preserve exactly the misleading outcome
F-079 was about - the publisher told its denied message was accepted.

### Delivery: three guards, placed where the queue is known

QoS 1/2 delivery and retained replay ride the service connection, so a wildcard
subscription overlapping a narrower subscribe deny is not filtered by NATS.
Three guards evaluate the snapshot's deny side per message:

| route | where | queue |
|---|---|---|
| retained replay | before packet-identifier allocation | the subscription's |
| QoS 1/2 and shared | before the shared QoS 2 move | the subscription's |
| affine shared QoS 2 | after the claim is validated, before `trackPublish` | derived from `claim.GroupDurable` |

The affine route needs its own guard **and its own queue source**: its
subscription is created with a nil queue, so by delivery time the subscription
no longer names the shared group it came from. The queue is recovered from the
claim's group durable instead - `mqttSharedNames` derives the queue, the deliver
subject and the durable from one hash, so it is a prefix swap. If it cannot be
derived, the delivery is refused **without** acknowledging: an unknown is not a
denial, and consuming the copy would destroy it.

Plain QoS 0 on a user topic keeps no broker-side guard. It rides the client's
own connection, NATS filters it, and leaving it unguarded is what keeps it a
control that can tell the two mechanisms apart.

**Queue provenance is proven end to end on the two routes where it can matter,
and moot on the third.** `$SYS.REQ.USER.INFO` serialises a queue-scoped deny as
`"<subject> <queue>"`, so a guard that passes an empty queue downgrades it to a
subject-only check - which matches nothing and therefore *admits* the message.
The affine route's queue comes from the claim's group durable and is pinned by
its own fixture; ordinary shared QoS 1/2 delivery is pinned by
`TestQueueScopedDenyOnOrdinarySharedDelivery`, and passing `_EMPTY_` there
delivers a denied message. The retained-replay guard reads a queue too, but a
shared subscription never receives retained messages at all (Spec5 [4.8.2],
pinned by `TestMQTTv5SharedSubscriptionRetainedSuppressed`), so that guard only
ever runs for a plain subscription whose queue is empty: passing `sub.queue`
there is behaviourally equivalent to passing nothing, and no fixture for it could
have a live control. It is written as `sub.queue` regardless, because that is the
true expression of the subscription's queue and hardcoding the emptiness would
become a bypass the day retained replay reached a shared subscription.

**A proven deny disposes of the message differently on each of the three routes,
and the predicate is not "shared" - it is whether the copy is still the group's
to offer.** On a plain subscription the deny is a permanent fact about the *only*
consumer that will ever be offered the message, so it is acknowledged; handing it
back would redeliver against `MaxDeliver` of -1 for the life of the session. On
the ordinary shared group consumer the selection was made **without consulting
the member's ACL**, so the deny is a fact about one member and not about the
message: acknowledging it would consume the group's message on behalf of the
members who *are* allowed it. It is handed back instead, **paced by the same
`mqttPacedRefusalNak` backoff the affine refusals use**. That sharing is
load-bearing, and an earlier revision of this route got the argument exactly
backwards: it used a small fixed delay and reasoned that a permanent refusal has
nothing to back off from. Permanence is the case the cap is *for*. With an allowed
member present the hand-back is one fast retry at the base delay and the group
serves it; with the denied member the ONLY one there is nobody to hand it to and no
event that resolves it, so a fixed delay is not a retry but an unbounded loop
against `MaxDeliver` of -1 - measured at 59 re-offers in three seconds at 50ms, and
10788 with no delay at all. The delay comes from JetStream's own delivery count, so
no per-message state is kept. Upstream NATS reaches the same outcome by filtering
denied queue members before selection; this port cannot, because the selection
happens on the service connection.

The affine shared QoS 2 route is shared too and is nevertheless **terminal**, which
is why the rule cannot be stated as "shared hands back". By the time that copy
exists the group has already chosen: the assignment moved the exchange to one
session, the copy carries that session's claim, and no other member can take it.
Handing it back would redeliver it to the same denied session forever. So a proven
deny there records `AffineDone` and acks the copy, exactly as a completed delivery
does - the exchange is discharged, not transferred. Only an **indeterminate**
affine verdict is returned, and that one keeps the exponential backoff, because
what it is waiting out really is transient.

An **indeterminate** verdict - no usable snapshot - is disposed of as neither. It
is a fact about us, not about the message, so the copy is withheld without being
acknowledged and `AckWait` keeps it for the client's next connection.

**A terminal deny also releases the packet identifier a previous transmission
left behind**, through `releaseTransmittedIdentifier`, and this is a second fact
about placement rather than a detail of it. Both guards sit before
`trackPublish` so that a refusal costs no *new* identifier and no new register -
that much the placement does buy. It buys nothing about an identifier the
delivery did not allocate: a message transmitted before a disconnect leaves a
durable in-flight register, and the resume restores it into `pendingPublish` and
`cpending` before any guard runs. A guard that disposed of the message and left
that entry standing would keep it counted by `qos12InflightForCap` for the life
of the session, so with a Receive Maximum of 1 the whole quota is gone and every
later *allowed* delivery is withheld - silently, with nothing on the wire saying
so, and indistinguishable from the denied message's own non-delivery. The release
is therefore part of what "terminal" means on both routes, and it happens before
the ack, in the same order as the expiry and Maximum-Packet-Size disposals. The
**indeterminate** arms deliberately do not do it: they decide nothing, and freeing
an identifier is a decision.

**An unavailable destination DEFERS a Will; it never destroys one.** Three Will
routes build a synthetic outbox record and publish it immediately rather than
enqueueing it - the immediate path, the owed-Will path a teardown that lost the
session gate takes, and the deferred-close discharge. None writes to
`$MQTT_willq`, so none leaves anything a later attempt could find, and under auth
passthrough all three publish through the internal account client whose
`deliverWillRecord` refuses while the service connection's identity is
unavailable. That combination made an ordinary NATS blip destroy an authorized
Will - a regression this work introduced, since before it an immediate Will fanned
out through the still-live per-client connection and its store rode nats.go's
reconnect buffer.

`will_blip_retry.go` closes it. A Will refused by the account fence is retained in
memory with the account it was authorized against, and re-attempted when the
service reconnect handler has rediscovered and revalidated that *same* account.
Four things about it are load-bearing:

- **Only the fence's refusal is retried**, through the `errMQTTWillDestinationUnavailable`
  sentinel. That refusal happens before any fan-out and before any store, so
  nothing is partially applied and a later attempt duplicates nothing. Any other
  delivery error may follow a partial effect and is reported as a loss instead.
- **`deliverWillRecord`'s fence stays the authority.** The queue does not decide
  that a Will may be published; it only asks again, per record. A record whose
  account is not the current one stays parked, so a broker that came back in a
  *different* account publishes none of them and discards none of them either -
  and a pass does not give up on the first refusal, because the refusals are
  per-account.
- **One pass at a time, holding the pass lock across the publishes.** This is the
  shape two review rounds paid for. An earlier version took a batch out of the
  queue, published it unlocked, and put back what it kept; every defect those
  rounds found lived in that bookkeeping - capacity handed out against records
  still in flight, and a discharge request dropped because another pass held the
  batch. Keeping each record in the queue, charged, until it is resolved makes the
  accounting exact by construction, and making a second caller *wait* rather than
  return makes a lost wake-up impossible: the waiter runs a full pass, which by
  definition sees everything the pass before it left.
- **Neither caller blocks.** Parking runs on a teardown path and the reconnect
  discharge runs inside a nats.go connection callback, so both go through
  `kickOwedImmediateWills`, which spawns the pass. At most one pass runs and one
  waits, and the coalescing flag is cleared as soon as the waiting pass takes the
  lock - before it looks - so a request arriving mid-pass schedules a fresh waiter
  instead of being folded into a pass that has already been past those records.
- **It is bounded and in memory, and claims nothing more.** The only durable park
  is `$MQTT_willq`, which is what is unreachable during the outage. Restart,
  shutdown and the capacity bound remain loss cases, stated in
  `docs/CONFORMANCE.md` residual 8 rather than implied to be handled.

The deferred-close route additionally captures its account at defer time rather
than reading the live one at discharge. Reading it later answers a different
question - where the broker is pointing *now* - so a Will authorized against one
account could be published into another, which is exactly what the fence exists to
stop.

**The affine indeterminate arm needs a test seam, and the seam is load-bearing.**
`mqttTestAffineGateV` runs immediately before that guard reads its verdict and
lets a test make the verdict indeterminate for the delivering client. Nothing
external can: a client resuming an owed copy holds a snapshot established
moments earlier, and every way of revoking it from outside also closes the
connection the copy would have arrived on. Without the seam, replacing the
paced refusal with the deny arm's terminal disposal passes the whole suite -
and that mutation records `AffineDone` and acks a session-owned QoS 2 exchange
on a transient identity blip, losing a message an entitled resume should have
received. The seam is nil in production.

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

**Two promises, and only the first is enforced.** The bound makes a silent connection's
LIFETIME finite — the callback closes the socket whenever it runs, which is the resource
argument. It does NOT enforce refusal of a CONNECT that arrives after the window: admission
tests whether the callback has already transitioned the state, never whether the window has
elapsed, so the effective admissibility bound is the configured value plus the callback's
scheduling delay. `docs/CONFORMANCE.md` states that gap and marks it not accepted; the
paragraphs below describe the mechanism as built.

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
