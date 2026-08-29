# NATS platform-adoption audit

This dossier records the 2026-08-14 audit of released and announced JetStream
features against the bridge's MQTT, authorization, consistency, and HA contracts.
The roadmap links the actionable results. Rejected candidates stay here so the
JetStream request audit does not repeatedly rediscover an attractive but unsound
substitution.

The audited baseline is nats-server 2.14.5 and `nats.go` 1.52.0. The server
dependency is both the embedded test server and the source of the exported types
used by the MQTT shim; production still talks to an external server, so a
load-bearing server behavior also belongs in the documented runtime floor.

## Term-fenced Raft proposals

nats-server 2.14.4 shipped
[`nats-io/nats-server#8370`](https://github.com/nats-io/nats-server/pull/8370),
which requires the JetStream layer to pass the leadership term under which an
asynchronous operation began into `Propose`/`ProposeMulti`. Raft rejects the
proposal when that term is no longer current, and leadership changes tear down
the old stream-leader state. The
[2.14.4 release note](https://github.com/nats-io/nats-server/releases/tag/v2.14.4)
describes the consequence as preventing stale proposals from a previous term
from changing state after a fast election. The pinned upstream evidence is
[`raft.proposeLocked`](https://github.com/nats-io/nats-server/blob/v2.14.5/server/raft.go#L914-L919)
and
[`TestNRGTermFencedProposals`](https://github.com/nats-io/nats-server/blob/v2.14.5/server/raft_test.go#L6393-L6455).

This is **not an apply-time recheck of**
`Nats-Expected-Last-Subject-Sequence`. It targets the stale stream-leader goroutine
that could perform the pre-proposal CAS check under one leadership epoch and
propose after a fast election; it does not make arbitrary unconditional stores
safe, add cross-stream transactions, explain administrative deletion, or provide
lineage across stream recreation.

**What the bridge actually gets from it was measured and upstream-confirmed** (F-030,
closed 2026-08-16). `test/depproof/` enumerates every route by which a write conditioned
on a superseded base could reach a proposal across a leader transition, fixtures the two
live ones inside isolated checkouts, refutes seven other shapes, scopes post-start
mirror/source drift outside F-030, and pairs mutations with each live mechanism. The term
fence covers ONE of those two routes; the other
is refused because a new leader does not accept messages before pending entries are
applied and then reconstructs the subject's expectation from its own applied state. A
NATS maintainer explicitly confirmed that these leader-only mechanisms guarantee header
enforcement across elections
([`nats-server#8469`](https://github.com/nats-io/nats-server/issues/8469#issuecomment-5304075553)).
The four repaired oracle defects keep the harness from being the sole proof, but do not
defeat the independent source and upstream contract. The v2.14.3 control run corrected
this audit's expectation: an old-term write is ADMITTED there, which is the difference
the floor buys, but it never becomes the surviving record, because an apply-time
log-order check that both generations have discards it.

That closure belongs to `$MQTT_willq`. The per-stream findings inherit the server contract
and the canary, and each needs its own consequence audit before it closes. **All three
have now run.** F-032's ran on 2026-08-17 and closed without lineage, producing F-062, which
closed on 2026-08-18; F-033's ran on 2026-08-18, also closed without lineage, producing
F-063, F-064, F-065 and F-066; and F-034's ran on 2026-08-22, closing without lineage too
but as the one of the three whose consequence turned out to be REACHABLE, producing F-067
and F-068. The lineage-deletion audit is its own roadmap bullet and is now unblocked.
Fields used for other facts do not fall merely because the term window does — though
`RWID` is no longer one of them at all, and the field has been **REMOVED**. F-036's
2026-08-27 attempt disproved it as that finding's predicate, because the watermark holds one
slot per client, so an `ackWID == rwid` conjunct recreates the superseded-watermark false
negative the fix exists to close. The vanished-record vocabulary item was the only remaining
candidate reader and decided against it too: a write identity names a record's bytes rather
than one occurrence of them, so it cannot say which occurrence a retirement removed.

### Rejected follower-local apply-time CAS

Issue #8469 proposed marking conditional entries so every replica would recheck its
physical store during apply. That design was withdrawn. Interest retention can make
storage depend on replica-local consumer state, and MaxAge or per-message TTL expiry is
timer-driven on each replica. Physical per-subject store state is therefore not a
deterministic consensus input. If one peer rejected while another stored, their failed
sequence accounting could diverge. Restricting emission by the stream's current
configuration does not repair historical state or order a concurrent configuration
change with the stream log. The maintainer confirmed the architectural rule: the leader
alone decides admission; followers persist the sequence it selected.

## Subject delete markers for retained convergence

`$MQTT_rmsgs` already enables per-message TTL and keeps one record per retained
topic. JetStream's `SubjectDeleteMarkerTTL` can make expiry of that last value
observable: the server writes a marker with `Nats-Marker-Reason: MaxAge`, including
when per-message `Nats-TTL` caused the expiry. See
[ADR-43](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-43.md).

This is a candidate mechanism for F-059, not an independent shortcut. The retained
observer must recognize a marker before decoding an MQTT retained record, and both
values and markers must be applied by stream sequence so a delayed marker cannot
erase a newer generation. A replayable consumer or authoritative reconciliation is
still required because a marker delivered through the current `AckNone` consumer
can itself be lost. Explicit empty-body MQTT tombstones remain the immediate durable
delete authority.

Enabling delete markers also enables rollup and requires purge permission. The
current broker-owned stream contract rejects rollup, so the implementation needs a
stream-prerequisite and ACL review. In server 2.14 the useful marker is age/TTL;
ADR-43 records delete-API and purge-API markers as not yet implemented. The sourced
`$MQTT_rmsgs` topology must be in the test matrix because source handling of TTL and
markers has its own rules.

## Stable ephemeral-consumer identities and metadata — DONE

**Landed.** The retained observer and session-record watch derive a stable name **and
delivery subject** from a per-`Server`-run identity, so an initialization retry asks
for the same consumer instead of increasing cardinality. A restarted process receives a
new identity; `InactiveThreshold` remains the crash cleanup. See "Per-run ephemeral
consumer identity" in `docs/ARCHITECTURE.md` and the five residual statements in
`docs/CONFORMANCE.md`.

**Two corrections this audit item needed, both found by implementing it:**

- **A stable name alone does not suffice**, and would have been a silent regression. A
  repeated named create ADOPTS the existing consumer at its current position, so a retry
  would inherit an observer already drained by a discarded manager and leave the retained
  index empty — invisible to every wildcard subscription. The observer therefore proves
  its name ABSENT before creating it, and `ActionCreate` keeps an unexpected occupant a
  refusal rather than an adoption.
- **Metadata is not an ownership predicate for the rollback.** The rollback reads no
  metadata: its licence is a confirmed create, because no error distinguishes "our create
  landed" from "a foreign consumer is at that name". The metadata's only machine reader
  is the server's own `ActionCreate` equality check, where the independent per-run nonce
  is what makes a colliding run's config unequal; an operator and any future sweep are
  the human readers.

## Staged use of the high-level JetStream client

The backend already creates `jetstream.JetStream`, while the MQTT core still owns a
ported raw request multiplexer. A wholesale replacement would lose useful test seams
and does not cover every specialized operation, but a narrow transport boundary can
migrate ordinary stream/consumer CRUD, information calls, publish acknowledgements,
and double acknowledgements incrementally. Per-MQTT-client connections must remain
the authority boundary under auth passthrough. Keep a thin raw adapter for atomic
batch framing and endpoints without a high-level equivalent.

Library-managed
[ordered consumers](https://pkg.go.dev/github.com/nats-io/nats.go@v1.52.0/jetstream#Stream.OrderedConsumer)
are a bounded spike for the retained observer and session-record watch: they recreate
their ephemeral pull consumer after deletion/restart and reset when ordering is lost.
They do not remove the session watch's committed-read backstop, and adoption requires
a before/after request-count and failure-semantics benchmark.

## Message schedules: post-alpha Will-outbox spike

JetStream schedules can durably own a delayed timer and can atomically cancel a
schedule while publishing another fact **in the same stream**. See
[ADR-51](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md)
and the [nats-server 2.14.0 release](https://github.com/nats-io/nats-server/releases/tag/v2.14.0).
A possible future design schedules an internal "Will due" fact; the bridge still
performs MQTT authorization, fan-out, retained handling, and terminalization.

This is not an F-030 or F-031 replacement. The accepted session record and its
acceptance fact commit together in `$MQTT_sess`, while the natural schedule lives in
`$MQTT_willq`; schedule cancellation supplies no cross-stream atomicity. A sound
adoption therefore requires a schema redesign that co-locates the required facts or
retains the present acceptance proof. Schedules also imply rollup/purge capabilities.

## Deliberate non-adoptions

- **Direct multi/batch get:** useful for request count, but Direct Get may answer
  from a lagging replica and explicitly lacks read-after-write coherence. The
  bridge's correctness-sensitive loaders retain the leader-routed management GET;
  see [ADR-31](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-31.md).
  A future leader-routed batch API could change this decision.
- **Pinned/prioritized consumers:** pin the pull consumer as a whole, are not an
  exclusivity guarantee, and would concentrate an MQTT shared group on one member
  rather than preserve per-delivery load sharing. See
  [ADR-42](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-42.md).
- **KV or counters as record stores:** do not preserve the raw stream-sequence
  identities, arbitrary record bodies/headers, exact-sequence delete classification,
  source behavior, or cross-subject atomic batch used by the current protocols.
- **Consumer pause/reset:** current delete/recreate sites change identity,
  configuration, or ownership; they are not cursor rewinds. Pause also stops all
  delivery and therefore does not express "stop new work while draining pending".
- **Fast-ingest batch publishing:** adds a second framing protocol and is justified
  only if the roadmap benchmark proves publish ingress is the bottleneck. Atomic
  batches remain appropriate for the F-031 session-record/acceptance-fact commit.
- **Upstream MQTT PR 8416's in-memory `qos2Exchanges` map:** a per-connection,
  PI-keyed cache holding the staged PUBLISH (so PUBREL skips the load) and a nil
  entry marking "released, discard in flight" (so a retransmitted PUBREL is
  screened). Three reasons it does not transfer. It is **connection-scoped**, and
  here a PUBREL can be served by a peer, so an in-memory fact is not evidence about
  the exchange. Its duplicate detection **swallows a `max_msgs_per_subject`
  rejection**, which learns only "the subject was occupied" — ported here it would
  collapse `held`, the `delivered` tombstone and `corrupt` into one verdict, and
  misreading a tombstone as a duplicate is the case that silently loses a payload;
  `mqttStoreQoS2MsgOnce`'s create-and-reconcile loop classifies the occupant
  instead. And the **fences cannot be cached away**: `verifyQoS2HoldForRelease` is a
  re-read whose job is to catch what changed between the load and the claim.
  *Precisely one fetch could go* — a subordinate cache carrying the generation, the
  hold sequence, an owned copy of the payload and the record's **expiry origin**
  (PUBREL ages Message Expiry from `stored.Time`, so restarting it at PUBREL would
  extend the message's lifetime), with explicit eviction on PUBCOMP, teardown and PI
  reuse, could skip the load on a same-connection hit and still run the claim and the
  proof. That is a performance option, not a reason to adopt the protocol.
- **PR 8416's fire-and-forget subject purge:** an unbounded subject purge can apply
  after PI reuse and destroy a later occupant. Every removal here carries a proven
  cutoff — see F-062 in [ha-arbitration.md](ha-arbitration.md).
- **PR 8416's `mqttIsMaxMsgsPerSubjectErr` / `exceptDuplicateError`:** interpret a
  store error to infer a duplicate. `qos2_move.go` deliberately replaced that with
  structural expected-sequence CAS classification.
- **Upstream MQTT PR 8415's `LoadAndDelete` in the JS API reply dispatcher:** the
  non-atomic `Load`+`Delete` in `processJSAPIReplies` is safe here because the handler
  is registered on exactly one wildcard subscription per `mqttJSA` and nats-go
  dispatches a subscription's callbacks on a single goroutine. Splitting that
  subscription — floated in `createSubscriptions` — does **not** by itself make the
  race reachable: disjoint parts give concurrent callbacks for *different* reply
  subjects. Only an overlapping or duplicate subscription would. The inherited comment
  claiming multi-goroutine dispatch was corrected instead, and it now records the four
  preconditions a split must satisfy.

## Already better than upstream

- **Invalid staged QoS 2 records.** PR 8416 changes upstream to fail the connection
  immediately on an invalid staged message rather than deferring PUBCOMP, so client
  retries converge. `mqttProcessPubRel` is stronger and already shipped: it answers
  0x92 only after **confirmed exact-sequence cleanup**, and where cleanup cannot be
  confirmed it suppresses PUBCOMP and closes — because PUBCOMP frees the packet
  identifier, and a surviving record would then swallow the client's next PUBLISH on
  it (invariant 3, `qos2_move.go`).

## Deferred prior art, not rejected

Distinct from the non-adoptions above: these are upstream mechanisms this broker has
**not assessed against measurements of its own**, not ones it has refused.

- **PR 8415, pipelining inbound QoS 1 stores.** A per-connection ack goroutine drains
  a bounded FIFO so the readLoop no longer blocks on the JetStream round trip. Our
  inbound QoS 1 path is the pre-8415 synchronous shape. Upstream reports 2.7K → 68K
  msg/s single-connection on an R3 cluster (p99 102ms → 5ms), with 1.1–1.2x
  aggregate gain across many connections.
- **PR 8416's pipelining half**, separable from the dedup protocol rejected above:
  moving QoS 2 PUBLISH and PUBREL work behind the same FIFO and carrying PUBREC and
  PUBCOMP through it. Evaluable against **this broker's existing durable protocol**.
  Upstream reports 690/s → 18.1K/s single-connection (p99 384ms → 19ms) and
  **1.4–1.5x aggregate** across many connections — a materially better aggregate
  result than 8415's, so the two must not be quoted with one shared figure.

  **Those numbers measure the whole of PR 8416, not the half proposed here.** 8416
  attributes its single-connection gain to dropping the load and the waited delete,
  and its aggregate gain to removing per-message JetStream API round trips — i.e. to
  exactly the mechanisms rejected above. **No published measurement isolates a
  pipeline-only QoS 2 design**, so the QoS 2 case rests on the QoS 1 result plus the
  structural argument, and producing a pipeline-only number is part of the
  evaluation rather than an input to it.

Both are recorded against the roadmap's *Performance and resource audits* item,
together with the invariants any pipeline here must preserve. Treat upstream's numbers
as **comparison data, not this broker's cap or target**: this port uses real `nats.go`
connections and a standalone JSA dispatcher, and a retained PUBLISH incurs an extra
store. The roadmap's own rule applies — benchmark before changing a query path.

For the inbound QoS 2 mechanism itself, see `docs/ARCHITECTURE.md`'s inbound-move
section rather than a second description here.

## Version and forward-compatibility actions

nats-server 2.14.5 fixes potential data loss when an idempotent stream create is
handled while an offline node catches up from a metadata snapshot. The bridge's
create-or-validate provisioning makes that patch directly relevant; see the
[2.14.5 release](https://github.com/nats-io/nats-server/releases/tag/v2.14.5).
The external-server floor is therefore 2.14.5 on every JetStream peer, not merely
2.14.4 where the term fence first appeared.

Upstream MQTT PRs **8415** and **8416** are **open**, not merged, and titled for
2.15; 8416 is based on 8415's `mqtt-qos1-pipeline` branch rather than `main`. Note
that a `go.mod` bump does **not** update `internal/mqtt/mqtt.go`, which is a *copy* of
upstream's `server/mqtt.go` — so tracking these is a **manual source-port audit** on a
future dependency bump, not something a version bump does for us. Their dispositions
are above: two protocol mechanisms rejected, one dispatcher change declined, and both
pipelining halves deferred pending our own benchmark.

The `js_ack_fc_v2` feature flag changes ACK and flow-control subjects to include the
JetStream domain and account hash and is announced as the v2.15 default in the
[2.14.0 release notes](https://github.com/nats-io/nats-server/releases/tag/v2.14.0).
The bridge parser already accepts both token layouts. Enabling the flag in clustered
CI now is a compatibility check for parsing **and ACLs**, not a production behavior
change.
