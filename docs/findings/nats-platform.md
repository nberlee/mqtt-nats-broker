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
[`raft.proposeLocked`](https://github.com/nats-io/nats-server/blob/v2.14.4/server/raft.go#L908-L920)
and
[`TestNRGTermFencedProposals`](https://github.com/nats-io/nats-server/blob/v2.14.4/server/raft_test.go#L6400-L6455).

This is **not an apply-time recheck of**
`Nats-Expected-Last-Subject-Sequence`. The exact bridge consequence therefore
must be proved rather than inferred. It targets the stale stream-leader goroutine
that could perform the pre-proposal CAS check under one leadership epoch and
propose after a fast election; it does not make arbitrary unconditional stores
safe, add cross-stream transactions, explain administrative deletion, or provide
lineage across stream recreation.

The first consumers of this guarantee are F-030 and F-032 through F-034. Their
closure gate is one deterministic clustered dependency test that holds a
CAS-checked operation across the relevant leadership transition and proves the
old same-base survivor cannot land on the supported server. The corresponding
pre-2.14.4 run is evidence that the test reaches the old window, not a supported
compatibility mode. Only after that test exists may the findings close and the
session/outbox lineage machinery be audited for deletion. Fields used for other
facts, notably `RWID` as possible positive retirement evidence for F-036, do not
fall merely because the term window does.

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

## Stable ephemeral-consumer identities and metadata

The retained observer and session-record watch currently use a fresh consumer name
on every manager initialization. One unguessable broker-process identity should
instead derive a stable name **and delivery subject** for each purpose within that
process/account. An initialization retry then repeats the same consumer
configuration instead of increasing cardinality. A restarted process receives a new
identity; `InactiveThreshold` remains the crash cleanup.

Populate `ConsumerConfig.Metadata` with at least owner, purpose, process instance,
and schema version. The field has been supported since nats-server 2.10 and is
documented in the current
[`nats.go/jetstream` API](https://pkg.go.dev/github.com/nats-io/nats.go@v1.52.0/jetstream#ConsumerConfig).
This gives rollback and any future orphan sweep an exact ownership predicate rather
than a loose name-prefix convention.

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

## Version and forward-compatibility actions

nats-server 2.14.5 fixes potential data loss when an idempotent stream create is
handled while an offline node catches up from a metadata snapshot. The bridge's
create-or-validate provisioning makes that patch directly relevant; see the
[2.14.5 release](https://github.com/nats-io/nats-server/releases/tag/v2.14.5).
The external-server floor is therefore 2.14.5 on every JetStream peer, not merely
2.14.4 where the term fence first appeared.

The `js_ack_fc_v2` feature flag changes ACK and flow-control subjects to include the
JetStream domain and account hash and is announced as the v2.15 default in the
[2.14.0 release notes](https://github.com/nats-io/nats-server/releases/tag/v2.14.0).
The bridge parser already accepts both token layouts. Enabling the flag in clustered
CI now is a compatibility check for parsing **and ACLs**, not a production behavior
change.
