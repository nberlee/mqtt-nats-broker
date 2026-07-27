# Architecture

```
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

This boundary applies only to bridge persistence and its JetStream backend. MQTT
3.1.1 and 5.0 wire-protocol compatibility remains a product requirement and is
covered by the conformance suites.

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
  identified by a generated epoch (`nuid:<NUID>`) stored in the session-stream
  metadata and carried on both the marker (`SessEpoch`) and each Will
  (`OriginSessionSeq`, `OriginSessEpoch`). The claimant fence suppresses a Will when a
  current-epoch marker is *strictly beyond* its origin sequence
  (`SessionSeq > OriginSessionSeq` — the connection's own delayed Will, at an equal or
  higher origin, still fires), or when the marker is from a different incarnation.
  Because the marker is permanent, this holds even after the newer connection
  finite-disconnects or its session record is deleted — the residual a session-record
  fence alone could not close. When no marker is present (administrative loss), the
  fence falls back to the numeric session-record rule (suppress iff a newer record
  with `DisconnectedAt == 0`), and a marker-stream outage defers pending Wills (never
  lost) until it recovers.

Residual that keeps literal exactly-once impossible:

- *At-least-once duplicate* — a crash between the external publish and the `done`
  commit republishes after the lease expires (the sanctioned duplicate).

The reconnect-suppression miss is now closed by the durable watermark; it reopens
only under administrative loss of `$MQTT_willcancel` (a documented degradation, not a
supported migration). This is a first-release storage format: pre-Layer-2 in-record
Wills, migrated outbox records, and a `$MQTT_sess` stream without a valid generated
epoch are unsupported and rejected with recreate guidance rather than carried forward.

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
