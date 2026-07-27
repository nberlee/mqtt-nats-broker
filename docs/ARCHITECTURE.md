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
| `$MQTT.qos2.in.*`, `$MQTT.out.*` | QoS 2 inbound dedup and PUBREL state |
| `$MQTT.sub.*`, `$MQTT.deliver.pubrel.*`, `$MQTT.qsub.*` | per-client delivery subjects |

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
  that rule and would otherwise be honoured. A legacy subscription found in a
  resumed session record is dropped from the record, not merely ignored.
- **Will topics** in the namespace are rejected at CONNECT: `0x05` in 3.1.1,
  `0x87` in 5.0. A Will never passes through the PUBLISH path, and a *delayed*
  Will is replayed from the outbox over the service connection where the client's
  own NATS permissions no longer apply, so it is refused up front.

### Recommended NATS permissions

Deny both directions on the whole prefix for the identity MQTT clients
authenticate as:

```
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
