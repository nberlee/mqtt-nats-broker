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
