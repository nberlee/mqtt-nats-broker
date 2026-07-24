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
