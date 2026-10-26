# mqtt-nats-broker

A standalone, horizontally-scalable **MQTT 3.1.1 / 5.0** broker that uses an
**external NATS/JetStream cluster** as its highly-available backend for sessions,
retained messages, QoS1/2 persistence, and message routing.

[![CI](https://github.com/nberlee/mqtt-nats-broker/actions/workflows/ci.yml/badge.svg)](https://github.com/nberlee/mqtt-nats-broker/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> **Status: pre-release working broker.** There is no public broker release or
> supported persisted-state format yet. The default build serves MQTT 3.1.1/5.0
> end-to-end — CONNECT, SUBSCRIBE, PUBLISH (QoS0), retained/sessions via
> JetStream, and auth passthrough with per-client NATS authorization — verified
> with a real MQTT client in CI. The upstream regression suite runs in CI against the
> standalone broker (behind the `mqtt_fulltests` tag). See
> [CONTRIBUTING.md](CONTRIBUTING.md#test-suites-and-build-tags).

## Why

NATS server ships MQTT **3.1.1** embedded. The **MQTT 5.0** implementation this
project is built on will not be merged upstream. Rather than fork the whole
server, this runs the MQTT layer as its own service backed by an external
NATS/JetStream cluster — so you get MQTT 5 **and** NATS-grade HA without carrying
a server fork.

## How it works

Stateless MQTT frontends sit in front of a replicated JetStream cluster. All
durable state lives in JetStream, so any client can reconnect to any frontend and
sessions survive instance loss. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Quickstart

The broker needs nats-server 2.15.1 or newer with one server option set; the
prerequisites, the binary and both container images are in
[docs/INSTALLATION.md](docs/INSTALLATION.md).

Run a broker + NATS locally:

```sh
docker compose -f test/conformance/docker-compose.yml up --build
```

Or run the binary against an existing NATS cluster:

```sh
go build ./cmd/mqtt-nats-broker
./mqtt-nats-broker -nats nats://nats-1:4222,nats://nats-2:4222 -port 1883
```

## Documentation

- [Installation](docs/INSTALLATION.md): prerequisites, the binary, the container images
- [Configuration](docs/CONFIGURATION.md): every flag and environment variable
- [TLS](docs/TLS.md): TLS, mTLS, certificate rollover, secure WebSocket
- [Kubernetes](docs/KUBERNETES.md): the Helm chart, example manifests, HA sizing
- [Authentication and authorization](docs/AUTHENTICATION.md)
- [Migration](docs/MIGRATION.md) from nats-server's own MQTT listener
- [Operations](docs/OPERATIONS.md) and [Troubleshooting](docs/TROUBLESHOOTING.md)
- [Architecture](docs/ARCHITECTURE.md), [Conformance](docs/CONFORMANCE.md) and
  [Provenance](docs/PROVENANCE.md)

The same documents are published as the project's GitHub Wiki at each release
([docs/WIKI.md](docs/WIKI.md)).

## Authentication

Auth is **passed through to NATS**: an MQTT client's username/password dial a
dedicated `*nats.Conn`, and the broker asks NATS who that is (`$SYS.REQ.USER.INFO`).
It refuses an identity outside the service connection's account, and holds the client
to that identity's publish and subscribe permissions on every route, including the
stored, retained and Will paths that ride the service connection. Anonymous clients
use the configured default credentials. The permissions are read once, at CONNECT;
that and the other residuals are in
[docs/CONFORMANCE.md](docs/CONFORMANCE.md#authorization-of-the-authenticated-identity--closed-with-residuals).
[docs/AUTHENTICATION.md](docs/AUTHENTICATION.md) has the deployment guide, with
user/password and operator (JWT) examples, and
[docs/MIGRATION.md](docs/MIGRATION.md) moves a deployment from nats-server's own MQTT
listener.

The broker reserves `$MQTT.>` for its own state and refuses client publishes,
subscribes, and Will topics that target it. Deny both directions on that prefix
for the MQTT client identity as well — the broker's checks bind MQTT clients,
the ACL binds anyone using the same credentials to reach NATS directly. See
[Reserved `$MQTT.>` namespace](docs/ARCHITECTURE.md#reserved-mqtt-namespace) for
the topic mapping and the exact permissions.

The MQTT client identity must also be denied the JetStream API, `$JS.>` in both
directions (and `$KV.>` and `$O.>` as default isolation): an MQTT topic maps onto a
NATS subject, so a client publishing to `$JS/API/STREAM/DELETE/$MQTT_sess` would
otherwise delete the broker's session stream. The recommended ACL is
`test/conformance/nats.conf` ([details](docs/ARCHITECTURE.md#what-a-deployment-must-allow)).

## Feature matrix

| Feature | 3.1.1 | 5.0 |
|---|---|---|
| QoS 0 / 1 / 2 | ✅ | ✅ |
| Retained messages | ✅ | ✅ |
| Persistent sessions (JetStream) | ✅ | ✅ |
| Shared subscriptions | — | ✅ QoS 0/1; QoS 2 experimental and not part of alpha |
| Will (+ delay) | ✅ | ✅ (delay) |
| Message / session expiry, reason codes, properties | — | ✅ |
| TLS / mTLS | ✅ | ✅ |
| Auth passthrough (per-client NATS authorization) | ✅ | ✅ |
| HA cross-instance session takeover | ✅ | ✅ |
| MQTT over WebSocket / secure WebSocket | ✅ | ✅ |

(MQTT 5 is always enabled; the shared-QoS-2 gate is a separate feature boundary.
For the alpha, a shared filter requesting QoS 2 is accepted at granted QoS 1 by
default, while non-shared QoS 2 is unchanged. The current affine shared-QoS-2
implementation and tests remain in the tree behind a programmatic test option that
the production command does not expose, with no persistence upgrade or downgrade
guarantee. All supported rows above are covered
by end-to-end tests driving real MQTT clients against the broker over in-process
JetStream — including load-balanced shared QoS 0/1 subscriptions, message expiry,
and reconnect-to-a-different-instance takeover. See the
[dedicated post-alpha roadmap chapter](docs/ROADMAP.md#p-shared-subscription-qos-2-post-alpha--maybe-someday).)

## Roadmap

The authoritative pre-alpha release checklist is maintained in
[docs/ROADMAP.md](docs/ROADMAP.md).

## Development

```sh
make test          # unit + end-to-end tests (real MQTT client over JetStream)
make vet lint      # static analysis
make full-suite    # upstream MQTT suite + harness-based tests (tag mqtt_fulltests)
make docker        # build the container image
make image-smoke-external  # build it and run it as a pod would (needs Docker)
make image-smoke-embedded  # the same for the embedded-NATS image, including a 3-node cluster
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License & attribution

Apache-2.0. Substantial portions are derived from
[nats-io/nats-server](https://github.com/nats-io/nats-server) (Apache-2.0); see
[NOTICE](NOTICE) and, for the source revision and what changed,
[docs/PROVENANCE.md](docs/PROVENANCE.md).
