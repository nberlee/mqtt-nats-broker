# mqtt-nats-bridge

A standalone, horizontally-scalable **MQTT 3.1.1 / 5.0** broker that uses an
**external NATS/JetStream cluster** as its highly-available backend for sessions,
retained messages, QoS1/2 persistence, and message routing.

[![CI](https://github.com/nberlee/mqtt-nats-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/nberlee/mqtt-nats-bridge/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> **Status: working broker.** The default build serves MQTT 3.1.1/5.0
> end-to-end — CONNECT, SUBSCRIBE, PUBLISH (QoS0), retained/sessions via
> JetStream, and NATS-enforced auth passthrough — verified with a real MQTT
> client in CI. The upstream regression suite runs in CI against the
> standalone broker (behind the `mqttport_fulltests` tag). See
> [docs/PORTING.md](docs/PORTING.md).

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

Run a broker + NATS locally:

```sh
docker compose -f test/conformance/docker-compose.yml up --build
```

Or run the binary against an existing NATS cluster:

```sh
go build ./cmd/mqtt-nats-bridge
./mqtt-nats-bridge -nats nats://nats-1:4222,nats://nats-2:4222 -port 1883
```

Container image (multi-arch amd64/arm64):

```sh
docker run --rm -p 1883:1883 -p 8080:8080 \
  -e MQTTNB_NATS_URLS=nats://nats:4222 \
  ghcr.io/nberlee/mqtt-nats-bridge:latest
```

## Configuration

Every flag has an `MQTTNB_`-prefixed environment variable equivalent; flags win.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `-nats` | `MQTTNB_NATS_URLS` | `nats://127.0.0.1:4222` | Comma-separated NATS URLs |
| `-nats-creds` | `MQTTNB_NATS_CREDS` | | NATS `.creds` file (service conn) |
| `-nats-user` / `-nats-pass` | `MQTTNB_NATS_USER` / `_PASS` | | NATS user/password (service conn); keep URL credential-less under auth passthrough |
| `-nats-name` | `MQTTNB_NATS_NAME` | `mqtt-nats-bridge` | Connection name |
| `-js-domain` | `MQTTNB_JS_DOMAIN` | | JetStream domain |
| `-stream-replicas` | `MQTTNB_STREAM_REPLICAS` | `0` (auto) | JetStream stream replicas |
| `-consumer-replicas` | `MQTTNB_CONSUMER_REPLICAS` | `0` | JetStream consumer replicas |
| `-host` | `MQTTNB_HOST` | `0.0.0.0` | MQTT listen host |
| `-port` | `MQTTNB_PORT` | `1883` | MQTT listen port |
| `-max-conns` | `MQTTNB_MAX_CONNS` | `0` (∞) | Max concurrent clients |
| `-auth-passthrough` | `MQTTNB_AUTH_PASSTHROUGH` | `true` | Dial per-client NATS conn from MQTT creds |
| `-default-user` / `-default-pass` | `MQTTNB_DEFAULT_USER` / `_PASS` | | NATS creds for anonymous MQTT clients |
| `-keepalive-max` | `MQTTNB_KEEPALIVE_MAX` | `0` (no override) | MQTT 5.0 Server Keep Alive cap (seconds) |
| `-response-info` | `MQTTNB_RESPONSE_INFO` | | MQTT 5.0 Response Information base topic |
| `-tls-cert` / `-tls-key` | `MQTTNB_TLS_CERT` / `_KEY` | | MQTT listener TLS |
| `-tls-ca` | `MQTTNB_TLS_CA` | | Client-CA → enables mTLS |
| `-log-level` | `MQTTNB_LOG_LEVEL` | `info` | `debug`\|`info`\|`warn`\|`error` |
| `-http-addr` | `MQTTNB_HTTP_ADDR` | `:8080` | Health/readiness (`/healthz`, `/readyz`) |

## Authentication

Auth is **passed through to NATS**: an MQTT client's username/password dial a
dedicated `*nats.Conn`, so NATS enforces that client's publish/subscribe
permissions on every topic. Anonymous clients use the configured default
credentials. In v1, all MQTT users share the service connection's NATS account
(session/retained/QoS streams are account-scoped).

## Feature matrix

| Feature | 3.1.1 | 5.0 |
|---|---|---|
| QoS 0 / 1 / 2 | ✅ | ✅ |
| Retained messages | ✅ | ✅ |
| Persistent sessions (JetStream) | ✅ | ✅ |
| Shared subscriptions | — | ✅ |
| Will (+ delay) | ✅ | ✅ (delay) |
| Message / session expiry, reason codes, properties | — | ✅ |
| TLS / mTLS | ✅ | ✅ |
| Auth passthrough (per-client NATS authz) | ✅ | ✅ |
| HA cross-instance session takeover | ✅ | ✅ |
| WebSocket | roadmap | roadmap |

(MQTT 5 is always enabled — there is no experimental flag. All rows above are
covered by end-to-end tests driving real MQTT clients against the broker over
in-process JetStream — including load-balanced shared subscriptions, message
expiry, and reconnect-to-a-different-instance takeover.)

## Roadmap

1. ✅ Port the protocol core off NATS-server internals; serve MQTT 3.1.1 + genuine 5.0 end-to-end ([docs/PORTING.md](docs/PORTING.md)).
2. ✅ Stored delivery, HA takeover, shared subscriptions, message expiry, wills — verified with real clients.
3. ✅ Conformance gate wired: `vibesrc/mqttconformance` passes **124/124** (3.1.1) and **106/106** (5.0). See [docs/CONFORMANCE.md](docs/CONFORMANCE.md).
4. MQTT-over-WebSocket transport.
5. split up mqtt golang files
6. Helm Chart published
7. Container image from scratch

## Development

```sh
make test          # unit + end-to-end tests (real MQTT client over JetStream)
make vet lint      # static analysis
make full-suite    # ported upstream MQTT suite (tag mqttport_fulltests)
make docker        # build the container image
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License & attribution

Apache-2.0. Substantial portions are derived from
[nats-io/nats-server](https://github.com/nats-io/nats-server) (Apache-2.0); see
[NOTICE](NOTICE).
