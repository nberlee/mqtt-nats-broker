# mqtt-nats-bridge

A standalone, horizontally-scalable **MQTT 3.1.1 / 5.0** broker that uses an
**external NATS/JetStream cluster** as its highly-available backend for sessions,
retained messages, QoS1/2 persistence, and message routing.

[![CI](https://github.com/nberlee/mqtt-nats-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/nberlee/mqtt-nats-bridge/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> **Status: pre-release working broker.** There is no public bridge release or
> supported persisted-state format yet. The default build serves MQTT 3.1.1/5.0
> end-to-end — CONNECT, SUBSCRIBE, PUBLISH (QoS0), retained/sessions via
> JetStream, and auth passthrough (partially NATS-enforced; see F-079 through
> F-081) — verified with a real MQTT
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

**Requires nats-server 2.14.5 or newer.** Earlier releases are explicitly
unsupported — not degraded, not fallback-handled. The bridge's durability
guarantees rest on specific JetStream behaviors (per-subject CAS, interest
retention, deduplication, per-message TTL, double ack); those are asserted against
the supported line and their stream prerequisites are validated at startup rather
than worked around. The external cluster's version is an operator prerequisite:
every JetStream peer must meet the floor, and a mixed-version cluster is unsupported.
`go.mod` pins the exact version tested. See "JetStream behaviors this design depends on" in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the list a version bump has to
re-verify.

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
| `-js-domain` | `MQTTNB_JS_DOMAIN` | | JetStream domain; a single subject token. Baked into session subjects, so changing it later orphans existing sessions ([details](docs/ARCHITECTURE.md#jetstream-domain)) |
| `-stream-replicas` | `MQTTNB_STREAM_REPLICAS` | `0` (auto) | JetStream stream replicas |
| `-consumer-replicas` | `MQTTNB_CONSUMER_REPLICAS` | `0` | JetStream consumer replicas |
| `-host` | `MQTTNB_HOST` | `0.0.0.0` | MQTT listen host |
| `-port` | `MQTTNB_PORT` | `1883` | MQTT listen port |
| `-max-conns` | `MQTTNB_MAX_CONNS` | `0` (∞) | Max concurrent clients |
| `-connect-timeout` | `MQTTNB_CONNECT_TIMEOUT` | `5s` | How long an accepted connection may send no CONNECT before it is closed. Must be `> 0`: the bound is fail-closed and cannot be disabled ([details](docs/ARCHITECTURE.md#connection-liveness-bounds)) |
| `-auth-passthrough` | `MQTTNB_AUTH_PASSTHROUGH` | `true` | Dial per-client NATS conn from MQTT creds |
| `-default-user` / `-default-pass` | `MQTTNB_DEFAULT_USER` / `_PASS` | | NATS creds for anonymous MQTT clients |
| `-keepalive-max` | `MQTTNB_KEEPALIVE_MAX` | `0` (no override) | MQTT 5.0 Server Keep Alive cap (seconds) |
| `-response-info` | `MQTTNB_RESPONSE_INFO` | | MQTT 5.0 Response Information base topic |
| `-tls-cert` / `-tls-key` | `MQTTNB_TLS_CERT` / `_KEY` | | MQTT listener TLS (set `-port 8883`) |
| `-tls-ca` | `MQTTNB_TLS_CA` | | Client-CA → enables mTLS (requires cert+key) |
| `-log-level` | `MQTTNB_LOG_LEVEL` | `info` | `debug`\|`info`\|`warn`\|`error` |
| `-http-addr` | `MQTTNB_HTTP_ADDR` | `:8080` | Health/readiness (`/healthz`, `/readyz`) |

### TLS

Setting `-tls-cert` and `-tls-key` turns the MQTT listener into a TLS listener;
adding `-tls-ca` additionally requires and verifies client certificates (mTLS).
The broker serves **one** listener: `-port` selects it in either mode, and there
is no implicit port change when the TLS flags are set — TLS deployments should
pass `-port 8883` (the conventional MQTT-over-TLS port). Serving plaintext 1883
and TLS 8883 simultaneously is not supported.

TLS configuration fails closed: an unreadable or invalid certificate, key, or
client CA aborts startup before anything is bound, so a broken TLS setup can
never fall back to a plaintext listener. Certificate rollover without a restart
is not implemented yet — see [docs/ROADMAP.md](docs/ROADMAP.md).

## Authentication

Auth is **passed through to NATS**: an MQTT client's username/password dial a
dedicated `*nats.Conn`. Anonymous clients use the configured default credentials.

> **Do not treat this as tenant isolation yet.** Three gaps are open alpha
> blockers, and each is documented rather than fixed:
>
> - NATS enforces **subscribe filter admission** (a filter the account denies
>   outright becomes SUBACK 0x80) and per-message denial on the **QoS 0** path.
>   It does **not** filter a wildcard that overlaps a narrower deny on the
>   QoS 1/2 or retained paths — those ride the service connection.
>   [F-081](docs/findings/runtime-correctness.md#f-081).
> - **Publish denial is not enforced at all.** A core-NATS publish denial is
>   asynchronous, so the broker never observes it, and a denied publish is
>   accepted, stored and delivered.
>   [F-079](docs/findings/runtime-correctness.md#f-079).
> - In v1 all MQTT users must share the service connection's NATS account
>   (session/retained/QoS streams are account-scoped) — a constraint nothing
>   enforces. [F-080](docs/findings/runtime-correctness.md#f-080).

The broker reserves `$MQTT.>` for its own state and refuses client publishes,
subscribes, and Will topics that target it. Deny both directions on that prefix
for the MQTT client identity as well — the broker's checks bind MQTT clients,
the ACL binds anyone using the same credentials to reach NATS directly. See
[Reserved `$MQTT.>` namespace](docs/ARCHITECTURE.md#reserved-mqtt-namespace) for
the topic mapping and the exact permissions.

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
| Auth passthrough (per-client NATS authz — partial: see F-079, F-080, F-081) | ✅ | ✅ |
| HA cross-instance session takeover | ✅ | ✅ |
| WebSocket | roadmap | roadmap |

(MQTT 5 is always enabled — there is no experimental flag. All rows above are
covered by end-to-end tests driving real MQTT clients against the broker over
in-process JetStream — including load-balanced shared subscriptions, message
expiry, and reconnect-to-a-different-instance takeover.)

## Roadmap

The authoritative pre-alpha release checklist is maintained in
[docs/ROADMAP.md](docs/ROADMAP.md).

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
