# mqtt-nats-broker

A standalone, horizontally-scalable **MQTT 3.1.1 / 5.0** broker that uses an
**external NATS/JetStream cluster** as its highly-available backend for sessions,
retained messages, QoS1/2 persistence, and message routing.

[![CI](https://github.com/nberlee/mqtt-nats-broker/actions/workflows/ci.yml/badge.svg)](https://github.com/nberlee/mqtt-nats-broker/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> **Status: pre-release working broker.** There is no public broker release or
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

**Requires nats-server 2.15.1 or newer.** Earlier releases are explicitly
unsupported — not degraded, not fallback-handled. The broker's durability
guarantees rest on specific JetStream behaviors (per-subject CAS, interest
retention, deduplication, per-message TTL, double ack); those are asserted against
the supported line and their stream prerequisites are validated at startup rather
than worked around. The external cluster's version is an operator prerequisite:
every JetStream peer must meet the floor, and a mixed-version cluster is unsupported.
One server **option** is a prerequisite too: configure
`jetstream { limits { default_max_consumers: -1 } }` on every peer, or give the account
tier an explicit positive `max_consumers` above your working set. From 2.15.0 the server
otherwise caps each stream at 1000 consumers, and asking for unlimited arms that cap
rather than opting out of it — the broker cannot detect the setting and cannot refuse
early (F-095).
`go.mod` pins the exact version tested. See "JetStream behaviors this design depends on" in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the list a version bump has to
re-verify.

Run a broker + NATS locally:

```sh
docker compose -f test/conformance/docker-compose.yml up --build
```

Or run the binary against an existing NATS cluster:

```sh
go build ./cmd/mqtt-nats-broker
./mqtt-nats-broker -nats nats://nats-1:4222,nats://nats-2:4222 -port 1883
```

Container image (multi-arch amd64/arm64), for an external NATS/JetStream deployment. It is
`FROM scratch` (the binary and the system CA bundle only), runs as the numeric user
`65532:65532`, and supports a read-only root filesystem:

```sh
docker run --rm --read-only --cap-drop ALL -p 1883:1883 -p 8080:8080 \
  -e MQTTNB_NATS_URLS=nats://nats:4222 \
  ghcr.io/nberlee/mqtt-nats-broker:latest
```

A `tls://` NATS URL is verified against the image's CA bundle. For a NATS server under a
private CA, mount the CA and name it with `SSL_CERT_FILE` (for example
`-e SSL_CERT_FILE=/etc/nats-ca/ca.crt`). The image sets no `HOME`, so `-nats-creds` takes
an absolute path. Log timestamps are UTC
([details](docs/ARCHITECTURE.md#the-runtime-image)).

## Configuration

Every flag has an `MQTTNB_`-prefixed environment variable equivalent; flags win.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `-nats` | `MQTTNB_NATS_URLS` | `nats://127.0.0.1:4222` | Comma-separated NATS URLs |
| `-nats-creds` | `MQTTNB_NATS_CREDS` | | NATS `.creds` file (service conn) |
| `-nats-user` / `-nats-pass` | `MQTTNB_NATS_USER` / `_PASS` | | NATS user/password (service conn); keep URL credential-less under auth passthrough |
| `-nats-name` | `MQTTNB_NATS_NAME` | `mqtt-nats-broker` | Connection name |
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
| `-tls-reload-interval` | `MQTTNB_TLS_RELOAD_INTERVAL` | `10s` | How often the TLS files are re-read for a renewal. Must be `> 0` |
| `-ws-port` | `MQTTNB_WS_PORT` | `0` (off) | MQTT over WebSocket listen port; the other `-ws-*` flags require it |
| `-ws-host` | `MQTTNB_WS_HOST` | the `-host` value | MQTT over WebSocket listen host |
| `-ws-path` | `MQTTNB_WS_PATH` | `/mqtt` | Request path served, exact and as escaped on the wire |
| `-ws-handshake-timeout` | `MQTTNB_WS_HANDSHAKE_TIMEOUT` | `5s` | Bound on the WebSocket opening handshake. Must be `> 0` |
| `-ws-no-tls` | `MQTTNB_WS_NO_TLS` | `false` | Plaintext WebSocket although TLS is configured, for a TLS-terminating ingress |
| `-ws-allowed-origins` | `MQTTNB_WS_ALLOWED_ORIGINS` | (same-origin) | Comma-separated browser origins, `scheme://host[:port]`, replacing the same-origin default |
| `-log-level` | `MQTTNB_LOG_LEVEL` | `info` | `debug`\|`info`\|`warn`\|`error` |
| `-http-addr` | `MQTTNB_HTTP_ADDR` | `:8080` | Health/readiness (`/healthz`, `/readyz`) |

### Go memory limit

At startup the broker sets the Go soft memory limit to 90 percent of the container's
memory limit (cgroup v1 or v2), so the GC tightens before the kernel's OOM killer acts.
`GOMEMLIMIT` in the environment overrides this entirely; `AUTOMEMLIMIT=<ratio>` changes
the fraction and `AUTOMEMLIMIT=off` disables it. With no container limit the Go limit is
unlimited. A failure to read the limit is logged and does not stop startup
([details](docs/ARCHITECTURE.md#the-go-memory-limit-follows-the-container-limit)).

### TLS

Setting `-tls-cert` and `-tls-key` turns the MQTT listener into a TLS listener;
adding `-tls-ca` additionally requires and verifies client certificates (mTLS).
The broker serves **one** MQTT-over-TCP listener: `-port` selects it in either mode, and there
is no implicit port change when the TLS flags are set — TLS deployments should
pass `-port 8883` (the conventional MQTT-over-TLS port). Serving plaintext 1883
and TLS 8883 simultaneously is not supported.

TLS configuration fails closed: an unreadable or invalid certificate, key, or
client CA aborts startup before anything is bound, so a broken TLS setup can
never fall back to a plaintext listener. A certificate outside its validity
window counts as invalid.

Certificates roll over without a restart. The broker re-reads the three files
every `-tls-reload-interval` and applies a complete, valid replacement to new
handshakes. Existing MQTT connections stay connected on the material they were
admitted under. Mount the files from a Kubernetes Secret, for example one
cert-manager renews, as a volume: the kubelet updates it in place, so no
Stakater Reloader or other rollout controller is needed. A `subPath` mount never
receives updates. An update that is incomplete, malformed, expired, or whose key
does not match its certificate is refused and logged, and the last valid
configuration stays active
([details](docs/ARCHITECTURE.md#tls-material-reload)).

### MQTT over WebSocket

`-ws-port` opens a second listener for MQTT over WebSocket, at `-ws-path`
(default `/mqtt`); clients must offer the `mqtt` subprotocol. It shares
`-max-conns`, sessions and takeover with the MQTT listener, and its TLS: with
`-tls-cert`/`-tls-key` set it serves secure WebSocket (`wss://`) from the same
material, reloaded with it, unless `-ws-no-tls` is set for an ingress that
terminates TLS (WebSocket clients then get neither encryption nor client-certificate
verification from the broker). Browser pages are admitted only from the
listener's own origin unless `-ws-allowed-origins` lists them; behind an ingress
that terminates TLS, list the page origins. Forwarding headers and the PROXY
protocol are not consumed, and no WebSocket extension (compression) is
negotiated ([details](docs/ARCHITECTURE.md#mqtt-over-websocket)).

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
| Shared subscriptions | — | ✅ QoS 0/1; QoS 2 experimental and not part of alpha |
| Will (+ delay) | ✅ | ✅ (delay) |
| Message / session expiry, reason codes, properties | — | ✅ |
| TLS / mTLS | ✅ | ✅ |
| Auth passthrough (per-client NATS authz — partial: see F-079, F-080, F-081) | ✅ | ✅ |
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
make full-suite    # ported upstream MQTT suite (tag mqttport_fulltests)
make docker        # build the container image
make image-smoke-external  # build it and run it as a pod would (needs Docker)
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License & attribution

Apache-2.0. Substantial portions are derived from
[nats-io/nats-server](https://github.com/nats-io/nats-server) (Apache-2.0); see
[NOTICE](NOTICE).
