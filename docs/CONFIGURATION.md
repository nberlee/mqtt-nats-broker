# Configuration

Every setting is a command-line flag with an `MQTTNB_`-prefixed environment variable
equivalent. Installation and the NATS prerequisites are in
[Installation](INSTALLATION.md); TLS is in [TLS](TLS.md).

## Flags and environment variables

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `-nats` | `MQTTNB_NATS_URLS` | `nats://127.0.0.1:4222` | Comma-separated NATS URLs, without credentials (refused: give them with `-nats-user`/`-nats-pass` or `-nats-creds`) |
| `-nats-embedded-config` | `MQTTNB_NATS_EMBEDDED_CONFIG` | | Run an embedded nats-server from this native configuration file instead of connecting to `-nats` ([details](ARCHITECTURE.md#embedded-nats)) |
| `-nats-creds` | `MQTTNB_NATS_CREDS` | | NATS `.creds` file (service conn) |
| `-nats-user` | `MQTTNB_NATS_USER` | | NATS user (service conn); keep URL credential-less under auth passthrough |
| `-nats-pass` | `MQTTNB_NATS_PASS` | | NATS password (service conn) |
| `-nats-name` | `MQTTNB_NATS_NAME` | `mqtt-nats-broker` | Connection name |
| `-js-domain` | `MQTTNB_JS_DOMAIN` | | JetStream domain; a single subject token. Baked into session subjects, so changing it later orphans existing sessions ([details](ARCHITECTURE.md#jetstream-domain)) |
| `-stream-replicas` | `MQTTNB_STREAM_REPLICAS` | none, required | JetStream replicas of the seven broker-owned streams: the JetStream cluster size (normally 3 for HA), `1` for a single server. Every existing broker-owned stream must have exactly this count ([details](#stream-replicas)) |
| `-host` | `MQTTNB_HOST` | `0.0.0.0` | MQTT listen host |
| `-port` | `MQTTNB_PORT` | `1883` | MQTT listen port |
| `-max-conns` | `MQTTNB_MAX_CONNS` | `0` (∞) | Max concurrent clients |
| `-connect-timeout` | `MQTTNB_CONNECT_TIMEOUT` | `5s` | How long an accepted connection may send no CONNECT before it is closed. Must be `> 0`: the bound is fail-closed and cannot be disabled ([details](ARCHITECTURE.md#connection-liveness-bounds)) |
| `-drain-delay` | `MQTTNB_DRAIN_DELAY` | `0` (immediate) | How long after SIGTERM the broker waits, with `/readyz` already answering 503, before it disconnects clients; a second signal ends the wait. Set it behind a load balancer or a Kubernetes Service ([Operations](OPERATIONS.md#shutdown)). Must be `>= 0` |
| `-auth-passthrough` | `MQTTNB_AUTH_PASSTHROUGH` | `true` | Dial per-client NATS conn from MQTT creds; costs one NATS connection per client ([details](#what-auth-passthrough-costs)) |
| `-max-ack-pending` | `MQTTNB_MAX_ACK_PENDING` | `1024` | QoS 1/2 push window: deliveries a subscription's consumer sends without an acknowledgement, `1`-`65535`. Applies to consumers created from then on, and to a shared group's durable on a fresh join ([details](#the-qos-12-push-window)) |
| `-default-user` | `MQTTNB_DEFAULT_USER` | | NATS user for anonymous MQTT clients |
| `-default-pass` | `MQTTNB_DEFAULT_PASS` | | NATS password for anonymous MQTT clients |
| `-keepalive-max` | `MQTTNB_KEEPALIVE_MAX` | `0` (no override) | MQTT 5.0 Server Keep Alive cap (seconds) |
| `-response-info` | `MQTTNB_RESPONSE_INFO` | | MQTT 5.0 Response Information base topic |
| `-tls-cert` | `MQTTNB_TLS_CERT` | | MQTT listener TLS certificate (set `-port 8883`) |
| `-tls-key` | `MQTTNB_TLS_KEY` | | MQTT listener TLS key |
| `-plain-port` | `MQTTNB_PLAIN_PORT` | `0` (off) | Additional plaintext MQTT listener beside the TLS `-port` (requires `-tls-cert`/`-tls-key`). Credentials and payloads cross it in clear, and no client certificate is checked on it |
| `-tls-ca` | `MQTTNB_TLS_CA` | | Client-CA → enables mTLS (requires cert+key) |
| `-tls-reload-interval` | `MQTTNB_TLS_RELOAD_INTERVAL` | `10s` | How often the TLS files are re-read for a renewal. Must be `> 0` |
| `-ws-port` | `MQTTNB_WS_PORT` | `0` (off) | MQTT over WebSocket listen port; the other `-ws-*` flags require it |
| `-ws-host` | `MQTTNB_WS_HOST` | the `-host` value | MQTT over WebSocket listen host |
| `-ws-path` | `MQTTNB_WS_PATH` | `/mqtt` | Request path served, exact and as escaped on the wire |
| `-ws-handshake-timeout` | `MQTTNB_WS_HANDSHAKE_TIMEOUT` | `5s` | Bound on the WebSocket opening handshake. Must be `> 0` |
| `-ws-no-tls` | `MQTTNB_WS_NO_TLS` | `false` | Plaintext WebSocket although TLS is configured, for a TLS-terminating ingress |
| `-ws-allowed-origins` | `MQTTNB_WS_ALLOWED_ORIGINS` | (same-origin) | Comma-separated browser origins, `scheme://host[:port]`, replacing the same-origin default |
| `-log-level` | `MQTTNB_LOG_LEVEL` | `info` | `debug`\|`info`\|`warn`\|`error`, for every record the broker writes ([Operations](OPERATIONS.md#logging)) |
| `-http-addr` | `MQTTNB_HTTP_ADDR` | `:8080` | Health and readiness, `/healthz` and `/readyz`; empty disables them. There is no metrics endpoint ([Operations](OPERATIONS.md#health-and-readiness)) |
| `-pprof-addr` | `MQTTNB_PPROF_ADDR` | (off) | Serve Go runtime profiles (`net/http/pprof`) on this address, a literal loopback IP and port such as `127.0.0.1:6060`; any other host is refused ([Operations](OPERATIONS.md#profiling)) |

## How flags and the environment combine

The environment is read first and the command line is laid over it: a flag overrides its
environment variable.

A variable that does not parse as its setting's kind (`MQTTNB_PORT=abc`,
`MQTTNB_AUTH_PASSTHROUGH=maybe`, `MQTTNB_DRAIN_DELAY=10`, or an empty integer, boolean or
duration) aborts startup with `invalid MQTTNB_<NAME>: ...`, even when the matching flag is
also given, because the environment is read before the flags are. A string variable takes
any value, the empty string included.

An `MQTTNB_` variable the broker does not define is not read and draws no message: check
the names you set against the table above.

A flag the broker does not define, or a flag value that does not parse, is refused as
`invalid command line: <kind> (-<flag>)`, without echoing the value.

After both are read the whole configuration is validated, and startup stops on the
first inconsistency; [Troubleshooting](TROUBLESHOOTING.md#the-broker-does-not-start)
lists the messages.

The settings, their names and defaults, and the Helm chart's values schema are the
broker's configuration surface; how it is defined, pinned and changed is in
[Architecture](ARCHITECTURE.md#the-configuration-surface).

## The QoS 1/2 push window

`-max-ack-pending` is the `MaxAckPending` of every QoS 1/2 consumer the broker creates:
how many deliveries JetStream sends a subscription without an acknowledgement. It is also
the session's in-flight cap (lowered further by an MQTT 5.0 client's Receive Maximum), and
it is what scales the memory a slow subscriber costs: deliveries the broker has not yet
processed wait decoded in the NATS client library, so their scale is subscriptions x push
window x message size. That is a scale, not a ceiling; [Conformance](CONFORMANCE.md#a-slow-subscriber-can-hold-the-client-librarys-pending-buffer-per-subscription)
states what bounds it and what does not. A lower value trades per-subscriber throughput for
less outstanding data.

`-max-ack-pending` supplies the push window for newly created QoS 1/2 consumers. A new
non-shared subscription consumer's window is reduced further by the client's Receive
Maximum when that is lower. Existing non-shared subscription and PUBREL consumers retain
their configured windows across reconnects, takeovers and restarts; recreating a missing
consumer uses the running broker's setting.

A fresh shared-group join creates or updates the group durable using the joining broker's
`-max-ack-pending` value, even when that durable already exists. This changes the window
for the whole group. Resuming an existing membership adopts the live durable's
configuration; it does not restore that member's earlier window. Use the same setting on
all brokers serving the group to avoid join-driven changes. If settings differ, the latest
applied create-or-update determines the group window.

The session's local in-flight cap follows the running broker's setting, reduced further by
the client's Receive Maximum. Lowering that cap does not necessarily lower an existing
durable's JetStream push window.

The 65535 cumulative check is an admission check against charges recorded by the current
in-memory session object, including charges for PUBREL consumer creation and shared-group
joins. Recorded charges are refunded when released. Restoring an existing consumer does
not add its window to this accounting, and subsequent changes to a shared durable's window
do not recompute earlier members' charges. Consequently, this check does not cap the sum
of all live consumer windows belonging to a persistent session.

## What auth passthrough costs

With `-auth-passthrough` (the default) every connected MQTT client has its own NATS
connection, dialed with its credentials: its read and write buffers and two goroutines in
the broker, and a client on the NATS server (in [embedded mode](ARCHITECTURE.md#embedded-nats)
that client lives in the same process, behind an in-memory pipe). That is a large share of
a connected session's memory. `-auth-passthrough=false` puts every client on the shared
service connection instead, and gives up per-client NATS authorization
([Authentication](AUTHENTICATION.md)).

## Stream replicas

`-stream-replicas` is the JetStream replica count of the seven streams the broker creates
in its account, and every durable consumer inherits it. It is the setting that makes
broker state survive the loss of a NATS server, so an HA deployment sets it explicitly:
normally `3` on a three-server JetStream cluster.

There is no default. The broker cannot see the size of the NATS cluster, so it does not
choose a count: startup refuses a missing count, `0` or a negative value before it
connects to NATS. Use `1` for a single NATS server.

The count is also checked: at startup every existing broker-owned stream must have
exactly that many replicas, or the broker refuses to start. Changing the setting
therefore does not change existing streams. The supported way to move to another count
is the one for any nonconforming stream bundle: stop every broker for the account,
delete all seven streams, and restart with the new value. That discards every session,
retained message and in-flight QoS state ([Operations](OPERATIONS.md#the-seven-streams)).

In the Helm chart the embedded topology sets the count itself, `1` for one server and
the peer count for a cluster; the external topology requires `broker.streamReplicas`
and refuses to render without it, so set it to the NATS cluster size there
([Kubernetes](KUBERNETES.md)). The embedded image sets no count either, because a mounted
cluster configuration would inherit it: state `1` for the one server it ships.

## Go memory limit

At startup the broker sets the Go soft memory limit to 90 percent of the container's
memory limit (cgroup v1 or v2), so the GC tightens before the kernel's OOM killer acts.
`GOMEMLIMIT` in the environment overrides this entirely; `AUTOMEMLIMIT=<ratio>` changes
the fraction and `AUTOMEMLIMIT=off` disables it. With no container limit the Go limit is
unlimited. A failure to read the limit is logged and does not stop startup
([details](ARCHITECTURE.md#the-go-memory-limit-follows-the-container-limit)). These
two variables have no flag and no `MQTTNB_` form.

## MQTT over WebSocket

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
negotiated ([details](ARCHITECTURE.md#mqtt-over-websocket)).

## Fixed bounds

These have no setting:

| Bound | Value | What it bounds |
|---|---|---|
| JetStream API request timeout | 5 s | every request the broker makes to the JetStream API |
| TLS handshake | 2 s | the MQTT listener's TLS handshake |
| Write deadline | 10 s | a write to a client; a client that cannot absorb it is disconnected |
| `$SYS.REQ.USER.INFO` | 2 s | the identity lookup at CONNECT under auth passthrough |
| Client drain at shutdown | 10 s | writing the server DISCONNECT to connected clients |
