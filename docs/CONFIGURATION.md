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
| `-nats-user` / `-nats-pass` | `MQTTNB_NATS_USER` / `_PASS` | | NATS user/password (service conn); keep URL credential-less under auth passthrough |
| `-nats-name` | `MQTTNB_NATS_NAME` | `mqtt-nats-broker` | Connection name |
| `-js-domain` | `MQTTNB_JS_DOMAIN` | | JetStream domain; a single subject token. Baked into session subjects, so changing it later orphans existing sessions ([details](ARCHITECTURE.md#jetstream-domain)) |
| `-stream-replicas` | `MQTTNB_STREAM_REPLICAS` | `0` | JetStream replicas of the ten broker-owned streams. Set it to the JetStream cluster size for HA (normally 3). `0` creates new streams with one replica and does not check an existing stream's count; it does not discover the cluster size ([details](#stream-replicas)) |
| `-host` | `MQTTNB_HOST` | `0.0.0.0` | MQTT listen host |
| `-port` | `MQTTNB_PORT` | `1883` | MQTT listen port |
| `-max-conns` | `MQTTNB_MAX_CONNS` | `0` (∞) | Max concurrent clients |
| `-connect-timeout` | `MQTTNB_CONNECT_TIMEOUT` | `5s` | How long an accepted connection may send no CONNECT before it is closed. Must be `> 0`: the bound is fail-closed and cannot be disabled ([details](ARCHITECTURE.md#connection-liveness-bounds)) |
| `-drain-delay` | `MQTTNB_DRAIN_DELAY` | `0` (immediate) | How long after SIGTERM the broker waits, with `/readyz` already answering 503, before it disconnects clients; a second signal ends the wait. Set it behind a load balancer or a Kubernetes Service ([Operations](OPERATIONS.md#shutdown)). Must be `>= 0` |
| `-auth-passthrough` | `MQTTNB_AUTH_PASSTHROUGH` | `true` | Dial per-client NATS conn from MQTT creds |
| `-default-user` / `-default-pass` | `MQTTNB_DEFAULT_USER` / `_PASS` | | NATS creds for anonymous MQTT clients |
| `-keepalive-max` | `MQTTNB_KEEPALIVE_MAX` | `0` (no override) | MQTT 5.0 Server Keep Alive cap (seconds) |
| `-response-info` | `MQTTNB_RESPONSE_INFO` | | MQTT 5.0 Response Information base topic |
| `-tls-cert` / `-tls-key` | `MQTTNB_TLS_CERT` / `_KEY` | | MQTT listener TLS (set `-port 8883`) |
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

## How flags and the environment combine

The environment is read first and the command line is laid over it: a flag overrides a
successfully parsed environment value. Two exceptions follow from that order:

- A malformed duration variable (`MQTTNB_CONNECT_TIMEOUT`, `MQTTNB_DRAIN_DELAY`,
  `MQTTNB_TLS_RELOAD_INTERVAL`, `MQTTNB_WS_HANDSHAKE_TIMEOUT`) aborts startup with
  `invalid MQTTNB_...`, even when the matching flag is also given, because it is read
  before the flags are.
- A malformed integer or boolean variable (`MQTTNB_PORT=abc`,
  `MQTTNB_AUTH_PASSTHROUGH=maybe`) is ignored and the default applies, with no message.
  Check the values you set.

A flag the broker does not define, or a flag value that does not parse, is refused as
`invalid command line: <kind> (-<flag>)`, without echoing the value.

After both are read the whole configuration is validated, and startup stops on the
first inconsistency; [Troubleshooting](TROUBLESHOOTING.md#the-broker-does-not-start)
lists the messages.

## Stream replicas

`-stream-replicas` is the JetStream replica count of the ten streams the broker creates
in its account, and every durable consumer inherits it. It is the setting that makes
broker state survive the loss of a NATS server, so an HA deployment sets it explicitly:
normally `3` on a three-server JetStream cluster.

`0`, the default, is not automatic. It creates any stream that does not exist yet with
**one** replica, and it does not check the replica count of streams that already exist.
The broker cannot see the size of the NATS cluster and does not try to. On a clustered
NATS a one-replica stream lives on one server: its data is unavailable while that server
is down and lost with its storage. `docs/CONFORMANCE.md` states this as a limitation.

A nonzero value is also checked: at startup every existing broker-owned stream must have
exactly that many replicas, or the broker refuses to start. Changing the setting
therefore does not change existing streams. The supported way to move to another count
is the one for any nonconforming stream bundle: stop every broker for the account,
delete all ten streams, and restart with the new value. That discards every session,
retained message and in-flight QoS state ([Operations](OPERATIONS.md#the-ten-streams)).

In the Helm chart the embedded topology sets the count to its peer count; the external
topology leaves `broker.streamReplicas` at `0`, so set it to the NATS cluster size there
([Kubernetes](KUBERNETES.md)).

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
