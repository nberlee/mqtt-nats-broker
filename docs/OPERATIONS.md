# Operations

Running the broker: what it reports, how it starts and stops, how it scales, and what
it keeps in JetStream. Settings are in [Configuration](CONFIGURATION.md); symptoms and
their causes are in [Troubleshooting](TROUBLESHOOTING.md).

## Health and readiness

The broker serves two endpoints on `-http-addr` (default `:8080`; empty disables both).
Keep that port off any Service the clients use. Both answers, the exit status and the
records below are the broker's operational interface: one definition
(`internal/operational`) states them, and they change only with it
([details](ARCHITECTURE.md#the-operational-interface)).

The health server is bound first, before the embedded NATS server starts and before NATS
is dialed, and answers until the broker has closed its NATS connection. A port that
cannot be bound stops startup (exit 1).

`/healthz` answers `200 ok` whenever the health server serves.

### Readiness answers

| `/readyz` answers | When |
|---|---|
| `503 draining` | a shutdown signal has been received, whether or not the broker served |
| `503 starting` | the broker has not served yet: the JetStream wait, the dial, the stream checks, the MQTT initialization and its retries, and a failed start's unwinding |
| `200 ready` | the broker serves and its NATS service connection is connected |
| `503 nats disconnected` | the broker serves and its NATS service connection is down |

Readiness reflects the broker's own NATS connection, not its identity: a client's
per-connection NATS link going down does not change it, and a service connection that
reconnected but could not be identified answers `200 ready` while every auth-passthrough
CONNECT is refused (the `could not be identified` record below says so). There is no
metrics endpoint.

## Startup

In order; a failure stops the process with `fatal: <reason>` on stderr and exit status
1, and a shutdown signal during startup stops it with exit status 0
([Exit status](#exit-status)):

1. The configuration is read and validated.
2. The Go memory limit is set ([Configuration](CONFIGURATION.md#go-memory-limit)).
3. The TLS material is read; an invalid certificate, key or CA is fatal.
4. The health server starts; `/readyz` answers `503 starting`.
5. In the embedded topology, the NATS server starts and the broker waits for its
   JetStream, logging `waiting for JetStream` every 10 s, with no overall timeout.
6. The broker connects to NATS, refuses a server below the NATS floor or with header
   support disabled, checks that JetStream answers, and refuses a JetStream API level below
   the floor ([Troubleshooting](TROUBLESHOOTING.md#the-broker-does-not-start)). None of
   these refusals is waited on. `connected to NATS backend` reports `server_version` and
   `jetstream_api_level`.
7. The MQTT listener starts. With auth passthrough, the broker first asks NATS which
   account its own service identity belongs to; it then checks the ten broker-owned
   streams ([below](#the-ten-streams)), waiting up to 30 s on a partial set, and creates
   any that do not exist yet. An initialization JetStream does not answer yet is retried
   with no overall timeout.
8. TLS reloading starts, and `/readyz` turns `200 ready`.

## Exit status

| Status | Meaning |
|---|---|
| 0 | the broker stopped as asked: a shutdown signal, at any phase after its handlers are registered, and the shutdown or the startup's unwinding returned; also `-h`. Not a promise that every buffered publication reached NATS: the service connection's close is bounded |
| 1 | the broker could not start, or the embedded NATS server stopped it: `fatal: <reason>` on stderr, except the embedded server's own fatal error, which is an `ERROR` record with `component: nats-server`. A startup step that does not observe a signal (the NATS dial, the first service-identity request, both bounded) reports its own failure here |

A signal that arrives before the broker has registered its handlers (while it reads its
configuration and TLS material), a later signal once the drain delay's wait has ended (or
any second signal without a drain delay), a Go runtime panic (status 2) and the
orchestrator's SIGKILL end the process outside this table.

## Shutdown

On SIGTERM or SIGINT the broker logs `shutdown signal received, draining` and `/readyz`
turns `503 draining` at once, while the MQTT listener still accepts. With `-drain-delay`
set it then waits that long, so a load balancer or a Kubernetes Service stops routing
to it; a second signal ends the wait early. Behind a load balancer set it: the default
is `0`, and the Helm chart and the example manifests set 10 s.

Then the broker closes its listeners and drains: every connected client gets a server
DISCONNECT with reason code 0x8B (server shutting down) if it speaks MQTT 5.0, and its
socket is closed; writing that takes at most 10 s. The broker then stops dispatching
Wills and waits for the Will publications already running, owed work gets its last
attempt while JetStream still answers, and the broker releases its
NATS state and closes its service connection, waiting at most 20 s for it to flush. In
the embedded topology the NATS server stops last. The phases have no overall timeout of
their own: the orchestrator's termination grace is the final bound, and the exit status
of a completed shutdown is 0 ([details](ARCHITECTURE.md#resource-lifetime-and-shutdown-release)).

Size the termination grace to cover the drain delay plus those budgets: the chart and
the example manifests use 60 s, 75 s in the embedded topology, and raising the drain
delay means raising the grace with it.

## Scaling and takeover

Broker instances are stateless: every durable fact lives in JetStream, so run as many
as the connection load needs behind a TCP load balancer and size the NATS cluster for
the state. A client may reconnect to any instance; a session held by another instance
is taken over through NATS, and the old connection is closed with reason code 0x8E
(session taken over). A WebSocket client and a TCP client with the same client
identifier take each other over the same way. A delayed Will is fired by whichever
broker is alive when it falls due ([details](ARCHITECTURE.md#high-availability)).

Each instance needs a unique node name: the host name, or `-nats-name` where the host
name is empty. Pods and containers have unique host names by default.

## The ten streams

The broker keeps its state in ten JetStream streams in the account its service
identity belongs to: `$MQTT_sess`, `$MQTT_willq`, `$MQTT_teardown`, `$MQTT_msgs`,
`$MQTT_qos2in`, `$MQTT_qos2assign`, `$MQTT_inflight`, `$MQTT_shared`, `$MQTT_out` and
`$MQTT_rmsgs`. They are created by the first broker to start and are **one unit**: they
are created, checked, recreated, backed up and restored together
([details](ARCHITECTURE.md#stream-prerequisites)).

At every start the broker checks the unit before it serves: all ten present, or none
(it then creates them); and each one's configuration as the broker requires it. A
partial set is waited on for up to 30 s, since another broker may be creating it, and
then refused. Nothing is repaired in place: a stream that does not conform refuses the
start, with a message naming the stream and the reason.

Do not:

- delete, recreate or edit some of the streams while brokers run, or start brokers
  while streams are being deleted. Running brokers keep serving, but every broker that
  starts afterwards refuses;
- change `-js-domain` on an existing deployment: the domain is part of every session's
  subject, so the existing sessions are orphaned
  ([details](ARCHITECTURE.md#jetstream-domain));
- change `-stream-replicas` and expect existing streams to follow
  ([Configuration](CONFIGURATION.md#stream-replicas)).

**Recreating the unit** is the one supported repair, and the way to change the replica
count: stop every broker for the account, delete all ten streams, start the brokers.
It discards every session, retained message and in-flight QoS state.

**Backup and restore** must capture the ten streams at one coherent instant; the broker
cannot detect a restore whose streams come from different moments. A supported
procedure is open work in the roadmap.

## Logging

The broker writes one stream of JSON records to stdout, at `-log-level`
(`debug`, `info`, `warn` or `error`), the MQTT protocol core's records included. In the
embedded topology the NATS server's records join the same stream, marked
`component: nats-server`. Its debug and trace output needs both gates: the NATS
configuration's `debug` or `trace` setting, and `-log-level debug`, since both arrive
as debug records. A startup failure is
the exception: it is printed to stderr as `fatal: <reason>`.

Records about one client connection start with its label, such as `cid:12`; no client
address is logged.

### Record envelope

| Key | Value |
|---|---|
| `time` | RFC 3339 with fractional seconds, in the host's time zone (UTC in the published images) |
| `level` | `DEBUG`, `INFO`, `WARN` or `ERROR` |
| `msg` | the record's message; frozen only for the records below |
| `component` | `nats-server` on the embedded NATS server's records, absent otherwise |

### Field kinds

| Kind | Encoded as |
|---|---|
| string | a JSON string |
| integer | a JSON number |
| boolean | a JSON boolean |
| duration | a JSON number of nanoseconds |
| time | an RFC 3339 JSON string |
| text | a JSON string; its wording is not part of the interface |

### Records

Alert on these: their `msg`, `level` and fields are the interface, matched exactly. Every
other record's text, the core's per-connection records included, can change in any
release.

| `msg` | `level` | Fields | When |
|---|---|---|---|
| `starting mqtt-nats-broker` | `INFO` | `version` (string) | once, first |
| `health server listening` | `INFO` | `addr` (string) | once, when the health port is bound |
| `health server error` | `ERROR` | `err` (text) | when the health server stops serving other than at shutdown |
| `profiler listening` | `INFO` | `addr` (string) | once, with `-pprof-addr` |
| `profiler listener could not bind` | `ERROR` | `addr` (string), `err` (text) | once, with `-pprof-addr`; the broker starts without the profiler |
| `profiler error` | `ERROR` | `err` (text) | when the profiler stops serving other than at shutdown |
| `embedded NATS server ready` | `INFO` | `server` (string), `config` (string) | once, in the embedded topology |
| `waiting for JetStream` | `INFO` | `err` (text) | at most every 10 s while the embedded server's JetStream does not answer |
| `connected to NATS backend` | `INFO` | `urls` (string), `in_process` (boolean), `server` (string), `server_version` (string), `jetstream_api_level` (integer), `cluster` (string), `js_domain` (string) | once, when the service connection passed the NATS floor and JetStream answered |
| `MQTT initialization did not complete; retrying` | `WARN` | `attempt` (integer), `err` (text) | at most every 10 s while the MQTT initialization is retried |
| `shutdown signal received during startup` | `INFO` | `err` (text) | once, when a shutdown signal ends startup before the broker served |
| `MQTT listener started` | `INFO` | `addr` (string), `node` (string), `tls` (boolean) | once, when the broker serves |
| `MQTT over WebSocket listener started` | `INFO` | `addr` (string), `tls` (boolean) | once, with `-ws-port` |
| `shutdown signal received, draining` | `INFO` | none | once, at the first shutdown signal after the broker served |
| `draining, waiting for endpoint deregistration` | `INFO` | `delay` (duration) | once, with `-drain-delay` |
| `NATS service connection error` | `WARN` | `err` (text) | per asynchronous error the service connection reports |
| `MQTT: the service connection reconnected but could not be identified; no CONNECT is accepted` | `ERROR` | `err` (text) | per reconnect whose identity request fails; `/readyz` still answers `200 ready` |
| `MQTT: the service connection dropped while it was being re-identified; the account stays unknown` | `WARN` | none | per occurrence |
| `MQTT: the service connection reconnected into another account; disconnecting every auth-passthrough client` | `ERROR` | `account` (string), `previous_account` (string) | per occurrence |
| `MQTT: refusing CONNECT: the service connection's account is not currently established` | `WARN` | none | per CONNECT refused for it |
| `MQTT: refusing CONNECT` | `WARN` | `client_id` (string), `reason` (text) | per CONNECT whose identity cannot be established or is bound to another account |
| `MQTT TLS configuration reloaded` | `INFO` | `subject` (string), `serial` (string), `not_after` (time), `generation` (integer) | per applied TLS reload |
| `MQTT TLS reload refused; keeping the active configuration` | `ERROR` | `err` (text) | once per run of identical refusals, not per attempt |
| `MQTT TLS certificate has expired and no valid replacement is on disk` | `WARN` | `subject` (string), `serial` (string), `not_after` (time) | once per served certificate, not repeated |
| `NATS service connection did not finish closing in time; closing it now` | `WARN` | `wait` (duration) | at most once, at shutdown |
| `NATS service connection closed` | `INFO` | none | once, at shutdown |

A refused CONNECT is explained in
[Troubleshooting](TROUBLESHOOTING.md#a-client-is-refused-at-connect), a refused TLS reload
in [TLS](TLS.md#certificate-rollover).

## Memory and connection limits

The Go memory limit follows the container's memory limit
([Configuration](CONFIGURATION.md#go-memory-limit)); it paces the garbage collector and
does not prevent an out-of-memory kill. `-max-conns` caps the clients of all listeners
together; a connection over the cap is closed before its CONNECT is read, with no
CONNACK and no log record. `-connect-timeout` closes a connection that sends no CONNECT
in time. After CONNECT, a client is held to its Keep Alive. An MQTT 3.1.1 client that
asked for Keep Alive 0 is never timed out. `-keepalive-max` applies to MQTT 5.0 clients
only: one asking for more than the maximum, or for 0, is given the maximum as its Server
Keep Alive and disconnected when it stays silent longer; without `-keepalive-max` an MQTT
5.0 client asking for 0 is never timed out either.

A connected session's memory is mostly its JetStream consumers (one per QoS 1/2
subscription), its NATS connection under auth passthrough
([Configuration](CONFIGURATION.md#what-auth-passthrough-costs)), and the deliveries a slow
subscriber has not taken yet, scaled by `-max-ack-pending`
([Configuration](CONFIGURATION.md#the-qos-12-push-window)). A disconnected client's own
memory is released when it disconnects, even while its persistent session stays.

## Profiling

`-pprof-addr 127.0.0.1:6060` serves the Go runtime profiles (`/debug/pprof/`) on a
listener of its own. The address must be a literal loopback IP: profiles expose process
memory, so they are never served to the network or on the health port. In Kubernetes, reach
it with a port-forward and take a heap profile:

```bash
kubectl port-forward pod/<broker-pod> 6060:6060
go tool pprof http://127.0.0.1:6060/debug/pprof/heap
```

The profiler is off by default and stops with the health endpoint at shutdown. A port that
cannot be bound is logged and the broker starts without it.
