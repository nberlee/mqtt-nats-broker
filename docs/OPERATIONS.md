# Operations

Running the broker: what it reports, how it starts and stops, how it scales, and what
it keeps in JetStream. Settings are in [Configuration](CONFIGURATION.md); symptoms and
their causes are in [Troubleshooting](TROUBLESHOOTING.md).

## Health and readiness

The broker serves two endpoints on `-http-addr` (default `:8080`; empty disables both).
Keep that port off any Service the clients use.

| Endpoint | Answers |
|---|---|
| `/healthz` | `200 ok` whenever the HTTP server is running |
| `/readyz` | `200 ready` while the MQTT listener is up and the broker's NATS service connection is connected; `503 nats disconnected` while that connection is down; `503 draining` before startup has finished and from the first shutdown signal on |

The HTTP server starts once the NATS connection is up (in the embedded topology, once
JetStream answers), so neither endpoint answers while the broker dials NATS. It does
answer while the MQTT listener starts, which includes checking the broker-owned streams
for up to 30 s, and `/readyz` reports `503 draining` until that is done. Readiness
reflects only the broker's own NATS connection: a client's per-connection NATS link
going down does not change it. A failure to bind the health port is logged
(`health server error`) and the broker keeps running without it.

There is no metrics endpoint.

## Startup

In order, and any failure stops the process with `fatal: <reason>` on stderr and exit
status 1:

1. The configuration is read and validated.
2. The Go memory limit is set ([Configuration](CONFIGURATION.md#go-memory-limit)).
3. The TLS material is read; an invalid certificate, key or CA is fatal.
4. In the embedded topology, the NATS server starts and the broker waits for its
   JetStream, logging `waiting for JetStream` every 10 s, with no overall timeout.
5. The broker connects to NATS and checks that JetStream answers
   (`connected to NATS backend`).
6. The health server starts.
7. The MQTT listener starts. With auth passthrough, the broker first asks NATS which
   account its own service identity belongs to; it then checks the ten broker-owned
   streams ([below](#the-ten-streams)) and creates any that do not exist yet.
8. TLS reloading starts, and `/readyz` turns ready.

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

Timestamps are in the host's time zone, which in the published images is UTC. Records
about one client connection start with its label, such as `cid:12`; no client
address is logged.

Records worth watching:

| Record | Meaning |
|---|---|
| `MQTT listener started` | the broker is serving; carries the address, the node name and whether TLS is on |
| `NATS service connection error` | the broker's own NATS connection reported an error |
| `MQTT TLS reload refused; keeping the active configuration` | a TLS update on disk is invalid ([TLS](TLS.md#certificate-rollover)) |
| `MQTT TLS certificate has expired and no valid replacement is on disk` | the served certificate has expired |
| `MQTT: refusing CONNECT for client ...` | a CONNECT was refused, with the reason ([Troubleshooting](TROUBLESHOOTING.md#a-client-is-refused-at-connect)) |
| `MQTT: the service connection reconnected into account ...` | the service identity's account changed; every auth-passthrough client is disconnected |

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
