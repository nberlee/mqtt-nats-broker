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
   account its own service identity belongs to; it then checks the seven broker-owned
   streams ([below](#the-seven-streams)), waiting up to 30 s on a partial set, and creates
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

## The seven streams

The broker keeps its state in seven JetStream streams in the account its service
identity belongs to: `$MQTT_sess`, `$MQTT_willq`, `$MQTT_msgs`, `$MQTT_qos2in`,
`$MQTT_qos2assign`, `$MQTT_out` and `$MQTT_rmsgs`. They are created by
the first broker to start and are **one unit**: they are created, checked, recreated,
backed up and restored together ([details](ARCHITECTURE.md#stream-prerequisites)).
`$MQTT_sess` also carries the in-flight and shared-group records, which had streams of
their own (`$MQTT_inflight`, `$MQTT_shared`) before 2026-11-09. `$MQTT_teardown`, which
held the generation-retirement watermark, is retired too: the authority's lifecycle now
records it ([issue #133](https://github.com/nberlee/mqtt-nats-broker/issues/133)). An
account that still holds any of the three is refused at start: delete them together with
the unit.

At every start the broker checks the unit before it serves: all seven present, or none
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
count: stop every broker for the account, delete all seven streams, start the brokers.
It discards every session, retained message and in-flight QoS state.

**Backup and restore** take all seven together, with every broker stopped
([below](#backup-and-restore)).

## Backup and restore

A backup is all seven streams, taken while **every broker for the account is stopped**. A
running broker writes records on each stream that name state on the others, so streams
captured one after another while brokers run are not one moment, and nothing at start
detects the difference. With every broker stopped nothing writes those records: JetStream
still expires messages and, on a `$MQTT_rmsgs` with `Sources`, ingests (see below), which
the brokers meet at start exactly as after an outage
([details](ARCHITECTURE.md#the-seven-streams-are-one-recreation-unit)).

Two procedures; which one a deployment uses follows from where its NATS servers run:

| Topology | Procedure |
|---|---|
| External (the broker's Deployment beside NATS servers it does not run) | network backup, or cold copy |
| Embedded (the chart's StatefulSet) and the broker in the NATS chart's pods | cold copy: stopping the brokers stops their NATS servers |

### Network backup

With [natscli](https://github.com/nats-io/natscli) 0.5 or later (`nats backup`; older
releases name the same commands `nats stream backup` and `nats stream restore`), connected
as a user of the brokers' account.

Back up:

1. Stop every broker for the account, and whatever restarts them (scale the Deployment
   to 0).
2. For each of the seven streams:
   `nats backup stream '$MQTT_sess' <dir>/sess` and then
   `nats backup validate <dir>/sess`. Consumers are part of the backup, which is the
   command's default; do not pass `--no-consumers`. A member that fails either command
   makes the set unusable.
3. Start the brokers.

Restore, from ONE backup set, never members of two:

1. Validate all seven members, and check the destination: the same NATS account as the
   brokers' service identity, the same JetStream domain (`-js-domain` is part of every
   session's subject), the brokers' permissions
   ([details](ARCHITECTURE.md#recommended-nats-permissions)), and limits that admit the
   seven. A restore keeps each stream's configuration, its replica count included, which
   must equal the brokers' `-stream-replicas`; `--replicas` and `--config` are not part of
   this procedure.
2. Stop every broker, and whatever restarts them.
3. Delete whichever of the seven exist (`nats stream rm '$MQTT_sess' -f`, ...).
4. `nats backup restore stream <dir>/sess` for each of the seven. **Every one must report
   success.** On any failure, timeout or interruption, delete all seven again and restore
   all seven again, however many are present: a failed restore can leave its stream in
   place, configured and stamped like a complete one but missing records, and nothing at
   start detects that.
5. On a JetStream cluster, wait until every consumer of the seven reports a leader
   (`nats consumer report '$MQTT_sess'`, ...); a single server's consumers report none and
   need no wait. Then start the brokers. Until a clustered consumer has a leader, its
   information request goes unanswered and a client whose session needs it is refused at
   CONNECT.

### Cold copy

A copy of every JetStream server's store directory, taken while every broker and every
NATS server holding the account's streams is stopped. It keeps each consumer's state
exactly as stored.

1. Stop every broker, and whatever restarts them. Then stop every NATS server of the
   cluster (in the embedded topology both are one step: scale the StatefulSet to 0, see
   [Kubernetes](KUBERNETES.md#backups-and-upgrades)), and wait until every store is
   unmounted.
2. Copy every server's store (a volume snapshot or a file copy). Copies taken at
   different times are fine while every server stays stopped; a copy of a running
   server's store is not supported.
3. Start the servers, then the brokers.

Restore: with every broker and server stopped, put every store back from ONE copy set,
each to the server it came from (the same server names, cluster name and routes: a
server's identity and its replication state are in its store), kept away from any cluster
still running on the originals. Start the servers, wait until every stream reports a
leader and current replicas (`nats stream info`) and, on a cluster, every consumer of the
seven a leader (`nats consumer report`; a single server's consumers report none and need
no wait), then start the brokers. On a cluster each consumer elects its own leader after
the servers start, and a client whose session needs one without it is refused at
CONNECT.

### What a restore brings back

As of the backup: sessions and their subscriptions, QoS 1 and 2 messages queued for
offline sessions, deliveries awaiting PUBACK, PUBREC or PUBCOMP (resent with DUP, under
their identifiers, at most one AckWait after the brokers start), inbound QoS 2 messages
awaiting PUBREL, retained messages and pending delayed Wills (one that fell due meanwhile
is published once the brokers run). Everything after the backup is lost. The streams'
consumers come back with them; the brokers adopt the durable ones by name and leave the
others to expire.

Two differences between the procedures, both stated in
[Conformance](CONFORMANCE.md#backup-and-restore-what-the-procedures-leave):

- A **network backup from a clustered JetStream** loses each consumer's acknowledgements
  above its acknowledgement floor (nats-server writes such a consumer at its floor). After
  a restore, a QoS 1 message a client already acknowledged above that floor is delivered
  to it again; a completed QoS 2 message is not. A cold copy keeps the acknowledgements.
- A **network backup of a `$MQTT_rmsgs` with `Sources`** is not a coherent image of the
  retained messages: a sourced replacement arriving during the backup can leave a topic
  with neither its old nor its new message, and after a restore nats-server resumes the
  sourcing from the restored messages, so it may receive upstream messages again or skip
  some. A cold copy keeps the store's sourcing position.

## Upgrades and rollbacks

Two facts about a release decide how to move to it: the **persisted format** it reads and
writes (`mqtt_nats_broker_format` on every broker-owned stream,
[details](ARCHITECTURE.md#the-persisted-format)), and, for the embedded topology, the
**nats-server version** it embeds. A broker started on streams of another format refuses
at start, naming the format it found and the one it reads, and does not create or change
those streams ([Troubleshooting](TROUBLESHOOTING.md#the-broker-does-not-start)).

- **Same format, and (embedded) the same nats-server: a rolling restart.** One broker at
  a time, each with the drain delay and termination grace from [Shutdown](#shutdown);
  clients reconnect to another broker and take their sessions over. A rollback under the
  same condition is the same rolling restart back. With the chart, `helm upgrade` and
  `helm rollback` roll it the workload's way, which is not always one at a time
  ([Kubernetes](KUBERNETES.md#backups-and-upgrades)). Two releases of one format serving
  together is the contract
  ([details](ARCHITECTURE.md#the-broker-to-broker-protocol)); no test can show it until a
  second release exists.
- **Same format, another embedded nats-server: offline, the MQTT state kept.** A mixed
  nats-server cluster is unsupported, so every pod moves together: take a cold copy (the
  rollback point), scale to 0, change the release, scale up. A rollback is the same with
  the cold copy put back first: a store a newer nats-server wrote is not promised to an
  older one.
- **Another format: offline, and the MQTT state does not carry over.** No release
  converts one format into another. Take a backup (the rollback point), then reset:
  external, stop every broker, delete the seven, deploy the new release; embedded, see
  [Kubernetes](KUBERNETES.md#backups-and-upgrades). A rollback removes what the new
  release created, restores the backup and deploys the previous release; everything since
  the upgrade is lost. Rolled out by mistake as a rolling update, the new brokers refuse
  and never turn ready (embedded: each refusing pod takes its NATS server down with it).

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
