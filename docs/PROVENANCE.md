# Provenance

Where this broker's code comes from, under which license, and where it deliberately
differs from its source. Attribution text is in [NOTICE](../NOTICE).

## Source

- **The MQTT protocol core** (`internal/mqtt/mqtt.go` and the files split from it,
  listed in `internal/mqtt/doc.go`) derives from nats-server's `server/mqtt.go` as
  extended for MQTT 5.0 on the `mqtt5-shared-subs` branch of
  [nberlee/nats-server](https://github.com/nberlee/nats-server). That work was proposed
  upstream in
  [nats-io/nats-server discussion #8362](https://github.com/nats-io/nats-server/discussions/8362)
  and declined there on 2026-07-07: nats-server keeps its MQTT 3.1.1 listener and does not
  take MQTT 5.0. Released nats-server versions contain no MQTT 5.0 code.
- **The WebSocket frame layer** (`transport_websocket.go`) adapts nats-server v2.15.1
  `server/websocket.go`; its header says what was not carried over.
- **Vendored helpers** (`compat_helpers.go`, `compat_proto.go`) are copied verbatim or
  lightly adapted from nats-server.
- **The upstream test suite** (`mqtt_test.go`, `mqtt_v5_test.go`, and the parked
  `*_ref_test.go` and `mqtt_ex_*` files) comes from nats-server's tests and keeps their
  headers; `CONTRIBUTING.md`, "Test suites and build tags", says how each runs.
- **The Helm chart's** `_jsonpatch.tpl` and `_tplYaml.tpl` are copied from the NATS Helm
  charts ([nats-io/k8s](https://github.com/nats-io/k8s), `helm/charts/nats`).

## Upstream revision

The import commit was not recorded. The baseline is **inferred**: the repository's first
commit (`9c282fb`, 2026-07-24) is closest to the `mqtt5-shared-subs` head
`00b5ca83341691967c44fe46b39591029ba6bf35` (2026-07-06, "mqtt: implement MQTT 5.0 Shared
Subscriptions"). Its `internal/mqtt/mqtt.go` shares 241 of its 249 top-level functions
with that head's `server/mqtt.go`, and a whitespace-insensitive line diff against it is
598 lines, against 2037 for the head's parent. The first commit's `go.mod` pinned
nats-server v2.14.3; that is the dependency it built against, not the source of the core.

Upstream changes taken since are recorded per change, by upstream pull request:
nats-io/nats-server#8692 (F-120; the `mqtt_test.go` cases marked "Ported from upstream
nats-server (#8692)"). `CONTRIBUTING.md`, "Syncing with upstream NATS", is the
procedure.

## License and attribution

The derived files are Apache-2.0, as is this project ([LICENSE](../LICENSE)). Each keeps
the NATS Authors' copyright header, and [NOTICE](../NOTICE) carries the attribution.

## Modifications

- **The runtime.** Upstream, an MQTT connection is a nats-server `*client`, and
  persistence and routing ride server internals. Here those types are re-implemented in
  `shim_*.go` against the public `nats.go` client, so the broker runs as a stateless
  frontend against an external NATS/JetStream cluster: core subscriptions and publishes
  on a per-client connection, and `mqttJSA` speaking the JetStream API over
  request/reply on a service connection. See "The shim surface" and "Backend seams" in
  [ARCHITECTURE.md](ARCHITECTURE.md).
- **The layout.** The core is split along this broker's responsibilities, not
  upstream's ("Source layout of the protocol core" in ARCHITECTURE.md).
- **The protocols.** Session takeover, QoS 2, Wills and in-flight state are carried
  across brokers by durable protocols that have no upstream counterpart; ARCHITECTURE.md
  documents them.

## Intentional divergence

- **MQTT 5.0 is always on.** The CONNECT gate accepts protocol levels 4 and 5 on every
  broker; there is no opt-in gate and no `max_protocol_version` cap (F-054).
- **Client identifiers** may be any valid UTF-8, which Eclipse Paho interop requires; the
  identifier is hashed, never used as a raw subject token. See "Client-ID handling" in
  [CONFORMANCE.md](CONFORMANCE.md).
- **One NATS account.** Session, retained and QoS streams are account-scoped, so the
  broker serves the account of its service connection. Clients authenticate through
  auth passthrough, and an identity from another account is refused
  ([AUTHENTICATION.md](AUTHENTICATION.md)). The upstream `Options.Users`,
  `Authorization` and `NoAuthUser` surfaces are inert.
- **WebSocket** is a second listener feeding the same core, not upstream's `net/http`
  server; compression, the no-masking negotiation and text frames are not supported.
- **Writes are synchronous.** Upstream's write loop and outbound buffer are replaced by
  a synchronous write under `wmu`, which keeps upstream's per-write deadline
  (`mqttWriteDeadline`, F-035).
- **Shared-subscription selection happens on the service connection**, so a member
  denied by authorization cannot be filtered out before a message is offered to it; the
  broker hands such an offer back to the group instead (CONFORMANCE.md).

The inherited test suite was adapted where those divergences make an upstream case
inapplicable, and nothing is skipped:

- **Config-based authentication (7 removed).** They exercised `Options.Users` and
  related options, which are inert here. Publish and subscribe authorization are covered
  by the broker's own authorization tests.
- **Cross-topic and cross-QoS order (3 rewritten).** MQTT orders only per (Topic, QoS);
  see "Per-(Topic, QoS) delivery order" in CONFORMANCE.md.
- **Client ID with an invalid character (1 removed).** It asserted the upstream
  restriction this broker lifts; `TestClientIDArbitraryChars` covers the accept path.
  `TestMQTTv5DisabledByDefault` is omitted, since MQTT 5.0 is always on.
- **An inherited flaky skip (1 re-enabled).**
  `TestMQTTQoS2InflightMsgsDeliveredAfterUnsubscribe` runs with a wider AckWait than
  upstream's 10 ms, which raced its own redelivery timer; the test states why its value
  is what it is.
