# Changelog

All notable changes to this project are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Before 1.0.0, alpha releases promise no
compatibility between versions and decide each break case by case; beta releases keep
the contracts and are compatible within them (`docs/ARCHITECTURE.md`, "Compatibility
boundary").

Each entry names the two facts that decide how to move to a release: the persisted format
it reads and writes, and the nats-server version the embedded image runs
(`docs/OPERATIONS.md`, "Upgrades and rollbacks").

## [0.1.0-alpha.1] - 2026-11-22

The first published release.

- **Persisted format:** `1` (`mqtt_nats_broker_format` on every broker-owned stream).
- **nats-server:** requires 2.15.2 on every JetStream peer, and not 2.15.3; the
  embedded image runs 2.15.2.

### Added

- An MQTT 3.1.1 and 5.0 broker whose sessions, subscriptions, retained messages,
  delayed Wills and QoS 1/2 exchanges live in seven replicated JetStream streams;
  broker instances are stateless and scale horizontally, and a client reconnecting to
  another instance takes its session over.
- QoS 0, 1 and 2; retained messages; persistent sessions; Will and Will Delay; MQTT 5
  message and session expiry, reason codes and properties; shared subscriptions at
  QoS 0 and 1.
- TLS and mTLS with certificate rollover without a restart, plaintext beside TLS, and
  MQTT over WebSocket and secure WebSocket.
- Authentication passed through to NATS: each client is held to its own NATS identity's
  publish and subscribe permissions, read at CONNECT.
- Two container images, multi-arch and `FROM scratch`: the broker in front of a NATS
  you run, and the broker with JetStream embedded (`-embedded`).
- A Helm chart for the embedded and the external topology, example Kubernetes
  manifests, and values that run the broker in the official NATS chart's pods.
- `/healthz` and `/readyz`, a drain on SIGTERM, structured logs, and documented backup,
  restore, upgrade and rollback procedures.

### Known issues

- A reconnecting client's CONNECT can exceed 300 ms on a slow store, because R1
  atomic batches are synced under the stream's locks in nats-server
  ([#183](https://github.com/nberlee/mqtt-nats-broker/issues/183)).
- A clustered broker can exit at startup when nats-server reports a stream it just
  created as not found; a supervisor restart recovers it
  ([#184](https://github.com/nberlee/mqtt-nats-broker/issues/184)).
- nats-server 2.15.3 can report a recovering consumer as not found, and the broker
  then replaces that client's durable; stay on 2.15.2
  (`docs/known-upstream-issues.md`, CONSUMERINFO-NONODE-NOTFOUND).
- A shared subscription requesting QoS 2 is granted QoS 1.

Every behavior gap is stated in `docs/CONFORMANCE.md`.

[0.1.0-alpha.1]: https://github.com/nberlee/mqtt-nats-broker/releases/tag/v0.1.0-alpha.1
