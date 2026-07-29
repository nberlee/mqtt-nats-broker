# Roadmap

This document is the authoritative checklist for the first public alpha.
Until every release blocker is complete, do not push release artifacts, publish
container images or Helm charts, or create an alpha tag.

The sections are ordered by dependency. Within a section, implementation may
proceed in parallel where the work does not overlap. Any new correctness,
security, interoperability, or data-loss finding becomes a release blocker and
is fixed before feature work continues.

## 0. Resolve known findings

- [x] Remove the accidental review sentence from the README attribution section.
- [x] Make MQTT TLS startup fail closed. An invalid or unreadable certificate,
  key, or CA must never result in a plaintext listener.
- [x] Reconcile the README TLS/mTLS feature claim with the missing
  certificate-rollover behavior. The README now documents working TLS and mTLS
  and states explicitly that live rollover is not implemented; rollover itself
  remains tracked under "TLS and certificate rollover" below.
- [x] Resolve the MQTT core TODO concerning JetStream domain assignment.
  `-js-domain` is now the single validated source of truth (the unreachable
  per-account and server-level branches are gone), and the broker no longer
  fabricates a `ClientInfo.Cluster` from its node name.
- [x] Resolve and document the policy for subscriptions to internal MQTT
  subjects. SUBSCRIBE now denies the whole reserved `$MQTT.>` namespace (not
  only the three delivery prefixes), legacy entries are purged from resumed
  session records, reserved Will topics are rejected at CONNECT, and the
  broker's own delivery subscriptions moved to the service connection so a
  full-namespace deny is deployable. Documented under "Reserved `$MQTT.>`
  namespace" in `docs/ARCHITECTURE.md`.
- [x] Replace brittle JetStream error-string matching in the QoS 2 path with a
  robust error classification. `isJSErrCausedBy` matches the JetStream API error
  code structurally and confines the unavoidable cause-text comparison to the
  generic `{err}` errors (`JSStreamStoreFailedF`, `JSStreamMsgDeleteFailedF`),
  which carry their real reason only in `ApiError.Description`; the QoS 2 dedup
  store and the session-record delete now go through it, and `mqttProcessPubRel`
  classifies its hold lookup and hold delete instead of discarding both errors.
  API error identity no longer depends on the rendered `ApiError.Error()` form,
  but a change to the flattened cause wording itself would still defeat
  recognition.
- [x] Close the destructive PUBREL move window. The QoS 2 hold record is now a
  two-state machine (`internal/mqtt/qos2_move.go`): a create-only CAS writes
  `held`, the message is delivered onward FIRST, and only then does a CAS at the
  held record's exact sequence replace it with a terminal `delivered` tombstone. A
  PUBREL that finds the tombstone answers `0x00` without re-delivering, for as
  long as the tombstone exists — indefinitely, and independently of any
  deduplication window. Every record carries an immutable `Nmqtt-Q2Gen` generation
  ID; the finalizer reconciles by generation and abandons without mutating when it
  finds a foreign one, so it can never overwrite a newer session generation's
  record on a reused `(client, packet identifier)` subject. A stable
  `Nats-Msg-Id` on `$MQTT_msgs` (whose deduplication window is a nats-server
  default) narrows the remaining window in which a retry duplicates the *stored*
  copy, but correctness does not rest on it. `$MQTT_qos2in` moved from
  `DiscardNew`/`DiscardNewPer` to `DiscardOld` so the CAS is a replacement rather
  than a rejection, and a non-conforming stream now fails startup with recreate
  guidance — **this is not a rolling broker upgrade**. Residual: the live
  core-NATS fan-out is at-least-once across the store-to-commit interval, and the
  duplicate *stored* copy is suppressed only while deduplication survives (not
  across a nats-server process restart). See "Inbound QoS 2 PUBREL is
  at-least-once across the unresolved move interval" in `docs/CONFORMANCE.md`.
- [x] Keep a started QoS 2 delivery with its chosen shared-subscription client.
  Resolved by session-affine ownership (`internal/mqtt/qos2_shared_affine.go`). A
  create-only CAS on the new `$MQTT_qos2assign` stream elects one winner per
  `(group durable, source sequence)` and every contender honors that claim
  **indefinitely**; the message is copied onto the winner's own
  generation-scoped `$MQTT_out` subject and delivered by a durable only that
  session generation can read, so the exchange completes off the queue-delivered
  group durable entirely. The group delivery is acked only once the claim proves
  the copy exists, and that ack is proven by a JetStream double ack before any
  claim is reaped — a queued ack would let a lost ack reopen the assignment.
  Assignment deliberately does **not** rest on the `Nats-Msg-Id` deduplication
  window (finite; using it recreates the violation). Completion facts are
  orthogonal and monotonic, so a discard can never stand in for an unproven
  source ack. Residuals, both stated in `docs/CONFORMANCE.md`: packet-identifier
  continuity across a broker restart or failover (the message stays
  session-affine and is neither lost nor redistributed, but is redelivered with
  DUP=1 and a fresh identifier), and mixed-version operation — the storage schema
  is rolling-compatible, the delivery protocol is not, so affinity holds only once
  every broker that can serve the group is q2aff-aware.
- [ ] Decide and test the expected QoS 2 behavior for an unacknowledged PUBLISH
  after UNSUBSCRIBE. Decided: comply with [MQTT-3.10.4-3] for plain consumers by
  draining — the durable and its delivery subscription survive the UNSUBSCRIBE
  while deliveries already started remain pending, and fresh deliveries are
  discarded. That needs a durable state machine (staged and committed drain
  states, a persist-before-ack transition on every pending removal, QoS 2 commit
  only after the PUBREL store succeeds, persisted delivery metadata, and recovery
  that reconciles against consumer pending state), so it is planned and reviewed
  separately. The QoS 1/2 to QoS 0 re-SUBSCRIBE downgrade
  (`execDeferredConsumerDelete`) is out of scope: it replaces a subscription
  rather than deleting one. The plain-consumer drain alone must not tick this
  item as universally compliant — it closes only once the shared QoS 2 item above
  is also resolved.
- [ ] Serialize session discard against inbound QoS 2 PUBREL release. A PUBREL
  racing a cross-broker session discard can deliver a message from a discarded
  session once (residual 4 of the move protocol). The intended fix is a
  `held -> releasing` CAS claim on the hold record that competes with the
  discard/cancel transition, so exactly one of "release it" and "discard it" wins.
  Narrow and not a regression — the pre-move code had the same race with a worse
  outcome — so it is a follow-up rather than a blocker.
- [ ] Reap orphaned inbound QoS 2 hold records. Nothing reaps a `held` record whose
  session record was lost, so it wedges its packet identifier until a later PUBLISH
  on that identifier reaps it. Pre-existing: neither created nor worsened by the
  move protocol, which added the terminal tombstone but no new leak.
  Extend the same sweep to the two structures session-affine shared QoS 2 adds:
  an orphaned `$MQTT_Q2AFF_*` consumer whose session record is gone (an older
  broker in a mixed-version window ignores `Q2Aff` entirely), and an assignment
  claim whose winner generation no longer exists and which no contender happens
  to revisit. Neither loses a message — a stalled claim is honored, not
  released — but both accumulate.
- [ ] Give outbound QoS 1/2 in-flight deliveries a durable identity, so a packet
  identifier survives a broker restart or failover rather than only a same-process
  reconnect. Today `pendingPublish`/`cpending` are memory-only, so after a restart
  a started delivery is re-offered with DUP set and a **freshly allocated**
  identifier, and a client is entitled to treat that as a new message. This is the
  single remaining cause of the HA exactly-once deviation, and it is not specific
  to shared subscriptions: it applies to every QoS 1/2 outbound path. A fix has to
  persist the identifier together with the source durable and sequence, the
  PUBLISH-versus-PUBREL phase, and a recovery ordering — which is why it is its
  own item rather than a detail of the shared-subscription work that surfaced it.
- [ ] Audit the remaining `$MQTT_*` streams for protocol prerequisites, deriving
  each list from what that stream's owner actually does rather than copying
  `validateQoS2InStream` wholesale — the required set genuinely differs per stream.
  `$MQTT_willcancel` does CAS writes and permanent retention but never deletes or
  purges by design, so `DenyDelete`/`DenyPurge` are *not* prerequisites there, while
  `NoAck`, `Sealed`, `SubjectTransform`, `MaxMsgSize` and the other write/CAS-affecting
  settings are; `validateWillCancelStream` currently checks state shape only.
  `$MQTT_willq` does CAS writes plus terminal deletion, so deletion prerequisites do
  apply. `$MQTT_sess` does CAS writes, deletion and cleanup. `$MQTT_willq` and
  `$MQTT_sess` have no validator at all today.

## 1. Settle release decisions

These decisions affect binary behavior, package names, images, charts,
documentation, and release artifacts. Make them before creating those surfaces.

### Product name

- [ ] Decide the product name before renaming packages, images, charts,
  workflows, or release artifacts.

Current recommendation: `mqtt-nats-broker` describes the product more
accurately than `mqtt-nats-bridge`. The process terminates MQTT and owns broker
semantics; it does not merely relay between two independent brokers.
`nats-mqtt-broker` is also accurate but may imply that this is an official NATS
project. Retain `mqtt-nats-bridge` only if continuity outweighs that ambiguity.

### Memory-limit strategy

- [ ] Decide whether to use `github.com/KimMachineGun/automemlimit`.

Current recommendation: use the pinned library with a documented 0.9 ratio and
allow an explicit `GOMEMLIMIT` override. Kubernetes `resourceFieldRef` cannot
multiply the memory limit by 0.9, while implementing Kubernetes quantity
arithmetic in Helm is fragile. Acceptance requires cgroup v1/v2 tests and clear
behavior when no container limit is configured.

## 2. Make the MQTT core maintainable

- [ ] Split `internal/mqtt/mqtt.go` along stable responsibilities: protocol
  parsing, connection lifecycle, sessions, subscriptions, retained messages,
  QoS flows, MQTT 5 properties, JetStream persistence, and transports.
- [ ] Keep behavior unchanged during the split and run the upstream and native
  suites after each extraction.
- [ ] Preserve provenance for code derived from NATS Server through NOTICE and
  concise file or package documentation.
- [ ] Do not preserve file layout merely to keep a line-by-line upstream diff;
  maintainability of this broker is the controlling requirement.

### Performance and resource audits

- [ ] Audit every JetStream query and request pattern inherited from the early
  NATS Server MQTT implementation against current JetStream APIs and behavior.
  Look for avoidable request/reply round trips, repeated stream or consumer
  lookups, serial operations that can be batched or run concurrently, full
  scans where direct lookups are available, and polling that can be replaced by
  push or event-driven behavior.
- [ ] Benchmark the current and proposed JetStream paths under connect,
  subscribe, publish, reconnect, retained-message, and session-recovery loads.
  Record request counts, transferred bytes, latency, and NATS/JetStream load;
  change a query path only when measurements show an improvement without
  weakening MQTT semantics, authorization, consistency, or HA behavior.
- [ ] Perform a full memory and resource-lifetime scan of the broker. Cover
  clients, sessions, subscriptions, retained-message caches, QoS state,
  JetStream request state, timers, goroutines, channels, buffers, NATS
  connections/subscriptions, TLS reload state, and shutdown paths.
- [ ] Add churn and soak tests that repeatedly connect, subscribe, publish,
  disconnect, expire sessions, fail over, and reload TLS. Use heap and goroutine
  profiles plus leak detection to prove memory and resource counts return to a
  stable baseline after work drains; fix every confirmed leak before alpha.

## 3. Add MQTT transports

### TLS and certificate rollover

- [x] Support MQTT TLS and mutual TLS as tested production listener modes.
- [x] Expose TLS on a second listener, or document why a single configurable
  listener (`-port`, plaintext or TLS) is sufficient. Decided and documented in
  the README: one listener, `-port` selects it in either mode, no implicit port
  change when TLS flags are set, and TLS deployments use `-port 8883`. Serving
  1883 and 8883 at once would need multiple listeners and is not supported.
- [ ] Reload certificate, private-key, and client-CA changes from projected
  Kubernetes Secrets without restarting the process or pod.
- [ ] Do not require Stakater Reloader or another rollout controller.
- [ ] Replace the active TLS configuration atomically only after the complete
  replacement configuration validates.
- [ ] Keep the last valid configuration active when a projected update is
  incomplete, malformed, or contains a mismatched key.
- [ ] Prove that new handshakes use the replacement certificate while existing
  MQTT connections remain connected.
- [ ] Cover expiry, malformed files, mismatched keys, CA rollover, concurrent
  handshakes, and failed reload recovery in native tests.

### WebSocket

- [ ] Implement MQTT over WebSocket and secure WebSocket.
- [ ] Define listener, path, origin, proxy-header, timeout, and TLS settings.
- [ ] Add native integration tests for MQTT 3.1.1 and 5.0 over WebSocket.
- [ ] Replace or retire the parked upstream WebSocket reference tests once
  equivalent behavior is covered.

## 4. Build the runtime image

- [ ] Build the runtime image `FROM scratch`.
- [ ] Copy the system CA bundle into the runtime image.
- [ ] Run as a numeric non-root user with a read-only root filesystem.
- [ ] Verify outbound TLS connections to NATS, mounted MQTT TLS Secrets,
  health probes, graceful shutdown, and signal handling.
- [ ] Build and smoke-test linux/amd64 and linux/arm64 images in CI.

## 5. Package for Kubernetes

### Kubernetes manifest

- [ ] Replace JSON-like flow mappings and sequences in the example manifest
  with readable block-style YAML.
- [ ] Set the effective Go memory limit to 90 percent of the container memory
  limit.
- [ ] Include secure defaults, probes, resource requests and limits, graceful
  termination, and optional TLS Secret mounts.

### Helm chart

- [ ] Add a production Helm chart for the broker.
- [ ] Include Deployment, Service, TLS mounts, probes, resources, security
  context, autoscaling options, PodDisruptionBudget, and graceful termination.
- [ ] Support an external NATS/JetStream cluster.
- [ ] Add the official NATS chart as an optional dependency.
- [ ] Expose the dependency configuration under `nats:` so users can place
  normal NATS chart values there without translating every field.
- [ ] Add Helm lint, template, install, upgrade, rollback, and optional-NATS
  tests to CI.
- [ ] Package and publish the chart as an OCI artifact in GitHub Container
  Registry only as part of an alpha release.

## 6. Document migration and security

- [ ] Write a migration guide from NATS Server's embedded MQTT listener to this
  standalone broker.
- [ ] Cover listener cutover, DNS or load-balancer changes, expected downtime,
  rollback, and verification.
- [ ] State explicitly whether sessions, retained messages, QoS state, and
  delayed Wills can be migrated. Do not imply persisted-state compatibility
  where none exists.
- [ ] Explain service credentials, anonymous-client defaults, and per-client
  authentication passthrough separately.
- [ ] Provide NATS user/password and JWT/account examples for MQTT
  authentication and publish/subscribe authorization.
- [x] Document MQTT-topic to NATS-subject mapping and denial of the internal
  `$MQTT.>` namespace. See "Reserved `$MQTT.>` namespace" in
  `docs/ARCHITECTURE.md`; the migration guide should link it rather than restate
  it.
- [ ] Include Kubernetes Secret and live TLS rollover examples.

## 7. Finish provenance and release documentation

### Remove porting-era terminology

- [ ] Rename the `mqttport_fulltests`, `mqttport_clustertests`,
  `mqttport_wstests`, and `mqttport_tlstests` build tags and CI references.
- [ ] Preferred replacements are `mqtt_upstream_suite`,
  `mqtt_cluster_reference`, `mqtt_websocket_reference`, and
  `mqtt_tls_reference`.
- [ ] Remove `port`, `ported`, and `porting` as project-identity language from
  the README, CI, lint configuration, comments, and documentation.
- [ ] Keep technically necessary networking terms such as listener port,
  `containerPort`, and port numbers.
- [ ] Replace `docs/PORTING.md` before alpha with concise provenance
  documentation covering the MQTT 3.1.1 source, upstream revision, license,
  modifications, and intentional divergence.
- [ ] Keep attribution in NOTICE and the README after removing the porting
  narrative.

### Release documentation

- [ ] Remove the pre-release changelog before the first alpha.
- [ ] Start a changelog with the first published alpha, or generate release
  notes from Git history and pull requests.
- [x] Replace the README's duplicate roadmap with a short link to this file.
- [ ] Make version-controlled files under `docs/` the source of truth for a
  GitHub Wiki.
- [ ] Publish the wiki from CI so it cannot drift from repository docs.
- [ ] Include installation, configuration, architecture, TLS, Helm/Kubernetes,
  authentication and authorization, migration, operations, troubleshooting,
  conformance, and provenance pages.

## Alpha release gate

The first alpha may be tagged only when:

- [ ] All findings and roadmap items above are complete.
- [ ] The full test, race, lint, conformance, Paho, container, and deployment
  gates pass with no unexplained failures or skips.
- [ ] CI is green from a clean checkout on every supported architecture.
- [ ] MQTT 3.1.1 and 5.0 conformance baselines have no unexplained regression.
- [ ] The scratch image and Helm chart install successfully in a clean cluster.
- [ ] TLS rollover, WebSocket, authentication, authorization, migration, backup,
  restore, upgrade, rollback, and shutdown procedures are documented and tested.
- [ ] Release artifacts use the final product name consistently.
