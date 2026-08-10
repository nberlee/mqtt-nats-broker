# Roadmap

This document is the authoritative checklist for the first public alpha. Technical
evidence, rejected approaches, test references, and residual risks live in the
[findings dossiers](findings/README.md); roadmap entries keep only the information
needed to order and gate work.

Until every item classified as an alpha blocker is complete, do not push release
artifacts, publish container images or Helm charts, or create an alpha tag.
Conditional blockers become release blockers when their corresponding feature is
included in the supported alpha scope. Investigations block only when they confirm
a defect or leave a release-gate failure unexplained.

The sections are ordered by dependency. Within a section, implementation may
proceed in parallel where the work does not overlap. Any new correctness,
security, interoperability, or data-loss finding must receive a stable finding ID,
a release classification, and a canonical dossier before feature work continues.

## 0. Resolve known findings

The 28 resolved findings are retained in the
[resolved-findings archive](findings/resolved.md). The entries below are the active
release view; their linked dossiers are canonical for mechanism, evidence,
rejected designs, tests, and residuals.

### 0.1 Remove pre-release persistence compatibility

Do these before further MQTT-core implementation. The bridge has no released
persisted-state contract, so carrying these paths through subsequent session,
retained-message, and Will changes would make every change maintain and test
formats that no user release depends on. Each checkbox is one commit.

- [x] [F-056: Remove inherited retained-message compatibility](findings/resolved.md#f-056).
  The legacy JSON reader, the old-server tombstone marker and the `RD`
  delete-notification protocol are gone, and so is the compaction delete beside
  the notification: the empty-body tombstone alone carries deletion, and the
  store evicts the record it replaces. An undecodable record is now rejected with
  a log and evicted rather than silently ignored. Filed while doing it: F-059 and
  F-060 below.
- [x] [F-058: Reject generationless delayed-Will records](findings/resolved.md#f-058).
  The empty-generation legacy bucket is gone. Every decoded `$MQTT_willq` record is
  validated — generation, origin sequence and epoch, state, and identity fields that
  must each be a single subject token — and bound to the subject it was stored on,
  at all three decode boundaries. A record that fails is refused: never claimed,
  published, cancelled or deleted, and reported once per subject. Two deltas from
  the finding's wording: the enqueue validates before persisting and falls to the
  existing immediate-publish fail-safe, and `willTakeoverFence`'s now-duplicate
  inline origin check is deleted rather than kept beside the validator. Residual,
  stated: a refused record is reclaimed by NOTHING — no sweep, fence or reaper
  touches it — so it persists until an operator deletes the message or recreates the
  stream, which is what the diagnostic prescribes. Permanence has a running cost, and
  the diagnostic says so, on two paths. The claim scan re-reads every extant record
  each interval, so a refused backlog adds `O(N)` JetStream gets per scan per broker
  and a large enough one can push a scan past its interval and delay valid due Wills.
  Worse, `mqttProcessConnect` waits for `cancelWillOutbox` before the CONNACK, so `N`
  refused records for one client add `O(N)` serial round trips to **every reconnect
  of that client, forever** — an availability cost for that client, not just a
  delayed Will. A quarantine stream, a bounded scan and a reaper were all rejected —
  each is a second mechanism deciding the fate of records the bridge has said it
  cannot interpret.

These cleanups do not remove MQTT wire compatibility or the operator migration
guide from NATS Server's embedded MQTT listener.

### 0.2 Decide the supported HA scope

The first decision is whether the alpha supports multiple MQTT frontends against a
replicated JetStream cluster. The README currently promises horizontally scalable,
cross-instance HA. If that promise remains, every conditional item in this
subsection is an alpha blocker. If the first alpha is explicitly single-frontend,
record that limitation in the README and conformance documentation and schedule
this epic after alpha.

- [ ] Decide and document the first-alpha HA support level.
- [ ] [F-027: Repair a recognised session-record fork](findings/ha-arbitration.md#f-027).
  **Conditional; very large.** Define the recoverable state and an arbitration
  mechanism before implementation.
- [ ] [F-029: Arbitrate simultaneous takeovers from one base](findings/ha-arbitration.md#f-029).
  **Conditional; very large.** This shares the F-027 arbitration design.
- [ ] [F-030: Add write lineage to the Will outbox](findings/ha-arbitration.md#f-030).
  **Conditional; large.**
- [ ] [F-031: Add write lineage to Will-cancellation watermarks](findings/ha-arbitration.md#f-031).
  **Conditional; large.**
- [ ] [F-032: Add write lineage to inbound QoS 2 state](findings/ha-arbitration.md#f-032).
  **Conditional; large.**
- [ ] [F-033: Add write lineage to shared QoS 2 assignments](findings/ha-arbitration.md#f-033).
  **Conditional; large.**
- [ ] [F-034: Add write lineage to outbound in-flight registers](findings/ha-arbitration.md#f-034).
  **Conditional; large.**
- [ ] [F-036: Revalidate quiet resident-session ownership](findings/ha-arbitration.md#f-036).
  **Conditional; very large.** Design this with F-027/F-029 rather than as a
  periodic-read patch.

### 0.3 Confirmed alpha blockers

These are concrete correctness, data-loss, race, or liveness defects in features
the README currently reports as supported. They remain blockers independently of
the HA-scope decision except where the dossier explicitly requires a remote
takeover.

- [ ] [F-051: Bound pre-CONNECT clients under auth passthrough](findings/dead-code.md#f-051).
  **Medium.** Derive admission timing from the configured client dialer and pin
  idle-socket closure without changing CONNECT authentication.
- [ ] [F-043: Fix the `removeSessFromFlappers` data race](findings/runtime-correctness.md#f-043).
  **Small to medium.** Reproduce both call sites and verify lock ordering first.
- [ ] [F-035: Bound every courtesy DISCONNECT write](findings/runtime-correctness.md#f-035).
  **Medium.** One watchdog must cover mutex acquisition and socket writing at
  every server-initiated DISCONNECT site.
- [ ] [F-041: Persist the MQTT 5 DISCONNECT expiry override](findings/session-and-will.md#f-041).
  **Medium.** The durable record and resume decision must agree even after an
  ambiguous write.
- [ ] [F-039: Restore shared membership during SUBSCRIBE rollback](findings/runtime-correctness.md#f-039).
  **Small.**
- [ ] [F-045: Release stranded shared pending state at membership removal](findings/runtime-correctness.md#f-045).
  **Medium.**
- [ ] [F-050: Refuse or retry an incomplete shared-subscription restore](findings/shared-qos2.md#f-050).
  **Medium.** CONNECT must not report a resumable session whose delivery path was
  not restored.
- [ ] [F-028: Preserve deferred disconnect state during foreign disposition](findings/session-and-will.md#f-028).
  **Medium.** Split the parked-disconnect loss and retained-continuation cleanup
  into separately testable changes.
- [ ] [F-037: Make the rejected finite-disconnect stamp crash-safe](findings/session-and-will.md#f-037).
  **Medium.**
- [ ] [F-038: Prevent takeover races from publishing or dropping the wrong Will](findings/session-and-will.md#f-038).
  **Large.** Treat this as durable Will-intent/outbox design, not a local close-path
  patch.
- [ ] [F-047: Keep affine assignments when a SUBSCRIBE packet fails](findings/shared-qos2.md#f-047).
  **Medium.**
- [ ] [F-048: Arbitrate affine cleanup before discarding assignments](findings/shared-qos2.md#f-048).
  **Medium to large.** Design and test F-047 and F-048 as one transactional-cleanup
  cluster.
- [ ] [F-059: Arbitrate retained-index application by stream sequence](findings/runtime-correctness.md#f-059).
  **Medium.** The volatile retained index applies stream events and local seeds
  without comparing sequences, so a delayed or lost application can leave a stale
  or absent entry. Two mechanisms, not one: a watermark orders the writers, and a
  replayable consumer or a reconciliation is what covers a lost `AckNone`
  delivery — the watermark alone cannot. Scope it by arm: a stale entry on an
  indexed subject is already reconciled by the next matching subscription after
  cache expiry, so the work that has no existing repair is the ABSENT entry.
- [ ] [F-060: Report a failed retained store to the publisher](findings/runtime-correctness.md#f-060).
  **Small to medium.** A PUBLISH whose RETAIN side failed to store is reported as
  successful, contrary to [MQTT-3.3.1-5]. Decide the contract for QoS 0 —
  which has no acknowledgement to withhold — alongside QoS 1 and 2.

### 0.4 Release-verification investigations

These entries preserve observations without pretending the implementation scope is
known. Capture complete logs, obtain a deterministic reproduction or identify the
failed test, and then either close the investigation or create a new confirmed
finding with acceptance criteria.

- [ ] [F-040: Reproduce reconnect versus DISCONNECT expiry-zero](findings/session-and-will.md#f-040).
  **Investigation.** A confirmed mechanism is a protocol blocker.
- [ ] [F-042: Identify the unexplained full-suite failure](findings/release-investigations.md#f-042).
  **Investigation.** Do not truncate suite output.
- [ ] [F-044: Determine whether the phase-2 shutdown observation is real](findings/release-investigations.md#f-044).
  **Investigation.**
- [ ] Cover retained convergence for a sourced `$MQTT_rmsgs` topology.
  **Investigation.** `docs/ARCHITECTURE.md` declares a cross-account sourced
  retained stream a supported topology, but the only test of it lives in the
  unbuilt `mqttport_clustertests` reference file, so nothing running exercises a
  record arriving through a source with a subject transform. Establish whether a
  native test can stand it up; if it can, the test is release verification.

### 0.5 Post-alpha candidates and scope cleanup

These do not block the alpha unless investigation shows that ordinary broker
operation can trigger correctness loss or unbounded resource growth.

- [ ] [F-046: Split the in-flight stabilization follow-ups](findings/inflight-followups.md#f-046).
  Decide the compatibility-sensitive schema naming before alpha; schedule the
  stale-metadata cleanup and settlement-fence work independently.
- [ ] [F-049: Provide operator recovery for unreadable affine claims](findings/shared-qos2.md#f-049).
  Keep basic diagnostics in alpha scope; automatic quarantine or repair may follow
  alpha if broker-owned state cannot create the malformed record.

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

### Durable enforcement of the session/marker coupling

- [ ] Decide whether the `$MQTT_sess` + `$MQTT_willcancel` "one logical unit"
  constraint should be durably enforced at startup rather than documented.
  Raised by the stream-prerequisite audit's review (round 7): deleting only
  `$MQTT_sess` mints a new incarnation epoch while permanent watermarks keep
  the old one; the fence then classifies them stale and falls back to the
  session-record rule, whose comment already names "unsupported partial
  recreation" as the carrier of this documented residual
  (`will_outbox.go`, `sessionRecordFence`). The reviewer proposed echoing the
  authoritative epoch into `$MQTT_willcancel` metadata and refusing a
  mismatch before publishing the manager. Deciding needs its own design pass:
  the echo is new arbitration-bearing durable state (who writes it, create
  races between initializing brokers, bootstrap when the marker stream is
  created first, self-heal on the SUPPORTED coupled recreation), and refusal
  on mismatch inverts `TestSessionManagerEpochRecreationYieldsDifferentEpoch`,
  which has pinned recreate-then-start since the watermark item landed. Not a
  release blocker: the exposure requires an administrative action the
  operational constraints in `docs/ARCHITECTURE.md` already forbid.

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

### Dead-code and reachability cleanup

Each checkbox below is intentionally scoped as one reviewable commit. Complete
them in order where a later lint change depends on an earlier cleanup.

- [ ] [F-052: Connect MQTT option validation to production startup](findings/dead-code.md#f-052).
  Adapt or retire the upstream validator, and make its tests exercise the real
  standalone startup boundary.
- [ ] [F-053: Remove independent dead declarations](findings/dead-code.md#f-053).
  Delete the unreferenced constants, errors, leaf wrappers, singular shared-holder
  helper, and superseded no-sequence message lookup.
- [ ] [F-053: Remove obsolete upstream integration clusters](findings/dead-code.md#f-053).
  Delete the unused configuration-reload, embedded-routing, and retained-permission
  rescan paths with the imports and shim state they strand.
- [ ] [F-054: Move test-only helper APIs out of production files](findings/dead-code.md#f-054).
  Keep live hook points, but place tagged-test setters and convenience methods in
  the test build where possible.
- [ ] [F-054: Reconcile the constant-state v5 capability](findings/dead-code.md#f-054).
  Derive it from supported configuration or collapse its impossible false branch.
- [ ] [F-054: Isolate the parked WebSocket path](findings/dead-code.md#f-054).
  Keep the future transport milestone explicit without carrying an apparently
  selectable non-nil WebSocket path in current production flow.
- [ ] [F-054: Isolate upstream-only QoS compatibility switches](findings/dead-code.md#f-054).
  Keep reference-test behavior without presenting the switches as reachable
  production configuration.
- [ ] [F-055: Re-enable reachability lint for `mqtt.go`](findings/dead-code.md#f-055).
  Replace the file-wide exclusions with default- and full-suite analysis plus
  narrowly documented exceptions.

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

- [ ] Every unconditional alpha blocker is complete.
- [ ] The supported HA scope is documented, and every conditional blocker for
  that scope is complete.
- [ ] Every release-verification investigation is closed or has produced a
  confirmed finding with an explicit release disposition.
- [ ] Every post-alpha candidate has an explicit disposition; an unchecked
  post-alpha item does not by itself block the alpha.
- [ ] The full test, race, lint, conformance, Paho, container, and deployment
  gates pass with no unexplained failures or skips.
- [ ] CI is green from a clean checkout on every supported architecture.
- [ ] MQTT 3.1.1 and 5.0 conformance baselines have no unexplained regression.
- [ ] The scratch image and Helm chart install successfully in a clean cluster.
- [ ] TLS rollover, WebSocket, authentication, authorization, migration, backup,
  restore, upgrade, rollback, and shutdown procedures are documented and tested.
- [ ] Release artifacts use the final product name consistently.
