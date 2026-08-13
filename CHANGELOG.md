# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Working MQTT TLS and mutual TLS listener, failing closed.** The server-side TLS
  handshake was a no-op stub: a listener configured with `-tls-cert`/`-tls-key`
  logged `tls://` but never upgraded the connection, and then panicked on the
  `*tls.Conn` assertion made after client setup. The handshake is now real
  (`tls.Server` under a bounded deadline, default 2s via `MQTTOpts.TLSTimeout`,
  socket replaced only after the handshake succeeds), so `-tls-ca` produces genuine
  verified mTLS. Startup now fails closed as well: the listener TLS configuration is
  resolved immediately after config load — before the NATS connection and the health
  listener — so an invalid or unreadable certificate, key, or client CA aborts the
  process instead of silently serving plaintext, and is reported even when NATS is
  unreachable. `-tls-ca` without a certificate and key is rejected in validation
  rather than accepted and ignored. The broker still serves a single listener:
  `-port` selects it in either mode (use `-port 8883` for TLS).
- **Connection setup no longer runs on the accept loop.** With a real handshake in
  place, accepting a connection inline meant one client that connected and sent no
  ClientHello stalled every subsequent accept for the whole handshake timeout, so a
  few idle sockets could keep the listener unavailable. `MaxConn` is no defense: it
  bounds how many clients are registered, not how long one of them can hold up the
  accept loop, and it is unlimited by default. Setup is now dispatched per
  connection, as upstream nats-server does, with a shutdown-safe admission gate that
  refuses new setups once shutdown starts and ensures none outlive the lifecycle.
  Shutdown drains clients before waiting on in-flight setups: a client parked in its
  handshake is already registered, so closing its socket is what lets that setup
  finish. Teardown paths close the raw accepted socket rather than the
  protocol-facing connection, so the drain cannot race a handshake swapping in the
  TLS wrapper.
- **Durable delayed-Will cancellation watermark.** Closes the reconnect-suppression
  residual in the Layer-2 delayed-Will outbox: an obsolete Will could still fire
  after a newer CONNECT that later finite-disconnected or deleted its session record.
  A permanent per-client marker on a dedicated `$MQTT_willcancel` stream
  (`MaxMsgsPer:1`, no age/size limits) is advanced at every accepted CONNECT as a
  gating write — no success CONNACK until it lands, with a rollback transaction that
  rejects the CONNECT and reconciles local state if it cannot. Sequences are compared
  only within one `$MQTT_sess` incarnation, identified by a generated epoch
  (`nuid:<NUID>`) stored in the session-stream metadata (`mqtt_sess_epoch`) and
  carried on both the marker and each Will (`OriginSessEpoch`). When no marker is
  present the fence falls back to the prior session-record rule; a marker-stream
  outage defers pending Wills (never lost) until it recovers. **First-release storage
  format:** the pre-release in-record Will migration path (`Migrated` field, migration
  worker, terminal tombstones) is removed, and a `$MQTT_sess` stream without a valid
  generated epoch is rejected with recreate guidance. Back up/restore/recreate
  `$MQTT_sess`, `$MQTT_willcancel`, `$MQTT_teardown` and `$MQTT_origin` as ONE unit,
  with every broker for the account stopped. The coupling is enforced differently on
  each, which is why the instruction covers all four: `$MQTT_teardown` and
  `$MQTT_origin` carry the session stream's incarnation epoch in their STREAM metadata
  and their validators refuse a session-only recreation at the next startup, while
  `$MQTT_willcancel` carries it per watermark RECORD and is not startup-enforced — a
  session-only recreation there is admitted, silently losing the suppression evidence
  that keeps superseded delayed Wills from firing.
- Project scaffold: Go module, config (`internal/config`), NATS/JetStream backend
  with auth-passthrough dial (`internal/backend`), runnable entrypoint with
  health/readiness endpoints.
- MQTT protocol core ported from NATS-server behind the `mqttport_wip` build tag:
  alias layer for 76 exported NATS wire types, vendored self-contained helpers,
  and behavioral shims for `client`/`Server`/`subscription`/`Account`/`ipQueue`
  plus `Options`/`MQTTOpts`. The tagged core compiles
  (`go build -tags mqttport_wip ./internal/mqtt/`).
- JSA/routing transport wired onto nats.go (`processSub`/`processUnsub`/
  `processInboundClientMsg` + header framing). The account session manager comes
  up against a live JetStream cluster and creates the MQTT streams
  (`$MQTT_sess`/`$MQTT_msgs`/`$MQTT_rmsgs`) through the ported code.
- Connection I/O engine (`readLoop`/`queueOutbound`/`acceptConnections`/
  `initClient`) and a public broker entrypoint (`mqtt.NewServer`/`Start`/
  `Shutdown`). A real paho MQTT client completes CONNECT → SUBSCRIBE → PUBLISH →
  receive over TCP + JetStream (`shim_e2e_test.go`), and the `cmd` binary serves
  MQTT when built with `-tags mqttport_wip`.
- MQTT 5.0 is always enabled: removed the `v5Enabled` opt-in gate and the
  "experimental" warning; the CONNECT version gate defaults to accepting 5.0.
- Per-client auth passthrough: the CONNECT auth gate dials a per-client
  `*nats.Conn` from the MQTT username/password so NATS enforces per-client
  authorization; invalid credentials are rejected with a not-authorized CONNACK
  and the per-client connection is drained on disconnect. Verified by
  `shim_auth_test.go`. Enabled in the `cmd` binary via `-auth-passthrough`.
- **Graduated the MQTT core out of the `mqttport_wip` build tag**: it now builds,
  vets, and serves in the default tree. The `cmd` binary serves MQTT by default;
  `go test ./...` runs the transport/session/e2e/auth tests (in-process
  JetStream, real paho client) with no tag. The ported NATS-server files
  (`mqtt.go`, `compat_*.go`) are excluded from lint to stay diff-able upstream.
- Native protocol test suite driving real MQTT clients against the broker
  (in-process JetStream): MQTT 3.1.1 QoS1/QoS2/wildcards/unsubscribe/fan-out
  (`protocol_test.go`), and **genuine MQTT 5.0** CONNECT + pub/sub with
  user-property round-trip via paho.golang (`v5_test.go`). The prior
  `TestE2EConnectV5` was misleading — paho.mqtt.golang is v3.1.1-only and
  silently fell back to v4; replaced with real v5 coverage.
- Established the testing strategy: the upstream suite assumes an embedded
  nats-server (config files, clustering, connz) and can't be ported wholesale,
  so coverage is native; the upstream files remain behind `mqttport_fulltests`
  as a reference. See `docs/PORTING.md`.
- Fixed three connection-layer bugs so stored delivery works: `closeConnection`
  now unsubscribes NATS subs (offline persistent-session queueing), `readLoop`
  routes ungraceful disconnects through `mqttHandleClosedClient` (Will on abrupt
  drop), and `parseFramedHeader` preserves case-sensitive MQTT header keys +
  restored the retained-publish hook in `processInboundClientMsg` (retained
  replay). Retained/offline/Will tests now pass under `-race`.
- Verified more ported features end-to-end with real MQTT clients: **HA
  cross-instance session takeover** (reconnect to a different broker instance
  resumes the session + offline queue), **v5 shared subscriptions** (load
  balanced), and **v5 message-expiry TTL** on retained messages. All 20
  internal/mqtt tests pass under `-race`.
- Conformance: `vibesrc/mqttconformance` passes **124/124 (MQTT 3.1.1)** and
  **106/106 (MQTT 5.0)** against the broker; wired into CI via
  `test/conformance/run.sh` with a ratcheting per-version baseline
  (`test/conformance/baseline/conformance-v{3,5}.txt`). Documented the Eclipse
  Paho interop incompatibility (it uses client IDs with spaces, which a
  NATS-backed broker rejects) in `docs/CONFORMANCE.md`.
- Client IDs: the broker now accepts **arbitrary valid-UTF-8 client IDs**
  (spaces, `.`, `*`, `>`, non-ASCII), matching mainstream brokers and MQTT 5.0
  [3.1.3.1], instead of requiring NATS-safe names. The client ID is only used in
  NATS subjects via `getHash` (the QoS2 dedup subject now hashes it too). This
  unblocked the Eclipse Paho v5 interop suite, which now runs (~14 pass; the
  remaining failures are being triaged). Conformance stays 124/124 + 106/106.
- Remaining: triage Paho v5 interop failures; WebSocket transport.
- Multi-arch (amd64/arm64) distroless container; CI (build/test/lint/docker) and
  release (ghcr image + binaries) workflows.
- Conformance harness for `vibesrc/mqttconformance`, `nats-io/mqtt-test`, and the
  Eclipse Paho interop suite, with ratcheting known-failure baselines.
- The ported upstream nats-server MQTT regression suite (`mqtt_test.go`,
  `mqtt_v5_test.go`, ~650 tests) now **runs** in CI against the standalone
  broker (behind `mqttport_fulltests`), via an embedded-nats-server test
  harness (`fulltests_harness_test.go`) that maps the suite's
  embedded-server assumptions onto the shim `Server`. Cluster,
  MQTT-over-WebSocket, and TLS-config subsets that reach genuinely
  unportable surface (unexported upstream JetStream internals, the
  unimplemented WebSocket transport, missing cert fixtures) are parked as
  unbuilt references under `mqttport_clustertests` / `mqttport_wstests` /
  `mqttport_tlstests`. `go test -tags mqttport_fulltests -race
  ./internal/mqtt/` passes with 0 failures and 0 skips. Seven upstream tests
  for the inapplicable config-based auth surface were removed (authorization
  is delegated to NATS auth passthrough); three cross-topic/cross-QoS tests
  were made order-independent; the obsolete arbitrary-client-ID rejection
  case was removed; and the inherited QoS2 flaky-test skip was re-enabled.
  See `docs/PORTING.md` ("Testing strategy").

### Fixed

- **Inbound QoS 2 PUBREL could lose the message outright.** `mqttProcessPubRel`
  removed the hold record *before* the onward store, so three windows discarded the
  message and then told the client the exactly-once exchange had succeeded: a delete
  JetStream committed while its response was lost (or a crash right after the
  commit), a delete that succeeded followed by a failing onward store, and a
  malformed record already deleted by the time its parse failed. The retry found
  nothing and was answered `0x92`.

  The hold record is now a two-state machine (`internal/mqtt/qos2_move.go`): a
  create-only CAS writes `held`, the message is delivered onward **first**, and only
  then does a CAS at the held record's exact sequence replace it with a terminal
  `delivered` tombstone. A PUBREL that finds the tombstone answers `0x00` without
  re-delivering — indefinitely, and independently of any deduplication window. Every
  record carries an immutable `Nmqtt-Q2Gen` generation ID: the subject
  `$MQTT.qos2.in.<hash>.<pi>` is reused across session generations, so a stream
  sequence is not an identity, and the finalizer reconciles by generation and
  abandons without mutating when it finds a foreign one rather than overwriting a
  newer generation's undelivered payload. A record with no usable generation is
  classified `corrupt` and disposed of, never mistaken for a duplicate. Cleanup that
  leaves a *nonterminal* record now withholds the PUBCOMP until the delete is
  confirmed, because a PUBCOMP frees the packet identifier; only the inert tombstone
  is reaped asynchronously. Duplicate-PUBLISH detection moved from a brittle
  `10077` + cause-text match to the structural CAS-rejection codes. The onward store
  carries a stable `Nats-Msg-Id` derived from the generation, which narrows (but is
  not relied on for) duplication of the *stored* copy on a retry.

  Costs no extra round trips on the success path (load + store + commit, where the
  old path was load + delete + store). Residual, documented in
  `docs/CONFORMANCE.md`: the live core-NATS fan-out is at-least-once across the
  store-to-commit interval, and the duplicate stored copy is suppressed only while
  JetStream deduplication survives — which it does across a broker restart and a
  stream-leader failover, but not across a nats-server process restart.

  **Operational note:** `$MQTT_qos2in` changes from `DiscardNew`/`DiscardNewPer` to
  `DiscardOld` so the terminal CAS replaces rather than being rejected. A stream that
  does not match the required configuration now **fails broker startup** with
  recreate guidance instead of being updated in place. Upgrading an existing
  development store means stopping **all** brokers for the account and recreating the
  stream — **this is not a rolling upgrade**.
- **An MQTT client could read the entire internal `$MQTT.>` namespace by
  subscribing to it.** Only three delivery prefixes (`$MQTT.sub.`,
  `$MQTT.deliver.pubrel.`, `$MQTT.qsub.`) were rejected, carrying an upstream
  TODO wondering why it bothered "since one can subscribe to `#`". A wildcard
  filter indeed cannot match a `$`-topic ([MQTT-4.7.2-1]), but an *explicit*
  filter is not "reserved" by that rule — and the broker's state moves as
  ordinary NATS traffic on those subjects, so `$MQTT/msgs/#` yielded every
  QoS 1/2 message in the account and `$MQTT/rmsgs/#` every retained message,
  regardless of the client's own per-topic NATS permissions. SUBSCRIBE now
  rejects the whole reserved prefix with SUBACK 0x80. A session **record**
  carrying such a subscription — only a development broker predating this deny
  could have written one — is **refused** at the CONNECT restore boundary,
  fail-closed and read-only, naming the offending key and telling the operator to
  stop the account's brokers and recreate the bridge-owned `$MQTT_*` streams. That
  check runs ahead of the generation check on purpose: this deny predates
  generation-scoped naming, so the record that really exists carries no `gen`, and
  the operator would otherwise be told only that. A clean-start CONNECT is refused
  too, since its discard would delete the record's consumers. Merely skipping the
  filter would have re-denied it on every reconnect forever, and migrating the
  record in place is what the pre-release policy in `CONTRIBUTING.md` excludes.
  **Security fix.**
- **A Will topic could still publish into `$MQTT.>`.** `mqttProcessPub` rejects
  a client PUBLISH into the reserved prefix, but a Will never goes through that
  path: the immediate Will is delivered by `mqttInitiateMsgDelivery`, and a
  delayed Will is replayed from the outbox over the **service** connection,
  where the client's own NATS permissions do not apply at all. The Will topic is
  now checked at CONNECT — after subject mapping, so a mapping cannot land in
  the namespace either — and rejected with CONNACK 0x05 (3.1.1) / 0x87 (5.0),
  which also means no outbox record is ever created. **Security fix.**
- **The broker's internal delivery subscriptions moved off the per-client NATS
  connection** onto the service connection. They are not user topics, but
  creating them on the permission-scoped connection meant a client's NATS
  identity could read them directly (`$MQTT.sub.>` is a JetStream push
  consumer's deliver subject, i.e. every QoS 1/2 delivery in the account), and
  it made `deny subscribe: ["$MQTT.>"]` undeployable — NATS evaluates deny
  before allow, so the three prefixes could not be carved back out. The
  recommended ACL now denies the whole namespace in both directions, and
  `test/conformance/nats.conf` ships it so the suite proves it does not break
  QoS 2, retained replay, or shared subscriptions. **Security fix.**
- **The JetStream domain is resolved from one source and validated.** The MQTT
  core carried an upstream `TODO record domain name in as here` in front of a
  three-branch resolution of which only one branch was reachable: `-js-domain`.
  The other two (`Options.JetStreamDomain`, `Options.JsAccDefaultDomain`) exist
  for leafnode and per-account topologies a standalone broker cannot have and
  were never populated outside tests; they are removed. `-js-domain` is now
  validated at startup as a single subject token — it is interpolated into both
  `$JS.<domain>.API.*` and the persisted session subject, where a dot, wildcard,
  or invisible character silently yields unroutable requests and a session
  namespace that can never be read back. The broker also no longer builds a
  `ClientInfo.Cluster` from its own node name (a hostname or `mnb-<nuid>`, never
  a NATS cluster name); nothing was misrouted, since the `Nats-Request-Info`
  header is stripped before publishing, but the value was dead weight waiting to
  be believed.
- A QoS 1/2 publish racing a still-in-flight SUBSCRIBE from another client
  could be **permanently lost** to the new subscription: the messages stream
  uses interest-based retention, and a message stored before the
  subscription's JetStream consumer registered interest was discarded
  outright by the stream (sequence burned, normal PubAck, nothing stored).
  QoS 1/2 stores now wait out any SUBSCRIBE currently being processed in the
  account (`mqttSubsBarrier`) before storing, so a publish processed after a
  SUBSCRIBE is always visible to it. Fixes Eclipse Paho interop
  `test_request_response` and `test_subscribe_options`; the suite is now
  **26/27 pass, 1 baselined** (`docs/CONFORMANCE.md`).
- A message published shortly after receiving a QoS 1/2 SUBACK could miss the
  new subscription's first delivery attempt and only arrive after the
  JetStream consumer's AckWait (30 s): the SUB on the consumer's deliver
  subject was buffered client-side with no round-trip before the SUBACK was
  sent. The deliver-subject subscription is now flushed before SUBACK. The
  narrower pre-SUBACK race is closed by the subs barrier in the entry above.
- MQTT 5.0 **outbound flow control**: a QoS 1/2 delivery withheld because the
  session is at the client's advertised Receive Maximum is now released
  **promptly** when the client frees an in-flight slot (PUBACK / failed PUBREC /
  PUBCOMP, and on session resume), by NAKing the oldest withheld delivery on its
  JetStream ack subject so JetStream redelivers within milliseconds instead of
  after the consumer's AckWait (default 30 s). Ordering is preserved and the
  redelivery is a first transmission (DUP=0). Fixes Eclipse Paho interop
  `test_flow_control1`; the suite is now **24/27 pass, 3 baselined**
  (`docs/CONFORMANCE.md`).
- JSA transport: a message with an empty payload (a bare JetStream ack, or a
  `$JS.API.CONSUMER.DELETE` request) was published with a stray trailing `"\r\n"`
  still attached because the payload-trim guard in `processInboundClientMsg`
  required `size > 0`. nats-server silently ignores such acks, so the consumer's
  ack-pending slot never freed and management deletes never applied. The trim now
  also covers zero-length payloads (`size >= 0`). Affects every `jsa.sendAck` /
  `jsa.sendMsg(subj, nil)` call site.
- A force-disconnected session (e.g. MQTT 5.0 Receive Maximum exceeded, reason
  147) left its inbound QoS2 dedup/hold entries (`$MQTT_qos2in`) behind
  forever, so a later session for the same client ID could collide by Packet
  Identifier and see a stale payload resurface on PUBREL. `sess.clear()` now
  purges those entries whenever a session is discarded (never on resume).
  Fixed two Eclipse Paho interop suite failures (`test_payload_format`,
  `test_publication_expiry`), which failed only in the full sequential suite
  after `test_flow_control2`'s QoS2 flood.
- Nine bugs found by getting the ported upstream MQTT suite to run and then
  driving its skip count down (`mqttport_fulltests`, see `docs/PORTING.md`):
  - MQTT 5.0 **No Local was a no-op for QoS0 direct delivery**: the check
    relied on `pc.isMqtt()`, true in upstream for a same-server in-process
    delivery but never true in the standalone broker (every QoS0 delivery
    arrives over a real NATS subscription through a synthetic per-subscription
    client). Now uses the same authenticated origin-marker check QoS1/2
    delivery already relied on, computed for every QoS (previously skipped for
    QoS0 as an optimization tied to the now-nonexistent in-process fast path).
  - Re-**SUBSCRIBE to an already-subscribed topic leaked the old NATS
    subscription**: a new `nats.Subscribe` was created without unsubscribing
    the previous one for the same delivery-subject id, so the stale
    subscription (with stale QoS/No-Local/Subscription-Identifier metadata)
    kept delivering alongside the new one.
  - A **malformed SUBSCRIBE filter** with the multi-level wildcard not in the
    last position (e.g. `foo/#/bar`) was never rejected at parse time; it
    converted to an invalid NATS subject and reached `nats.Subscribe`,
    disrupting the connection's other in-flight requests instead of failing
    just that filter with SUBACK 0x80. Now validated per Spec [MQTT-4.7.1-3]
    before conversion.
  - **Retained-message payloads were truncated by 2 bytes**: a
    trailing-`"\r\n"` strip left over from upstream's raw wire-protocol
    framing (which this shim's messages never carry) silently dropped the
    last 2 bytes of every retained message's payload.
  - **Connected-client accounting only ever grew**: nothing removed a client
    from the server's client map on disconnect.
  - A **cold restart could resurrect a deleted retained message**: the
    direct-from-stream load path didn't recognize a zero-length body (the
    delete-tombstone convention) as "no retained message."
  - A **retained message published shortly before a matching SUBSCRIBE could
    be missed**: the in-memory index of which subjects have a retained message
    is populated asynchronously; the authoritative direct-stream fallback only
    ran for subjects the index already knew about, and a literal filter now
    always queries the stream directly. In addition the publishing node now
    seeds its own retained index/cache synchronously at store time, making
    retained-then-subscribe deterministic on that node (wildcard filters too).
  - A **deleted retained message could resurface after a cold restart**: the
    retained stream is `MaxMsgsPer=1` + `AllowMsgTTL`, and storing an
    empty-body delete tombstone does not evict the prior message the way a
    non-empty replacement does — so the old logic (delete the tombstone) left
    the original to reload on restart. Now the (TTL-less) tombstone is kept as
    the subject's last message (read back as "deleted") and the prior message
    is deleted by its known sequence.
  - An **MQTT client could publish into the internal `$MQTT.*` namespace**
    (e.g. mapping `$MQTT/JSA/...` → `$MQTT.JSA.*`) to forge session-persist /
    JSA traffic and evict other sessions. `mqttProcessPub` now rejects any
    client PUBLISH mapping into the reserved `$MQTT.` prefix, independent of
    NATS permissions; the example `test/conformance/nats.conf` also now denies
    `$MQTT.>` publish for the client user (defense in depth). **Security fix.**
- Log capture now works under the fulltests harness (`SetLogger` routes the
  shim's slog output to the capture logger), which also brought three small
  production log improvements: client log lines carry the client id, a
  readLoop parse failure is logged at Error (protocol violations should be
  visible), and `closeConnection` logs "Client connection closed".
- Test-harness bug (not shipped code): the ported suite's
  `s.Shutdown(); o.StoreDir = <same dir>; s = testMQTTRunServer(t, o)` restart
  idiom started a second embedded nats-server against the same on-disk store
  while the first was still running, corrupting JetStream state.
  `testMQTTRunServer` now reuses the still-live instance for a StoreDir it
  already has one for.

### Notes

- MQTT 5.0 is always enabled (no experimental flag).
- The MQTT listener is not yet served pending completion of the core port.
