# Conformance status

The broker is tested against the conformance suites named in the project plan.
Results below are from running each suite against the running broker
(`cmd/mqtt-nats-bridge`) backed by a JetStream-enabled NATS server.

## vibesrc/mqttconformance (primary) ✅

Rust CLI mapping tests to OASIS normative statements (e.g. `MQTT-3.1.0-1`).

| Protocol | Result |
|----------|--------|
| MQTT 3.1.1 | **124 / 124 passed** |
| MQTT 5.0   | **106 / 106 passed** |

Reproduce:

```sh
git clone https://github.com/vibesrc/mqttconformance && (cd mqttconformance && cargo build --release)
# broker listening on :1883 (e.g. docker compose -f test/conformance/docker-compose.yml up)
mqttconformance/target/release/mqtt-conformance run -H 127.0.0.1 -p 1883 -v 3
mqttconformance/target/release/mqtt-conformance run -H 127.0.0.1 -p 1883 -v 5
```

`test/conformance/run.sh` automates this and gates CI on a ratcheting
known-failures baseline (`test/conformance/baseline/conformance-v{3,5}.txt`).

## Eclipse Paho interoperability suite ✅ runs; 26/27 pass, 1 baselined

The MQTT 5.0 suite (`client_test5.py`) is cloned at a pinned revision by
`test/conformance/run.sh` and driven by `test/conformance/paho_driver.py`, which
runs **one test method per process** under a `timeout` (several suite tests
busy-wait for their expected messages with no internal timeout and would
otherwise hang). The driver also sets the `host`/`port`/`topic_prefix` module
globals that the suite only assigns in its own `__main__`, not in `setData()`.

Of 27 tests, **26 pass**. The remaining 1 is baselined
(`test/conformance/baseline/paho.txt`) as deliberate, documented divergences —
CI gates on *new* failures only:

| Test | Why baselined |
|------|---------------|
| `test_subscribe_identifiers` | Spec-permitted delivery choice. The suite requires overlapping subscriptions on one client to arrive as a single PUBLISH carrying every matching Subscription Identifier; the broker delivers one PUBLISH per matching subscription (MQTT 5.0 [3.8.4], matching EMQX / Mosquitto 2.1+ / NATS). |

### Broker behavior fixed to pass this suite

- **Outbound flow control** (`test_flow_control1`): QoS 1/2 deliveries are
  throttled to the client's advertised Receive Maximum (JetStream
  `MaxAckPending` capped by `sess.rmax`), and a message withheld at the cap is
  now **released promptly** when the client frees an in-flight slot. When a
  PUBACK / failed-PUBREC / PUBCOMP frees a slot, the broker NAKs the oldest
  withheld delivery on its JetStream ack subject
  (`sess.deferredNaksLocked` → `jsa.sendMsg(subj, AckNak)`), forcing JetStream to
  redeliver within milliseconds instead of after the consumer's AckWait (default
  30 s). Ordering is preserved (ascending stream sequence, [MQTT-4.6.0-1]) and
  the redelivery is a first transmission (DUP=0, [MQTT-3.3.1-1]) since the
  withheld message was never sent. The same NAK path drains deferred deliveries
  on session resume (`resendPendingRedelivery`). Spec5 [4.9]. Tests:
  `TestMQTTv5ReceiveMaximumPromptRelease` /
  `TestMQTTv5ReceiveMaximumPromptReleaseQoS2` (`internal/mqtt/mqtt_v5_test.go`).
- **Zero-payload JetStream ack/management delivery** (uncovered while fixing
  `test_flow_control1`): the JSA transport's payload-trim guard in
  `processInboundClientMsg` (`internal/mqtt/shim_engine.go`) used `c.pa.size > 0`,
  so an empty-payload message (a bare JetStream ack, or a `$JS.API.CONSUMER.DELETE`
  request) was published with the sendq drain loop's trailing `"\r\n"` still
  attached. nats-server's `processAck` matches ack payloads exactly / by prefix
  with no default case, so a 2-byte `"\r\n"` was silently ignored — the ack never
  landed and the consumer's ack-pending slot never freed. Changed to `>= 0` so the
  trim applies to legitimate zero-length payloads too. This affects every
  `jsa.sendAck` / `jsa.sendMsg(subj, nil)` call site, not just flow control.
- **Deliver-subject interest flushed before SUBACK**: the JetStream consumer for
  a QoS 1/2 subscription was already created synchronously before SUBACK, but the
  NATS subscription on its deliver subject (`$MQTT.sub.<nuid>`) was issued without
  a round-trip — the post-Subscribe `Flush` in `processSub`
  (`internal/mqtt/shim_engine.go`) exempted all `$MQTT.*` subjects (it existed
  only to surface permission violations on user subjects). A publish arriving
  between SUBACK and interest registration missed the consumer's delivery attempt
  and waited out AckWait (30 s). `processSub` now flushes `$MQTT.sub.*`
  subscriptions too, so SUBACK implies the subscription is deliverable. (These
  internal delivery subjects now live on the service connection rather than the
  per-client one, and the flush follows them — see
  [Reserved `$MQTT.>` namespace](ARCHITECTURE.md#reserved-mqtt-namespace).)
  Regression test: `TestV5SubscribeThenImmediatePublish` (`internal/mqtt/v5_test.go`).
  The remaining pre-SUBACK race is closed by the subs barrier below.
- **QoS 1/2 publishes wait out in-flight SUBSCRIBEs** (`test_request_response`,
  `test_subscribe_options`): both tests race a publish against the second
  client's still-in-flight SUBSCRIBE (a suite bug makes their post-subscribe
  wait return instantly). The messages stream uses interest-based retention, so
  a message stored before the subscription's JetStream consumer registered
  interest was discarded outright by the stream — **permanently lost** to the
  new subscription (never delivered, not merely delayed; the store's PubAck
  even reports a normal sequence). No consumer start-position setting can fix
  this: the message is never stored. Instead, SUBSCRIBE processing
  (`processSubs`) now holds a per-account barrier and the single QoS 1/2 store
  site (`mqttInitiateMsgDeliveryJSA`) waits out every SUBSCRIBE in flight
  before storing (`mqttSubsBarrier`, `internal/mqtt/mqtt.go`), matching the
  effectively-synchronous subscription registration of in-memory brokers. A
  publish is only delayed while a SUBSCRIBE is actively being processed;
  otherwise the barrier is a single uncontended mutex check.
  Regression test: `TestV5PublishRacingSubscribe` (`internal/mqtt/v5_test.go`).
- **Payload Format Indicator** (`test_payload_format`): a v5 PUBLISH/Will that
  declares PFI=1 but carries non-UTF-8 bytes is now rejected — PUBACK/PUBREC
  reason **0x99** (QoS 1/2), DISCONNECT 0x99 (QoS 0), CONNACK 0x99 (Will). See
  `mqttProcessPub` / `mqttParseConnect` in `internal/mqtt/mqtt.go`.
- **QoS2 hold purge on session discard** (`test_payload_format`,
  `test_publication_expiry`): a force-disconnected session (e.g. `myclientid`
  flooded QoS2 PUBLISHes past Receive Maximum in `test_flow_control2` and was
  disconnected with reason 147) used to leave its `$MQTT_qos2in` dedup/hold
  entries behind forever (`MaxMsgsPer: 1`, `DiscardNew`). A later session for
  the same client ID then collided by Packet Identifier and could have a stale
  payload resurface on PUBREL — the "stray `message N`" that broke these two
  tests when run after `test_flow_control2` in the full sequential suite (both
  passed standalone). `sess.clear()` (`internal/mqtt/mqtt.go`), the single
  funnel for every session-discard path, now also purges the discarded
  session's QoS2 hold entries via a JetStream `STREAM.PURGE` filtered on the
  client's hashed subject prefix. Regression test:
  `TestSessionClearPurgesQoS2Hold` (`internal/mqtt/qos2_purge_test.go`).
- **Server Keep Alive** (`test_server_keep_alive`): configurable cap via
  `MQTTNB_KEEPALIVE_MAX` / `-keepalive-max` (0 = no override), advertised in
  CONNACK. The conformance environment sets it to 60.
- **Subscribe failure** (`test_subscribe_failure`): with auth passthrough, a NATS
  subscribe-permission denial is detected (a post-`Subscribe` `Flush` surfaces
  the violation in `LastError`) and returned as SUBACK **0x80**. The conformance
  NATS config (`test/conformance/nats.conf`) denies the `mqtt` user subscribe on
  `test.nosubscribe`. The SUBACK Reason String is kept concise: the interop
  client mis-parses multi-byte property-length varints, and a long reason string
  would trip it (its own bug, but concise reasons are good practice regardless).
- **Response Information**: `MQTTNB_RESPONSE_INFO` / `-response-info` supplies a
  CONNACK Response Information string when a client sets Request Response
  Information = 1 (spec MAY; off by default, not required by the suite).

### Running it locally

```sh
docker compose -f test/conformance/docker-compose.yml up --build -d
SUITES=paho BROKER_HOST=127.0.0.1 BROKER_PORT=1883 ./test/conformance/run.sh
```

The compose file wires auth passthrough + the deny ACL + `MQTTNB_KEEPALIVE_MAX=60`.
Note the service connection uses `MQTTNB_NATS_USER`/`MQTTNB_NATS_PASS` with a
credential-less `MQTTNB_NATS_URLS`: `DialClient` reuses that URL for every
per-client auth-passthrough dial, and a URL-embedded userinfo would override the
per-client credentials (nats.go `connectProto`), so all clients would inherit the
service's rights.

### Historical note (resolved)

Previously the suite failed at `setUpClass` because a NATS-backed broker rejected
client IDs containing spaces (its cleanup helper uses `"clean retained"`).
Rejecting such IDs is spec-allowed (MQTT [MQTT-3.1.3-5] / 5.0 [3.1.3.1]: a server
MUST accept only `[0-9a-zA-Z]`, 1–23 bytes, and MAY accept more). We chose to
**relax** the broker to accept arbitrary valid-UTF-8 client IDs (see below), which
unblocked the suite.

## nats-io/mqtt-test

Not run here — the module was not fetchable in the build environment. It is also
exercised by the ported upstream external tests (`mqtt_ex_*`, tag
`mqttport_fulltests`) when the `mqtt-test` / HiveMQ `mqtt-cli` binaries are on
`$PATH`.

## Client-ID handling

The broker accepts **arbitrary valid-UTF-8 client IDs** (spaces, `.`, `*`, `>`,
non-ASCII), matching mainstream brokers and MQTT 5.0 [3.1.3.1]. Only UTF-8
validity and the NUL character are enforced (`mqttValidateString`). Empty client
IDs are accepted only with clean-session/clean-start set (the broker assigns
one, per [MQTT-3.1.3-6/7]).

This is a deliberate divergence from NATS's embedded MQTT, which requires
NATS-safe client IDs. The client ID is only used as a NATS subject token via a
hash (`getHash`) — sessions and the QoS2 dedup subject
(`mqttQoS2InternalSubject`) — so arbitrary characters are safe. See
`internal/mqtt/mqtt.go` (`mqttParseConnect`, `mqttQoS2InternalSubject`) and
`internal/mqtt/clientid_test.go`.

## Known deviations

All are tracked in `docs/ROADMAP.md`.

### Shared-subscription QoS 2: affinity holds; packet-identifier continuity does not

A QoS 2 message the broker has started sending to a Shared Subscription member
now stays with that member. Ownership is a durable assignment claim on
`$MQTT_qos2assign`, elected by a create-only CAS per
`(group durable, source stream sequence)` and honored by every contender
indefinitely; the message is copied onto the owning session's own
generation-scoped `$MQTT_out` subject and delivered by a durable only that
session generation can read (`internal/mqtt/qos2_shared_affine.go`). So:

- UNSUBSCRIBE, disconnect, or the owner being the last holder of the group
  durable no longer re-offers the message to another member;
- the exchange completes with the client it began with when that client returns;
- if the owner's session ends, the message dies with it — which MQTT 5.0 [4.8.2]
  permits — rather than being redistributed.

One failure mode is outside the protocol's control and is recorded rather than
hidden: if the session's affine JetStream consumer is destroyed out of band,
`$MQTT_out`'s interest retention deletes the copies it still held, and those
QoS 2 messages are lost. Because the affine side of an assignment is recorded
**before** its copy is acked, that outcome is distinguishable from a completed
delivery in the durable state alone — no broker has to have witnessed the
consumer disappear — and each affected assignment is marked `Lost`: a terminal
state that is deliberately **not** reapable, so the record survives for an
operator to find instead of being written off as a completed delivery.

That discrimination holds **while the owning session record still exists**. If the
record disappears first — the session expired, or a broker died without tearing it
down — a legitimate teardown deleting the consumer and an out-of-band destruction
of it look identical, so orphan reconciliation records `Discarded` (which
MQTT 5.0 [4.8.2] permits) rather than `Lost`. A `Lost` flag already committed is
never automatically removed; what cannot always be recovered afterwards is a loss
that occurred before the record vanished and that no broker had yet recorded. A
discarded assignment is itself retained until the source is proven closed, so
reaping it can never let a redelivery elect a second winner.

One deviation remains.

Packet-identifier continuity across a broker restart or failover is **no longer** a
deviation, here or on any other outbound path: a started delivery's identifier is
durable and is restored before the redelivery. See "Outbound in-flight identifiers"
below.

**Mixed-version operation.** The storage schema is rolling-compatible: an older
broker simply ignores the new stream and the new session-record fields. The
delivery *protocol* is not. An older broker serving the same shared group never
consults `$MQTT_qos2assign`, so during a rolling upgrade it can still redistribute
a started delivery that a newer broker has claimed. Shared-subscription QoS 2
affinity holds only once **every** broker that can serve the group runs a
q2aff-aware version. There is no runtime enforcement of this today; a
cluster-wide capability gate is noted as a possible follow-up.

Plain (non-shared) subscriptions no longer drop an unacknowledged QoS 1/2 PUBLISH
on UNSUBSCRIBE — see the next section.

### UNSUBSCRIBE completes what it started

An UNSUBSCRIBE that leaves started QoS 1/2 deliveries behind converts the
consumer into a **drain** rather than tearing it down
(`internal/mqtt/unsub_drain.go`). The durable and its delivery subscription
survive, already-started deliveries keep being redelivered and completed, and
messages published to the filter afterwards are discarded — the client
unsubscribed, so it is not owed them. This is what [MQTT-3.10.4-3] requires, and
it holds across a disconnect, a broker restart and a failover, because the drain
is part of the persisted session.

Only deliveries actually put on the wire are drained. One withheld by the
session's in-flight cap was never sent, so it is discarded rather than kept alive.
A re-SUBSCRIBE to a draining filter reclaims the drain instead of building a
second consumer beside it.

Residuals:

- **Offline backlog.** A drain's durable still holds interest in its filter while
  the session is disconnected, so messages published meanwhile accumulate in
  `$MQTT_msgs` until the next connection discards them or the drain is reaped.
  Bounded by the Session Expiry Interval and by storage capacity — global stream
  limits on `$MQTT_msgs` are refused at startup (they would evict undelivered
  messages), so no stream limit bounds it.
- **Stream-level consumer inactivity limits.** A drain must outlive an offline
  period, and `ConsumerLimits.InactiveThreshold` on `$MQTT_msgs` gives every
  consumer a deletion timer that no consumer config can opt out of. Where that
  limit is set — or cannot be read — the broker **refuses to drain**: UNSUBSCRIBE
  tears the consumer down immediately, as it did before, so a started QoS 1/2
  delivery is dropped and [MQTT-3.10.4-3] is not honored on such a stream. It logs
  a warning naming the limit. Running `$MQTT_msgs` without a stream-level consumer
  inactivity limit is therefore a deployment requirement, exactly as it already is
  for shared subscriptions. The `$MQTT_msgs` startup validator deliberately adds
  no check here: the limit can be imposed *after* startup, so these runtime
  refusals remain the authority (the same field **is** a startup failure on
  `$MQTT_out`, whose affine durables must survive offline sessions — see
  "Stream prerequisites" in `docs/ARCHITECTURE.md`).
- **Per-consumer inactivity timers.** A consumer created under the broker's own
  `consumer_inactive_threshold` carries the same timer per consumer. That one *is*
  escapable, so the broker clears it when the drain is committed, and retries at
  every resume while it is still set. The clear is best-effort: if it fails, the
  drained durable keeps its timer and can be reaped while the session is offline.
  Its started deliveries go with it, and that outcome is reported as **loss**,
  never as completion.
- **Plain QoS 1/2 durables are reconciled at CONNECT, and only there.** A
  resumed session verifies every plain QoS 1/2 consumer its record names —
  and recreates a reaped one under a fresh identity — **before the CONNACK**
  (see "Plain QoS 1/2 consumer reconciliation on resume" in
  `docs/ARCHITECTURE.md`), so an inactivity-reaped durable no longer leaves a
  silently dead subscription for the life of the session. What remains:
  - No backlog is *guaranteed recoverable* across a reap: the replacement is
    `DeliverNew`, the reaped consumer's pending went with it, and while
    messages published after the reap may survive because an overlapping
    consumer retained them, no cursor proves which of them the reaped
    consumer was owed. The gap is logged, not replayed.
  - Deliveries the reap destroyed surface as reserved identifiers or reported
    loss, never as completion.
  - An indeterminate verification **refuses the CONNECT**: the client must
    reconnect. Live re-SUBSCRIBE is not a recovery mechanism.
  - Out-of-band consumer deletion mid-connection is not detected;
    reconciliation happens at CONNECT. The same applies to the narrow window
    in which an inactivity reap has already been decided inside JetStream but
    not yet performed: interest registered after that decision cannot revoke
    it, and the lookup still reports the consumer present, so it is trusted
    for that connection and repaired at the next one.
  - Mixed-version: an unfixed broker still blind-trusts the record, so the
    gap persists wherever such a broker accepts the resume.
  - A re-SUBSCRIBE that adopts a drain whose durable was reaped inherits a
    dead entry until the next CONNECT repairs it.
  - A backlog delivery that runs out its phase-gate wait (reconciliation
    slower than the connection's AckWait), or that belongs to a filter the
    sweep reclaims unadopted, is redelivered rather than delivered in place:
    it is still marked a first transmission, but it can arrive **after**
    messages that were pushed later. Ordering across a redelivery is not
    preserved — the same residual any AckWait or NAK redelivery carries.
  - A message JetStream pushed into a reconciliation subscription that the
    broker then loses to a crash is redelivered with DUP set on what is, to
    the client, its first transmission: nothing durable records that the
    attempt was never put on the wire. This is the pre-existing behavior of
    any pushed-but-untransmitted delivery at a crash, not specific to
    reconciliation.
  - A backlog large enough to exhaust the NATS client's per-subscription
    pending-byte limit while deliveries are parked on the phase gate has its
    excess dropped by the client library before the broker sees it. Nothing is
    lost — the messages were never acknowledged, so JetStream redelivers them
    — but those redeliveries carry DUP on their first sight by the client,
    since the broker never got the chance to record them as withheld.
  - A resume refused **before the grant point** leaves the session's Session
    Expiry Interval and Clean flag untouched, in memory and in the record:
    they are applied only once every check that could refuse the CONNECT has
    passed. Its expiry clock does restart from the refused connection's close,
    as it does after any network connection ends, so a client that repeatedly
    fails to complete a resume keeps its session alive for as long as it keeps
    trying.
  - The exception is the last step: once the granted terms are durably
    stored, a **failure to write the CONNACK itself** leaves the session
    running under them even though the client never saw the acknowledgement.
    Undoing them there would mean repairing a record that was correctly
    written, so the session keeps the terms its client asked for and the
    client re-grants them on its next connection — which, for a requested
    interval of 0, means the session ends at that close exactly as the client
    requested.
  - A filter subscription restoration skipped after the CONNACK (e.g. its
    PUBREL setup failed) has its reconciliation subscription swept and stays
    unavailable until the next CONNECT — a live re-SUBSCRIBE reaches the
    exists-branch and never recreates missing interest.
  - A live re-SUBSCRIBE that matches a **stale unmatched `Cons` entry** (an
    entry with no corresponding subscription, which the reconciliation
    deliberately does not verify) likewise installs no delivery interest and
    stays unavailable until a subsequent CONNECT reconciles it.
- **A drained QoS 2 source is held until its PUBREL is proven.** An ordinary QoS 2
  PUBREC acknowledges the stored message as soon as the PUBREL is stored. A
  *drained* one does not: on the interest-retained `$MQTT_out` a store with no
  consumer succeeds while keeping nothing, so the claim is released only once the
  PUBREL has been delivered, or found present at its sequence by the resume
  barrier. The cost is one redelivery of the source in the rare case where the
  store landed but its delivery did not; the alternative is losing the message.
- **Mixed-version operation.** An older broker ignores the `Drains` record field,
  so its orphan sweep can delete a drained durable and lose its started messages.
  Same class as the affine residual above.
- An unbounded drain is deliberate: a client that never acknowledges holds one
  durable per drained filter until its session ends.

The QoS 1/2 → QoS 0 re-SUBSCRIBE downgrade is out of scope — it replaces a
subscription rather than removing one — and shared-subscription QoS 1
redistribution to another member remains permitted.

### Outbound in-flight identifiers survive a restart and a failover

An unacknowledged outbound QoS 1/2 delivery is re-offered under its **original**
packet identifier after a broker restart, a failover, or a resume on a different
broker — not only on a same-process reconnect. [MQTT-4.4.0-1] requires exactly that,
for PUBLISH and PUBREL alike, and it now holds on every outbound path: plain
subscriptions, shared-subscription QoS 2, and drained filters.

The identity is durable. A started PUBLISH-phase delivery has an **in-flight
register** on `$MQTT_inflight` naming its packet identifier and source delivery, and
the PUBLISH is not transmitted until that record is confirmed; past PUBREC the
correlated PUBREL record owns the identifier, as it already did. On resume both are
read back before any source interest exists, so the redelivery reuses the identifier
the client is holding and a fresh allocation can never collide with one. See
"Outbound in-flight registers" in `docs/ARCHITECTURE.md`.

What this closes, beyond the deviation itself: an allocation could previously mint an
identifier that an unreplayed PUBREL record still owned, because the in-memory PUBREL
map was empty until that record was delivered.

Residuals:

- **Retained-message deliveries are not covered.** A retained QoS 1/2 send has no
  JetStream backing at all, so it has no register and cannot be redelivered after a
  restart in the first place; it is released on resume rather than kept (which is
  unchanged, and is what stops it leaking a Receive Maximum slot).
- **Promptness, not continuity, still depends on the reconnect being same-process.**
  The identifier is restored either way, but the immediate NAK-driven redelivery needs
  a live per-attempt JetStream ack subject, which a recovered entry does not have — so
  after a restart the redelivery waits out `AckWait`.
- **An unaccountable identifier space withholds new QoS 1/2 deliveries rather than
  refusing the session.** If the durable identifier space cannot be fully accounted for
  on resume — the walk failed, a register's body is corrupt, or two registers name one
  delivery on different identifiers — the session stops allocating identifiers until a
  later resume reads it cleanly: QoS 0 keeps flowing and reserved exchanges complete,
  but new QoS 1/2 deliveries are withheld and redelivered by AckWait. This is a
  deliberate choice over denying the CONNECT, and over letting a redelivery proceed
  under an identifier that cannot be proven to be the one the client holds. A session
  discard lifts it, since the identifier space it protected is retired with the
  incarnation.
- **A source acknowledgement that cannot be confirmed costs one discarded
  redelivery.** On the QoS 2 PUBREC path the source ack is confirmed; if that
  confirmation fails the source is *retired* — left unacknowledged on purpose, so
  JetStream re-offers it once more, and that attempt is acknowledged and discarded rather
  than delivered. The client never sees the payload twice: not before its PUBCOMP, not
  after it, and not after a broker restart, because the retirement is recorded on the
  in-flight register as well as in memory. If that record cannot be written, the
  exchange's PUBREL is **not** acknowledged away — while it survives, recovery reads it as
  proof the exchange is past PUBREC and writes the mark itself — so the protection does
  not degrade to best-effort; the cost is a legitimate PUBREL redelivery until the mark
  lands. The retirement is released when the source's acknowledgement is confirmed, so it
  is bounded by failures rather than by traffic.
- **A delivery lost out of band keeps its identifier reserved.** If the consumer
  behind a started delivery is destroyed externally, the delivery was lost rather than
  completed and the client may still hold its identifier, so the identifier is not
  reclaimed for the life of the session. The same applies to a register whose body
  cannot be read: its subject still names the identifier, so that identifier is
  reserved and the record kept. Either way it costs one reserved identifier and one
  Receive Maximum slot — the safe direction, since reusing it would put a second
  message on an identifier the client still has in use.
- **Shared-register takeover arbitration excludes unreadable records.** A resumed
  session CAS-claims every readable live or retired shared register before installing
  it, and every nonce-keyed rewrite honors that object claim, so a stale close cannot
  delete a register the new owner has recovered. An unreadable register cannot be
  rewritten while preserving its allocation identity, however, so it remains
  unarbitrated and a stale writer can still know its sequence. This is bounded to the
  existing corrupt-record state: recovery reserves the identifier, withholds new
  allocation and reports the condition loudly rather than reusing an identity it cannot
  prove free.
- **A crash can lose one surviving disconnect's shared purge.** When a session survives
  its disconnect, the purge frees its pending shared-group deliveries in memory and then
  removes their registers. That pair is not atomic: a crash in between leaves the
  registers intact with no durable verdict, and the next resume can restore those pending
  entries as ordinary live state wherever their sources are still live or unprovable (a
  provably complete source is dropped). Only a tombstone or a persisted nonce verdict
  makes recovery drop a register outright, so what is lost is that close's purge and what
  returns is the pre-purge state — the identifiers and Receive Maximum slots it had
  freed. Bounded to one crash per close and self-limiting: the next surviving close
  purges again. The same window applies to every close, not only the record-restored
  sessions the purge newly reaches — the DEFERRED close included, where the linearization
  points are the cancellation tombstones (written first), the exact deletes, and the one
  classified record save: a crash part-way through the rungs leaves some allocations
  durably tombstoned and others live with no verdict, and a crash after the rungs loses a
  fallback verdict whose nonce existed only in memory.
- **A deferred close whose record write DEFINITELY fails loses restart-survival of the
  deadline — unless a later rejection probe lands.** The in-memory purge stands, the timer
  is armed, and the verdicts stay staged for the next convergence point, but nothing
  durable records the disconnect time — so a restart before any later write reads the
  record as connected and the session waits for a reconnect instead of expiring. The same
  posture as the ordinary close's failed disconnect save. **Narrowed** by the
  record-rejection settlement below: when that failure was a definite CAS rejection, the
  probe re-saves current memory, so a LANDED probe makes the deadline durable after all.
  A contended or unreadable one does not, and neither does a failure that was not a CAS
  rejection.
- **A close parked by an uncertain record write is memory-only until it is applied.** The
  parked action carries the ORIGINAL disconnect time and the Will, both captured while the
  client still existed, and neither is reconstructible: a crash before the discharge loses
  the earned deadline and the Will outright, with the record still describing a connected
  session. For a finite session the origin broker's periodic expiry sweep may later stamp
  a disconnect time of its own — a LATER deadline than the client earned — and a clustered
  non-origin broker does not; a never-expiring session is not swept at all. The pending
  shared state is restored by the next resume and purged by a future surviving close.
- **A crash after the deferred save has landed but before the Will is published can lose
  the Will.** The deadline is durable at that point — including when the save landed
  ambiguously and a readback later proved it — but the Will existed only in the action the
  discharge consumed. Pre-existing: making a parked Will durable earlier is Will-outbox
  scope, not purge scope.
- **A refused discard's obligation is memory-only, so a crash or a remote takeover in that
  window drops it.** On a broker running this code, no discard runs on an unclassified
  sequence: `clear` refuses unconditionally, at entry and again with the record fence
  held. FOUR of the six call sites are REFUSAL-AWARE, each acting on the sentinel rather
  than destroying, and what each then does differs: the deferred discharge re-parks its
  consumed action (pre-existing); CONNECT's clean start rejects without evicting; and the
  clean close and session expiry park the discard and restore residency only when the
  shared helper ACCEPTS, failing closed to the historical evict-and-log when it declines.
  The two remaining callers
  that clear a transient object built from a record they just read have an unreachability
  proof rather than a recovery, plus the same fail-closed refusal if that proof were ever
  wrong; and the parking helper itself DECLINES for an already-parked action that is not
  the discard-only shape (below). What remains is that the parked discard exists only in
  session memory, so there are exactly THREE ways to lose it, and every statement below is
  conditional on all three: a crash before the discharge; a remote persist, which evicts
  the re-added object under the manager lock alone — correctly, since a record another
  broker owns is not this one's to delete, but the obligation goes with it; and the helper
  DECLINING, which needs neither of the other two and whose reachability is unknown. Absent
  all three, the next successful local classification retries the discard and completes it.
  A remote persist has two orderings and neither is a guarantee: arriving after the re-add
  it evicts, and arriving in the window between the caller's `removeSession` and the
  re-add it takes the no-local-session branch and starts a reconciliation worker that then
  finds the object resident and deliberately does nothing — so convergence there is
  eventual, through the uncertainty classification calling the object foreign, not
  immediate.
  - For a v5 DISCONNECT that overrode a non-zero Session Expiry Interval to 0, losing the
    obligation is a spec deviation and not merely a leak. The override is applied to
    `sess.expiryInterval` and `c.mqtt.sessionExpiry` in memory and never persisted, so the
    record still says `ExpiryInterval > 0` and `Clean = false`: a non-clean reconnect that
    meets the surviving record resumes a session the DISCONNECT ended and reports Session
    Present true, against Spec5 [3.1.2.11]. Same violation as the reconnect-race item in
    `docs/ROADMAP.md` §0 but a different mechanism — that one is purely in-memory, this one
    is a durable record on a broker that may never have seen the DISCONNECT — and it has
    its own §0 bullet there. All three loss paths above reach it, the declined one without
    any crash or takeover, so the deviation must not be described as crash-or-takeover-only.
  - A crash in the CLEAN close's window leaves an expiry-0, `Clean = true` record with its
    consumers: nothing reaps it autonomously (the retry pass needs residency, the expiry
    sweep skips `ExpiryInterval == 0`, and the consumer sweep reads the record's consumers
    as referenced), so only a later CONNECT on the same client ID discards it.
  - **The same crash can lose that close's WILL, because the recovery runs BEFORE the Will
    is published.** The refusal deliberately keeps the Will out of the parked action — a
    memory-only Will is dropped by a takeover, which is worse — so the close publishes it
    by falling through to `mqttHandleWill`, after the recovery has parked the obligation
    and restored residency. A crash in between loses the Will permanently: no durable
    intent exists for an owed-now Will. The window is PRE-EXISTING — today a failed
    `clear()` logs and reaches the same `mqttHandleWill` just as late — and this change
    keeps the same ORDERING while replacing that log with the recovery's own work: a
    parking call under `sess.mu` and a residency insert under the manager lock, and no
    log at all on the path that parks successfully. Whether the window is net wider is
    not established here and is not claimed. Reordering was
    considered and not taken: publishing first needs a second `mqttHandleWill` call site
    with a hand-derived takeover-fence baseline, and deferring the recovery until after
    the Will only trades this window for a longer one in which the session is evicted
    with its record intact. Closing it properly needs a durable, cluster-visible Will
    intent, which is Will-outbox scope. Recorded as a candidate follow-up rather than
    claimed safe.
  - A crash in the EXPIRY window can leave a finite record still saying
    `DisconnectedAt = 0` when the original disconnect save had failed, since that save is
    best-effort and the timer is armed regardless. An origin or standalone sweep may then
    stamp a new, LATER disconnect time, and a clustered non-origin broker deliberately will
    not — so if the origin never returns, the record can read as connected indefinitely.
    Pre-existing (the failed-disconnect-save residual), reachable through this window too.
  - A CONNECT refused this way asks the client to retry, and a crash after that rejection
    still loses the memory-only continuations the rejection preserved. Strictly smaller than
    the eviction it replaces, which lost them immediately.
- **For a session whose ambiguity never resolves, the recovery holds the object and the
  record instead of reaping them.** The expiry sweep's fallback — stamp a disconnect time
  and expire — is suppressed for a resident session, and that trade is deliberate: the
  fallback reaps by WRITING a new deadline onto a record nobody has classified, which can
  turn an ambiguous write that landed into a different record the resolver then calls
  foreign, dropping the local obligation during foreign disposal. A resident object means
  a broker is actively retrying, which is the class `reconcileResidentRecordState` already
  holds every other unresolvable session in. The cost, in full: growth is unbounded
  across client IDs (the residency maps have no quota) and each retained object holds
  its subscriptions, consumers, drains, pending deliveries, retained continuations and
  its parked action; both
  the object and the record keep the referenced consumers out of the orphan sweep's reach;
  and `reconcileResidentRecordState` performs a synchronous readback per unresolved candidate
  with no budget, ahead of the expiry, orphan-consumer and orphan-QoS2 sweeps on the same
  goroutine, so N unresolved sessions delay those three by roughly N × the JetStream API
  timeout and make shutdown latency linear in the same walk. For a record carrying
  `DisconnectedAt > 0` the recovery is additionally RE-ENTERED about once per sweep interval
  — the sweep's residency guard covers only its `DisconnectedAt == 0` branch — costing one
  record load, a timer, a gate acquisition and one warning line per stuck session per
  interval. A pass budget for that walk is a candidate follow-up, not part of this change.
- **The refusal-and-recovery protection is PER BROKER, not fleet-wide during a rolling
  upgrade.** It adds no stream, subject, record field or cross-broker message, so there
  is no decoder incompatibility in either direction and both versions read and CAS the
  same record. But an older broker keeps its own destructive-and-leaky behavior for the
  sessions it owns, and because the parked obligation is memory-only there is nothing
  another broker — older or newer — can adopt: a takeover simply evicts it. So the
  invariant holds for a session whose owning broker runs this code, and holds fleet-wide
  only once every broker serving the account does. Stated as scope, not as
  rolling-upgrade safety.
- **A parked action that is not the discard-only shape has unknown reachability, and keeps
  the historical evict-and-log outcome.** The shared recovery helper accepts an existing
  obligation only when it already ends at disconnect AND carries no Will. Anything else —
  a surviving lifetime, or an expiry-0 action owing a Will — declines: the action carries
  no owner identity, so nothing there can prove whose deadline or whose Will a retarget would
  overwrite. Whether those shapes can reach a discard caller at all is not established, and
  the design deliberately does not depend on the answer.
- **A void-register verdict staged during the CONNECT accept rollback is lost with the
  aborted object.** The rollback's record snapshot is taken inside the rollback, so a nonce
  staged after it is deliberately not credited (crediting it would claim durable what
  nothing wrote). What the surviving register then means has three shapes: with a resumable
  record, a live, lost or unprovable source has its identifier RESERVED fail-closed; a
  source proven complete has its register scheduled for asynchronous deletion, and a failed
  no-wait delete simply leaves it for a later reconciliation; and with the record ABSENT
  after a failed rollback (classified applied) the next CONNECT starts a fresh generation,
  so the old-generation register is ORPHANED — the orphan sweep's territory — rather than
  reserved. Pre-existing behavior of the rollback path.
- **Recovery's opaque and duplicate-source reservations are never purged.** A register
  whose body cannot be read, and the loser of a duplicate claim on one source, are held
  as identifier reservations that carry no durable name — so no durable-keyed purge can
  reach them, and a surviving disconnect leaves them reserved. Fail-closed by design:
  the identifier stays out of use rather than being handed to a second message, at the
  cost of one reserved identifier and one Receive Maximum slot each.
- **Mixed-version operation.** As with the affine and teardown protocols: an older
  broker never reads `$MQTT_inflight`, so it re-offers a started delivery under a
  fresh identifier and its orphan sweep does not know the stream. Continuity holds
  once every broker serving the account understands it.

### An ambiguous record write COMPLETES rather than aborts, once it is settled

A save whose outcome is unknown is settled by re-attempting its exact bytes at its own CAS
base, never by reading the base sequence and calling the write dead (see
`docs/ARCHITECTURE.md`). That closes the false-foreign disposal for a non-zero base. What
it does *not* promise:

- **A wedged proposal keeps the session unresolved across passes.** The old arm restored
  writability immediately, by guessing. Now a proposal genuinely stuck in process upstream
  leaves the session unresolved until the pipeline settles it (apply, or a leader
  transition clearing the in-process entry), bounded by the periodic pass cadence. That is
  an availability cost, not a correctness one: unresolved is the posture every caller
  already handles, and a wrong verdict is not.
- **For operations the client was never positively acknowledged for, the ambiguity now
  COMPLETES.** A SUBSCRIBE with no SUBACK, the 3.1.1 UNSUBSCRIBE (whose error closes the
  connection with no UNSUBACK), and a refused CONNECT's lifetime grant all keep their
  mutation, and once a resolution verdict is obtained that mutation is durable unless a
  foreign owner intervened. Both outcomes were always legal — the client was told nothing
  and must reconcile on reconnect — and the landed one was always possible; what changed
  is that it became deterministic. A granted Session Expiry Interval of 0 therefore ends
  the session at that close even though the CONNECT was refused, which is the
  grant-at-durability rule this broker already followed whenever the write happened to
  land.
  The shared QoS 2 affine provisioning is NOT a second acknowledged caller either: its
  save runs BEFORE the loop that persists shared memberships, and that later save is
  refused while the session is unresolved — so the per-filter SUBACK failure it reports
  is truthful, with no membership for that filter becoming durable.
- **SEPARATELY, the v5 UNSUBSCRIBE is acknowledged**, and its UNSUBACK is made truthful:
  the packet's true per-filter reasons are restored in place of the 0x83 whenever the
  attempt's own landed continuation has fired by the time the packet tail loads its flag.
  The residual is exactly that scope. Whenever PROOF is unavailable at the flag load — the
  resolution deferred by a busy fence or gate, an unreadable read, an ambiguous
  re-attempt, or a re-attempt rejected while the record reads at-base or is unreadable —
  the unsubscribe may be durably applied (already landed, or landing before or after the
  acknowledgment, including a verdict arriving between the flag load and the enqueue)
  while the 0x83 stands. The guarantee is defined at the flag-load linearization point,
  not at socket write time. It is NOT claimed strictly narrower than before: an
  ambiguously-successful re-attempt can durably apply an unsubscribe whose original
  proposal alone would never have landed, a new execution the forward-resolution posture
  accepts. 3.1.1 is unaffected — no per-filter codes, and the error closes the connection
  with no UNSUBACK.
- **A DISPLACED connection is not acknowledged, with one interleaving left open.** The
  acknowledgment is refused when this connection's one server-initiated DISCONNECT has
  already been ATTEMPTED, read under `c.mu` in the same critical section as the write —
  which is what every displacement reaching that point has done. (The tail also samples
  `sess.c == c` beforehand; that half is defensive and changes no reachable outcome
  today.) What is NOT covered is a displacement that detaches without ever notifying —
  one whose 0x8E the CONNACK gate refuses, or whose claim an earlier writer already
  spent — landing after the ownership sample: such a connection can still receive its
  UNSUBACK. That is the pre-existing "nothing gates non-DISCONNECT traffic" residual of
  the claim mechanism, narrowed here rather than closed.
- **An unconditional create (CAS base 0) keeps its existing classification.** A first-ever
  record write carries no expected-last-subject-sequence header, so it is never classified
  at-base and is never re-attempted; a late landing of such a create can still be read as
  another owner's record. Pre-existing headerless-create territory, stated as residual 7
  above.
- **A record found BELOW the attempt's base is classified foreign fail-safe**, not proven:
  sequences are monotonic within a stream incarnation, so that shape implies a reset or
  re-provisioned store. Nothing is adopted and nothing is written there.
- **A leader transition can also let this broker's re-attempt overwrite a FOREIGN
  same-base proposal, and that is not fixed here.** If another broker's takeover is
  accepted at the same base, survives the election unapplied, and applies before this
  re-attempt, the re-attempt lands last and erases it — the local session then settles as
  ours while the takeover is discarded by `MaxMsgsPer: 1`, and the displaced broker's
  client is evicted by the persist observation. This is the pinned per-subject-CAS
  linearization contract failing (`docs/ARCHITECTURE.md`, "JetStream behaviors this design
  depends on"), and it fails identically for EVERY arbitrating CAS this broker makes: the
  definite-rejection probe's re-attempt of current memory, the repair retry's exact-byte
  re-send, and any ordinary save. What this change contributes is FREQUENCY, not the
  window — the resolver now writes where it previously only read. Closing it requires an
  arbitration identity that survives a leader transition, across every writer; filed as
  its own §0 roadmap item rather than folded in here.
- **A leader transition can let BOTH of this broker's proposals commit, and that is
  accepted rather than prevented.** The shared CAS base ordinarily excludes the second,
  but a transition clears the in-process map while a replicated proposal survives, and a
  clustered apply does not
  re-check the expected sequence. Both landings carry IDENTICAL bytes, log order makes the
  re-attempt's sequence the surviving one, and `$MQTT_sess` is `MaxMsgsPer: 1`, so the
  duplicate is discarded on arrival and the end state is one record with those bytes at the
  adopted sequence. The cost is one consumed stream sequence and one extra (idempotent)
  persist notification. No cluster leader-transition test exercises the double commit
  itself — making an accepted-but-unapplied entry survive an election deterministically is
  not something this harness can do — so what is pinned by test is the CONSEQUENCE: two
  identical framed writes both landing leave exactly one record, with the attempt's bytes,
  at the sequence the broker adopted.
- **An ABSENT record is fail-safe too, and one window stays open.** A clustered stream
  checks the expected sequence only before proposing, so a proposal already accepted into
  the log applies even if an exact-sequence delete removed the record in between — an
  absent subject is therefore not proof that the attempt is dead. The resolver arbitrates:
  a re-attempt refused with **10164** (a write for the subject is in process, possibly that
  proposal) leaves the session UNRESOLVED rather than disposing it. A refusal with
  **10071** is classified foreign, and that is the residual: a leader transition clears the
  in-process map while a replicated proposal can survive it, so a proposal that lands after
  such a transition can still be read as another owner's. Narrowed from "absence is a
  verdict", not closed.

### A definite record-CAS rejection is settled by a probe, with bounded convergence

A rejected session-record CAS says the subject moved, and the only thing that evicts a
displaced session object otherwise is an at-most-once core-NATS persist notification. The
settlement re-attempts the write so a fresh CAS arbitrates ownership (see
`docs/ARCHITECTURE.md`). What it does *not* promise:

- **For an UNBOUND session, convergence is bounded by the pass's SCHEDULING, not by the
  sweep interval alone.** The settlement runs on the next periodic pass that reaches
  the session. That pass is the same serial, unbudgeted walk over every resident session
  that the residency residual above describes: it performs blocking JetStream work per
  candidate that needs it, so a session late in the walk converges after roughly the
  interval PLUS the backlog ahead of it (order N × the JetStream request timeout for N
  candidates doing round trips). A pass budget is a candidate follow-up, the same one the
  residency residual names; until it exists, the honest bound is "the next pass that reaches
  this session", not "one interval".
- **A BOUND session is settled before its next packet is dispatched, and that is narrower
  than "never serves a displaced client".** The gate runs per packet inside `mqttParse`,
  claims the ownership gate as the bound client, and serves only on a landed CAS; every
  other outcome closes the connection — a 0x8E ATTEMPT when the probe proved the session
  foreign, a 0x89 "server busy" attempt when nothing was established, and SILENCE when the
  session was found REBOUND (a takeover owns the notification and makes its own 0x8E
  attempt; a courtesy packet here would contradict it on the same socket). Each of those is
  an attempt rather than a delivered packet: a connection's one server DISCONNECT is claimed
  once (see (12)), the takeover notification is additionally gated on the CONNACK, and a
  write can lose to a concurrent close. A refused ownership claim is retried while this
  connection is still the owner rather than read as a verdict. What that leaves:
  1. **Outbound deliveries continue** to the displaced client until an inbound packet
     triggers the settlement, or the connection ends. Closing that needs a delivery-side
     gate, which is the "resident session never re-validates" item's territory.
  2. **A silent client with keepalive 0 converges only at unbind.** The bound is a
     conformant client's PINGREQ — about 1.5 keepalive intervals — and a client that
     promises nothing is owed nothing.
  3. **The packet that races the marker's arming is served.** A rejection that lands after
     a packet's pre-dispatch check is invisible to it; the NEXT packet gates. This is
     inherent to any check-then-dispatch: a barrier synchronised with the arming would
     deadlock, because arming happens inside the save on the dispatch path itself.
  4. **A no-verdict close may drop a connection whose session was still ours.** That is the
     availability side of failing closed. The Will then follows the ordinary close rules,
     which includes the rejected-close Will publication residual below.
  5. **While suspended by an unresolved write of ours, the connection fails closed
     immediately without probing** — no probe is possible — and the teardown follows the
     courtesy write, per (9). That 0x89 now goes through the shared claim like every other
     server DISCONNECT, so a displacement that already notified this connection suppresses
     it and the close is silent.
  6. **The rolling-upgrade asymmetry is unchanged**: protection is per owning broker (see
     the last bullet), so an older broker keeps its bound split-brain.
  7. **The AT-BASE false foreign verdict is CLOSED for a non-zero CAS base; two
     fail-safe residuals remain.** The uncertainty resolver no longer clears an ambiguity
     on a plain load at the base sequence: it RE-ATTEMPTS the attempt's exact bytes, so a
     late-committing proposal loses that arbitration instead of being read later as
     another owner's record. Still open, and NOT covered by that closure:
     - the **unconditional create** (base 0, the first-ever record write, which carries no
       CAS header and is therefore never classified at-base): an ambiguous create whose
       landing arrives late can still be classified foreign. Pre-existing
       headerless-create territory, not a property of the resolver.
     - **an absent record whose re-attempt is refused 10071** (nothing in process for the
       subject). That refusal does not prove the ORIGINAL proposal is dead: a leader
       transition clears the in-process map while a replicated proposal can survive it, so
       a proposal landing after such a transition is still classified foreign — evicting
       the session, closing a live connection with 0x8E and discarding its Will before its
       own write lands. Narrowed (10164 now defers instead of disposing) rather than
       closed; see the settlement section below.
  8. **An ambiguous probe's fail-closed close parks a memory-only teardown**, so a crash
     before resolution loses the parked Will and the original expiry deadline. That is the
     pre-existing parked-teardown exposure, newly reachable from this path.
  9. **The fail-closed teardown begins only after the courtesy 0x89 write returns, and that
     write is unbounded**, so a stalled v5 client defers it indefinitely. Pre-existing
     property of the parse-error footer, shared by every parse error today, and NOT
     gate-held — the settlement releases the ownership gate before returning. A claim
     already consumed by a displacement removes the write, and with it this delay, but only
     in that case.
  10. **The park after an ambiguous probe is not guaranteed.** Between the sentinel return
      and the close bookkeeping, the periodic pass's bound-uncertain resolver can classify
      foreign and dispose first, reaching the owed-Will arm the Will bullet below names.
  11. **The foreign arm's 0x8E is an unbounded courtesy write too**, as is the periodic
      pass's disposal 0x8E — which is additionally performed gate-held. Same class as (9),
      filed as its own §0 roadmap item. (When the claim or the CONNACK gate suppresses the
      notification there is no write and no delay; that is not a bound, only a case.)
  12. **At most one server-DISCONNECT ATTEMPT per connection, which is narrower than one
      delivered packet with the right reason.** The second-DISCONNECT class is closed: the
      notification is a per-connection claim (`mqttClaimDisconnectLocked`) shared by all four
      displacement paths, the parse-error footer, the packet-too-large 0x95 and the shutdown
      drain, so a 0x89 can no longer follow a 0x8E — or a 0x8E another 0x8E — on one socket,
      which is what [MQTT-3.14.4-1] forbids. Every enqueue-path writer consumes it in the
      same `c.mu` critical section as its write; `shutdownDisconnect` is the exception and
      RESERVES it, claiming under the lock and attempting `writeDirect` after releasing it.
      What remains:
      - **Zero notifications is a legal outcome.** The displaced arm closes synchronously
        while the displacer notifies asynchronously, so the socket can be gone first; a
        3.1.1 or size-suppressed build never reaches the claim; a connection displaced
        before its own CONNACK is closed silently by design ([MQTT-3.2.0-1]); and the
        shutdown drain can spend its reservation and then write nothing — suppressing a
        competing 0x8E — if the process dies or the deadline-bounded `writeDirect` fails
        after the unlock.
      - **A notification can be dropped for an already-acknowledged client.** The CONNACK
        gate is read under `c.mu` and the flag is stored inside the CONNACK write's own
        critical section, so a notifier entering AFTER that write always sees it. Entering
        BEFORE it, the notifier reads `false` and stays silent, and the client gets a
        success CONNACK followed by a silent close. Nothing can retract bytes or predict a
        future CONNACK, so this ordering is accepted; it is strictly better than the
        pre-CONNACK 0x8E it replaces. Its server-side half — the displaced CONNECT's own
        goroutine continuing until the asynchronous close lands — is unchanged by the claim
        and is arbitrated by the session record's CAS, which rejects every write it still
        attempts once another broker has moved the record.
      - **Which reason wins is race-determined**, so the winning attempt's reason — if it
        is delivered at all — can be stale: a 0x89 "try again later" claimed a moment
        before a real takeover is never corrected to 0x8E: written bytes cannot be retracted.
        A client told 0x89 by mistake reconnects and is handled normally.
      - **Non-DISCONNECT packets can still trail the final DISCONNECT** until the close
        lands — a PINGRESP, or an in-flight PUBLISH from a delivery goroutine — which
        [MQTT-3.14.4-1] also forbids. Nothing gates those; closing it needs a delivery-side
        gate, the "resident session never re-validates" item's territory.
      - **The notification write is still unbounded**, per (9) and (11), and the
        `processSessionPersist` overlap with a gate-held probe itself remains: that path
        runs on the JSA delivery goroutine and cannot wait on the gate.
      - **Per owning broker**, like every in-process fix here: during a rolling upgrade a
        session resident on an older broker keeps the old double-DISCONNECT exposure on its
        own sockets. No wire, stream or record state is involved (see (6)).
  13. **The gate hold is bounded against non-blocking writers only.** "At most one probe per
      arming, roughly three JetStream timeouts" needs the record fence to reach the waiting
      settlement once its current holder releases. The TRY-BARGE half of that is closed:
      `beginRecordTx` announces itself before it contends on `sess.mu`, and every
      non-blocking acquisition refuses while an announce is outstanding, so sustained
      off-readLoop writer activity can no longer pass the fence between try-callers while
      the settlement starves — at most one already-admitted transaction precedes it.
      What remains open is BLOCKING-vs-BLOCKING: `Cond` gives no FIFO, so a newly arriving
      `beginRecordTx` can win the post-broadcast mutex race against an older waiter, and
      identity churn can starve one waiter indefinitely even at a small constant count. In
      ordinary operation there is at most one blocking caller per session, which is an
      observation about the call sites rather than an enforced invariant; a same-client-ID
      CONNECT can still fail against a starved waiter's gate hold. The remedy also has a
      RECIPROCAL cost, accepted deliberately: while blocking writers keep arriving,
      non-blocking writers can be refused indefinitely, and their retries are event-driven
      (packet tails, redeliveries, later register stores, the periodic pass) rather than
      time-bounded — so a refusal on a session that then goes idle converts an immediate
      persistence into one that waits for the next event. The void-verdict release handoff
      is deferred the same way, which lengthens the memory-only window for a staged
      verdict (the periodic backstop's own attempt defers too; the usual convergence is
      the prioritized transaction's own landed save, since every landed save marshals
      current memory). And the never-expire close stamp, the one non-blocking writer that
      does NOT retry, takes its unordered fallback on more occasions — including with the
      fence FREE but reserved for an announced waiter. Nothing is marshalled at the instant
      of that refusal, but the prioritized writer can open its transaction and marshal
      `DisconnectedAt = 0` before the fallback stamps, so that arm reaches the SAME one-body
      exposure rather than a cost-free state. Waiter priority is a per-broker property of
      an in-process fence: during a rolling upgrade a session resident on an older broker
      keeps the original barge exposure until that broker upgrades, and no wire, stream or
      record state is involved either way.
  14. **A takeover still mid-flight can have its Will published by this connection's
      close.** The rebound arm applies the takeover's own discard rule as soon as the rebind
      is observable. Before the rebind there is nothing to observe: if the bounded retry
      gives up while the session is still ours (a CONNECT holding the gate for longer than
      the spin, without having rebound yet), the fail-closed close marks the Will owed —
      independently of whether its 0x89 attempt is written at all — and the
      takeover's later discard preserves an owed Will by design. Narrow, and the same
      Will-disposition race roadmap bullet 1297 tracks.
- **Sustained contention holds the object for as long as it lasts.** A readback that finds
  the record at the probe's own base sequence, or that cannot be performed at all,
  establishes nothing, so the marker stays and the object is kept until a later pass — at
  one probe per pass, indefinitely if the contention never clears.
  Reading the base
  sequence as ownership is not available as a shortcut: JetStream returns 10164 while a
  write for the subject is in process, so that read is compatible with the contender
  committing a moment later.
- **A rejected close still publishes its Will, and the settlement cannot retract it.**
  When the ordinary close's disconnect save is CAS-rejected by a remote takeover, the Will
  is published synchronously by that close, before anything has classified the rejection;
  a probe that later proves the session foreign cannot take it back. Pre-existing and
  untouched here — a CAS rejection is contention, not takeover evidence, so discarding the
  Will on one would drop a Will a session that is still ours owes, and parking it is
  memory-only and non-transferable. Its own roadmap item, together with the periodic pass's
  bound-foreign disposal, which reaches the same owed-Will arm.
- **A takeover with the notification lost and NO local rejection is still not reaped.**
  The probe runs only where a rejection armed the marker. A resident session that never
  writes again is never re-validated, so what this change bounds is convergence for
  rejection-caused staleness, not for staleness in general. Filed as its own roadmap item
  rather than papered over.
- **The marker is memory-only.** A crash drops it, which is benign: nothing durable was
  written and the crash removes the object the settlement would have removed.
- **The record arbitrates the last accepted state transition; it is not a liveness
  lease.** A broker that crashes between its accept save and its success CONNACK leaves a
  record that reads as connected with no acknowledged connection behind it, and `Origin`
  is only a configured node name (uniqueness is documented, not enforced). Ownership is
  revalidated by CAS, and no part of this design infers liveness from the record.
- **Protection is PER BROKER and behaviourally asymmetric during a rolling upgrade.** No
  stream, subject, record field, header or cross-broker message is added, and the probe is
  an ordinary session save an older broker already understands — so wire compatibility is
  symmetric in both directions. But a new broker settles only its OWN rejections; an older
  broker keeps its stale objects and its bound split-brain — the pre-dispatch gate protects
  the sessions THIS broker owns, not the account. During a mixed-version window
  the contender in process may be the older broker, which is precisely why the probe
  retries a CAS instead of trusting a read.
- **The never-expire disconnect stamp is a Will-fence fix, not an expiry change.** A
  never-expiring session's close records the disconnect time so a later write cannot read
  as a live newer connection and cancel an owed delayed Will. The stamp is taken inside a
  RECORD TRANSACTION when one can be had, which is what orders it: `sess.mu` alone leaves
  a body an off-readLoop writer had already marshalled with `DisconnectedAt = 0` free to
  land after the Will's origin sequence is captured, while holding the fence means every
  earlier marshal has completed and advanced the sequence that capture reads. The
  acquisition is NON-BLOCKING, and what a refused acquisition costs is stated exactly: the
  close **still stamps**, unordered, so every marshal from that point on — the rejection probe
  included — carries the disconnect time, and the residual is at most the ONE body already
  marshalled inside an open transaction, which no non-blocking form can order against.
  Refusal has two causes and the same cost. An open (or already-admitted) transaction is
  the case above. WAITER PRIORITY refuses on a FREE fence reserved for an announced
  blocking writer: nothing is open at the refusal, but that writer can take `sess.mu`,
  open its transaction and marshal `DisconnectedAt = 0` before the stamp is taken, so the
  exposure is the same one body rather than none. Because this site does not retry, waiter
  priority gives more occasions of this same residual — not a cost-free arm (residual 13
  above).
  Stamping there is not optional: the probe is a writer this change ADDS, and skipping it
  would leave the probe free to persist current memory with `DisconnectedAt = 0` at a newer
  sequence, which the fence reads as a reconnect. The fence is not WAITED for, because
  waiting bought two defects for one fix — a wait spans the very writer being waited on
  going ambiguous, which makes the close's entry uncertainty check stale, and a park taken
  after a wait timestamps the close up to a JetStream timeout late, re-opening an
  already-elapsed Will Delay for a reconnect to cancel. The in-fence arm additionally
  re-checks that the record state is still resolved and parks the teardown if it is not;
  the refused arm makes no such check, because without the fence there is no sound one
  to make. The field is inert for expiry (every reader pre-filters on never-expire), and
  the pre-existing finite-close window where the purge's own write publishes
  `DisconnectedAt = 0` before the disconnect save corrects it is unchanged — not widened,
  and not bounded in time either, since that corrective save can itself fail and the
  window then lasts until some later settlement succeeds.

### Session teardown: arbitrated across brokers, with two upgrade-window residuals

Discarding a session (clean start, a v5 DISCONNECT with expiry 0, session expiry,
takeover-discard) now runs under a durable **teardown claim** on the session record
and deletes only resources named after the **retiring session incarnation**
(`internal/mqtt/sess_teardown.go`, `sess_incarnation.go`). A broker that loses the
claim deletes nothing; the release is a sequence-targeted delete whose outcome is
classified by re-reading, never assumed; and a CONNECT that finds a claim finishes
it before starting fresh, so a crashed teardown self-heals rather than leaking.

One residual, confined to an upgrade window, and one hard requirement:

- **A session record without a generation is rejected, not resumed.** Records
  written before generation-scoped naming existed carry no `gen`, and a CONNECT that
  finds one fails with recreate guidance rather than falling back to hash-only
  names. That fallback is not a weaker form of the same safety: such a session has
  no identity to scope an arbitration with, so its inbound QoS 2 release could not
  be arbitrated against a session discard, and the discard would blind-purge a held
  payload whose releaser had already claimed it — silent loss, and strictly worse
  than the ordering deviation the claim exists to fix. The bridge is pre-release and
  its `$MQTT_*` schemas carry no compatibility contract, so the remedy is to
  recreate the development store.
- **Mixed-version operation.** An older broker ignores the `Teardown` marker (it
  would overwrite it) and uses the old names, so both halves hold only once every
  broker in the cluster understands them — the same rule as the shared-subscription
  affinity protocol above.

### Inbound QoS 2 PUBREL is at-least-once across the unresolved move interval

The destructive move window that used to lose messages here is closed. The QoS 2
hold record on `$MQTT_qos2in` is a two-state machine
(`internal/mqtt/qos2_move.go`): a create-only CAS writes `held`, PUBREL delivers
the message onward **first**, and only then does a CAS at the held record's exact
sequence replace it with a terminal `delivered` tombstone. A later PUBREL that
finds the tombstone answers `0x00` without re-delivering.

The guarantee, stated exactly:

> No silent loss on any PUBREL path. Once the terminal state commits, suppression
> of re-delivery is indefinite and independent of the deduplication window, the
> broker's lifetime, and the nats-server's. Across the *unresolved* interval
> between the onward store and that commit, the durable (QoS 1/2 subscriber) path
> is exactly-once only while deduplication survives; otherwise it is
> at-least-once. The live core-NATS fan-out is at-least-once in that same interval
> and cannot be made otherwise.

This is **not** "exactly-once across a nats-server restart", and should not be
described that way.

The deviations that remain:

- **At-least-once live fan-out.** A failure between the core-NATS fan-out and the
  terminal commit re-runs the fan-out on the PUBREL retry, so QoS 0 MQTT
  subscribers, plain NATS subscribers and the retained store may see the message
  twice. "I fanned out" cannot be committed atomically with the fan-out; the
  window is one JetStream round trip wide and cannot be closed.
- **Best-effort suppression of the duplicate stored copy in that same window.**
  Each PUBREL attempt of one generation stores with the same `Nats-Msg-Id`
  (derived from the record's immutable `Nmqtt-Q2Gen`), so JetStream deduplicates
  the retry. That window is authoritative across a broker restart and a
  stream-leader failover, but not across a nats-server process restart in which
  the first copy had already been consumed and acked — `$MQTT_msgs` is
  interest-retention, so the deduplication rebuild cannot see it. QoS 1/2
  subscribers may then receive the message twice.
- **A delivered exchange whose tombstone was already reaped answers a later PUBREL
  `0x92`** rather than `0x00`. Absence is indistinguishable from never-existed,
  and Spec5 sanctions `0x92` during recovery.
- **A release that has already won its claim may still fan out after the discard
  completed.** A PUBREL now takes a durable **release claim** before delivering
  anything (`internal/mqtt/qos2_release_claim.go`): a create-only CAS on a sibling
  of the hold subject, which a session discard competes for and which exactly one
  of them can win. So a discard that observes no claim can never be followed by a
  delivery of that message, and the finalizer can no longer read a purge's absence
  as proof the exchange completed. What the claim does not order is the wire: a
  releaser that claimed before the discard observed the register is committed, and
  its live core-NATS fan-out may reach subscribers after the discard released the
  session record and its CONNACK reported Session Present false. The delivery
  *decision* is ordered against the discard; the delivery is not. A discard that
  observes a claim waits briefly for the exchange to terminate, and on the same
  broker an in-process fence closes the window outright — but a broker that dies
  mid-release cannot be waited out.
- **A discarded session leaves one small claim record per inbound QoS 2 exchange
  that was in flight.** The `discarded` marker is the only durable proof that a
  late releaser must lose, so nothing may reap it at teardown time — "no releaser
  is still in flight" is not provable. It is inert (roughly 150 bytes, on a subject
  scoped to the retired incarnation). It is retired by the periodic orphan sweep
  once it has been **continuously observed as an orphan**, unchanged, for a
  deliberately long grace (one hour, against five minutes for construction-window
  state) and only **after** the hold record it fences has been destroyed and
  confirmed gone. The grace is a stable-observation span on the sweeping broker's
  monotonic clock, not an age: no two JetStream timestamps are from one clock, so
  none is ever subtracted. This is explicit
  reconciliation, not stream expiry: `$MQTT_qos2in` continues to reject any
  `MaxAge > 0`, because age eviction cannot honour that ordering. A session with no
  exchanges in flight leaves nothing.
- **After an aged `discarded` marker is retired, a releaser that preloaded the hold
  answers `0x00` where it would previously have answered `0x92`.** With the marker
  intact such a releaser observes it and reports the identifier as gone; once the
  sweep has destroyed the hold and removed the marker, it wins a fresh claim,
  re-reads the hold as absent, and reports success. Nothing is delivered and
  nothing is lost either way — only the reason code is more optimistic than the
  marker-backed one.
- **A `releasing` claim whose broker died is retired the same way**, hold first.
  This does not prove a paused process can never resume; it preserves the ordering
  that already governs one. A releaser that has not passed its authoritative
  re-read delivers nothing, and one already beyond it is inside the post-claim
  delivery-ordering residual above. Age cleanup is not exactly-once coordination.
- **A superseded PUBREL is answered `0x92` after its message was in fact
  delivered.** When a newer session generation has taken the same
  `(client, packet identifier)` subject, the finalizer abandons without mutating
  anything and reports the identifier as gone. No loss, and the newer
  generation's payload is untouched.

What is handled rather than deviating: a hold lookup that fails for any reason
other than "not found", an unresolved release claim or an unresolved proof of the
hold record, a failed onward store, and an unconfirmed terminal commit all
suppress the PUBCOMP and close the connection instead of reporting a result, so
the client retries against intact session state. A record that cannot be
identified or cannot be parsed, one whose Message Expiry Interval elapsed while it
was held, and one whose release a discard won, are each **released with a
confirmed delete before** the client is answered — an unconfirmed cleanup
withholds the PUBCOMP, because a PUBCOMP frees the packet identifier and the
client's next PUBLISH on it would otherwise be matched against the surviving
record.

A releaser that stalls while a peer completes the same exchange delivers nothing:
after taking its claim it re-reads the hold record and requires both the sequence
and the generation it originally loaded, so an exchange already committed and
reaped is answered `0x00` from the peer's work rather than fanned out a second
time. That check is generation-qualified in every accepting case, including the
tombstone — a later exchange's tombstone on the same packet identifier is
**not** proof that this one completed.

Operational note: `$MQTT_qos2in` requires `DiscardOld` and `MaxMsgsPer: 1` (plus
the write, CAS, subject, acknowledgement, retention, deletion and purge semantics
the protocol depends on). A non-conforming stream fails broker startup with
recreate guidance rather than being mutated in place. Upgrading a pre-existing
development store therefore means stopping **all** brokers for the account and
recreating the stream; it is not a rolling upgrade.

### Broker-owned stream prerequisites: startup refusal, a snapshot, and dropped fallbacks

Every broker-owned `$MQTT_*` stream is now validated at account-manager
initialization against the list its own protocol derives (see "Stream
prerequisites" in `docs/ARCHITECTURE.md`). This extends the startup-refusal
surface that previously covered five streams to all nine, with the same
consequences the `$MQTT_qos2in` precedent established:

- **A nonconforming stream fails broker startup**, with an error naming the
  stream, the offending setting, and the recovery. Nothing is repaired or
  updated in place.
- **A nonconforming stream splits a mixed fleet.** An older broker keeps serving
  the account while an upgraded broker refuses initialization, surfacing as
  CONNECT failures on the new broker only. The recovery is **stop-all**: stop
  every broker for the account, recreate or fix the named stream per the
  error's guidance, then restart. This is not a rolling upgrade. A *conforming*
  deployment restarts and rolls exactly as before — default broker-created
  streams are conforming, and so are supported administrative variations such
  as a source on `$MQTT_rmsgs`.
- **Startup validation is a snapshot.** A stream config mutated *after*
  initialization is not re-checked until the next process start or manager
  recreation. Runtime guards exist exactly where the code already has them: the
  UNSUBSCRIBE-drain refusal and the shared-subscription guard for a
  stream-forced `ConsumerLimits.InactiveThreshold` on `$MQTT_msgs`, and
  JetStream's own consumer-create rejection for a `MaxAckPending` limit below
  the broker's cap. A persisted "validated" flag was considered and rejected:
  it goes stale against exactly the mutations it claims to cover.

Two pre-release fallbacks were **removed** rather than validated around, under
the compatibility boundary (no released contract exists for either):

- **The `AllowMsgTTL` degraded mode is gone.** Per-message TTL support on
  `$MQTT_msgs` and `$MQTT_rmsgs` is a startup prerequisite on every broker that
  accepts MQTT 5.0 — the default (MQTT 5.0 Message Expiry of stored and
  retained messages); a stream that refuses it fails initialization instead of
  silently not reaping expired messages. A `max_protocol_version: 4` broker
  never emits `Nats-TTL` (Message Expiry is an MQTT 5.0 property), so it
  accepts a TTL-less pre-existing stream; raising the protocol cap later makes
  the next initialization refuse that stream, fail-closed. The v4 acceptance
  presumes the store's history matches the configuration: within this build no
  sequence of normal operation can pair a TTL-less stream with persisted
  expiring MQTT 5 state (the broker creates its streams TTL-enabled, and a
  v5-accepting broker fails closed on a TTL-less stream before it can accept a
  single v5 CONNECT), but administrative stream recreation under live state or
  a post-start config/stream mutation can — a persisted expiring v5 Will then
  stalls with logged store rejections and lease retries (never silently lost)
  until the stream is fixed. Recreate per the compatibility boundary. The
  support floor is nats-server 2.14, which always understands `AllowMsgTTL`.
- **The two inherited upstream migrations are gone.** Pre-2.10 nats-server
  per-client `$MQTT_sess_<hash>` session streams and pre-2.10.10 single-subject
  retained messages are no longer transferred at startup — and they are no
  longer *detected* either: this broker never wrote those formats (only
  nats-server 2.9-era upstream MQTT did), so a store containing them starts
  with a fresh, empty `$MQTT_sess` as the authority, the legacy streams are
  ignored where they stand, and the orphan reconciliation sweeps — which
  reconcile against that authority — may remove consumers that referenced the
  ignored state. Pointing this build at such a store without recreating it
  first is unsupported; recreate the development store per the compatibility
  boundary. The retained-messages setup no longer rewrites `Subjects` or
  `MaxMsgsPer` in place either — `$MQTT_rmsgs` is create-then-validate like
  every other broker-owned stream.
