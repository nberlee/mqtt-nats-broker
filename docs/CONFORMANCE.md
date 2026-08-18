# Conformance status

The broker is tested against the conformance suites named in the project plan.
Results below are from running each suite against the running broker
(`cmd/mqtt-nats-bridge`) backed by a JetStream-enabled NATS server.

**No NEW residual may be conditioned on a rollout.** Every broker in existence is built
from this tree — see "Compatibility boundary" in `docs/ARCHITECTURE.md`, which is
normative — so "protection is per broker until the fleet upgrades" is not a real
qualification, and writing one invites a future change to preserve behavior for a broker
that will never run. State what a broker running this code does.

The rule applies throughout this document: every residual below states what a broker
running this code does, and a limit that is real is stated as the scope of the
mechanism that enforces it, not as a property of a fleet's version mix.

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
  funnel for every session-discard path, now also destroys the discarded
  session's QoS2 hold entries. (It no longer does so with a hash-prefix
  `STREAM.PURGE`, as this bullet originally described: it enumerates the retiring
  incarnation's generation-scoped hold subjects and arbitrates each one, purging
  only where it wins the release claim. See "Inbound QoS 2 PUBREL is at-least-once
  across the unresolved move interval".) Regression test:
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

**Scope.** Affinity is enforced by the brokers that consult `$MQTT_qos2assign`, which
is every broker serving the account — they all run this tree (see the Compatibility
boundary). It is stated as scope rather than as a rollout condition: a broker that did
not consult the claims could redistribute a started delivery another broker had
claimed, and there is no runtime capability gate that would stop it. That gate is
noted as a possible follow-up, not as a mixed-fleet accommodation.

**A record whose shared QoS 2 membership carries no affine generation is refused,
and nothing reclaims it.** No writer on this tree can produce that pairing, so it
can only have been written by a pre-release development broker. The same applies to
a persisted shared QoS byte outside 0..2, which the resume would otherwise clamp
into a live QoS 2 membership. Both are refused at
CONNECT, in the startup expiry sweep, at a fired expiry timer, and by the sweep's
shared validator (see "Session-affine shared-subscription QoS 2" in
`docs/ARCHITECTURE.md`). The residual is what the refusal costs, and it is wider
than that one session:

- The record is never resumed, never rewritten, never claimed and never deleted, so
  it and any consumers it names persist. Expiring it would destroy the state an
  assignment owed to a started delivery is arbitrated from, so that is the stated
  recovery rather than an oversight.
- **The affine half of the orphan QoS 2 sweep is suspended account-wide while it is
  present, and only that half.** The record carries qos-2 memberships while naming no
  affine generation, so it contributes no affine reference although the session it
  describes may own an affine consumer and outstanding claims; nothing can then prove
  which affine resources are still referenced, and affine claim reconciliation and
  affine consumer deletion are skipped for every session in the account. Since
  nothing removes the record, that is permanent, and it is logged as a warning naming
  the recovery on every pass. **Inbound QoS 2 holds, outbound in-flight registers and
  PUBREL consumers keep being reclaimed**: the record's identity and generation are
  sound, so the references it contributes to those classes are complete. A record
  whose *identity* cannot be established — undecodable, unattributable, or carrying a
  malformed `Gen` — is the stronger case and still aborts the whole pass. Either way
  the recovery is the same: stop the account's brokers and recreate the bridge-owned
  `$MQTT_*` streams.

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
- **Scope.** A drain survives because the brokers that read the `Drains` record field
  exclude its durable from the orphan sweep's candidates and hand it to teardown as
  this session's own — and that is every broker serving the account, since they all run
  this tree (see the Compatibility boundary). It is scope rather than a rollout
  condition: a broker that did not read `Drains` would let its orphan sweep delete a
  drained durable and lose its started messages, and there is no runtime capability
  gate that would stop it. Same class as the affine scope above.
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
  held. FIVE of the seven call sites are REFUSAL-AWARE, each acting on the sentinel
  rather than destroying, and what each then does differs: the deferred discharge re-parks
  its consumed action (pre-existing); CONNECT's clean start rejects without evicting; the
  clean close and session expiry park the discard and restore residency only when the
  shared helper ACCEPTS, failing closed to the historical evict-and-log when it declines;
  and the CONNECT rollback's zero-lifetime branch parks it on a session that never left
  the maps, reporting `rollbackParked` and removing nothing, or takes its historical
  outcome — classify and remove — when the helper declines.
  The two remaining callers
  that clear a transient object built from a record they just read have an unreachability
  proof rather than a recovery, plus the same fail-closed refusal if that proof were ever
  wrong; and the parking helper itself DECLINES for an already-parked action that is not
  the discard-only shape (below). What remains is that the parked discard exists only in
  session memory, so there are exactly THREE ways to lose it, and every statement below is
  conditional on all three: a crash before the discharge; a remote persist, whose
  classifying eviction worker evicts the re-added object — correctly, since a record
  another broker owns is not this one's to delete, but the obligation goes with it (the
  eviction is asynchronous now, defers to a held ownership gate or record fence, and can
  be refused outright, which changes when the obligation is lost, not whether); and the
  helper DECLINING, which needs neither of the other two and whose reachability is
  unknown. Absent all three, the next successful local classification retries the discard
  and completes it. A remote persist has two orderings and neither is a guarantee:
  arriving after the re-add it evicts, and arriving in the window between the caller's
  `removeSession` and the re-add it takes the no-local-session branch and starts a
  reconciliation worker that then finds the object resident and deliberately does nothing
  — so convergence there is eventual, through the uncertainty classification calling the
  object foreign, not immediate.
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
- **The protection is PER SESSION, not per account.** It adds no stream, subject, record
  field or cross-broker message: the parked obligation is memory-only, so there is
  nothing another broker can adopt and a takeover simply evicts it. The invariant
  therefore holds for a session while the broker that owns it holds it — which is the
  whole account, because every broker runs this tree (see the Compatibility boundary).
  Stated as scope, not as a rollout condition.
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
- **Scope.** As with the affine protocol: identifier continuity is enforced by the
  brokers that read and sweep `$MQTT_inflight`, which is every broker serving the
  account — they all run this tree (see the Compatibility boundary). A broker that did
  not read the stream would re-offer a started delivery under a fresh identifier and
  its orphan sweep would leave the registers behind, and there is no runtime capability
  gate that would stop it.

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
- **What the shared QoS 2 affine provisioning DOES leave durable is the path itself, and
  the settlement removes it when no membership owns it.** The bytes made durable name
  `Q2Aff`/`Q2AffGen` — in the
  fresh-SUBSCRIBE shape for a subscription that never became durable, and in the
  pre-affine RESUME shape for shared memberships the restored record already carried and
  the settlement preserves — and no orphan sweep will touch a consumer the record
  references. A generation MINTED by that save therefore registers a landed continuation
  that unwinds the path under the resolver's token — unless a real membership owns it by
  then, or the generation still owes a STARTED DELIVERY, which a failed packet may not
  discard because `Discarded` means the owning session ended and this one is still live —
  in this order: confirm the record that no longer names the path, discard the
  generation's assignments, release the delivery subscription and the MaxAckPending
  charge, and only then delete the consumer — a delete whose own failure is logged and
  leaves an unreferenced durable for the orphan sweep. A generation the save merely
  re-established is deliberately excluded: it may owe assignments to a delivery already
  started to it. If the assignment scan FAILS — it is account-wide and fails closed on any
  claim it cannot read or attribute — the subscription and the charge are still released,
  because once the record stops naming the path nothing else can reach them. The discard
  is a STREAMING one, so its progress is PARTIAL rather than none: the generation's claims
  the scan had already reached are discarded, and a claim that was already proven on both
  sides may also have been reaped, while the malformed record aborts the remainder. Those
  remaining assignments and the consumer stay pending, and they are not merely deferred to
  the orphan sweep: the same unreadable claim aborts that sweep's affine inventory too,
  deliberately, since an incomplete inventory cannot authorize any consumer delete. Until
  that record is repaired or removed, one malformed claim therefore wedges affine CONSUMER
  reclamation account-wide, leaving an unreferenced consumer on `$MQTT_out` for as long as
  it persists. The wedge is confined to that class: the sweep's other phases —
  including PUBREL durable reclamation, which needs no assignment inventory — keep
  running. The RESIDUAL is the crash window between the ambiguous
  save and its settlement. After a restart the record names the path and the resume
  re-establishes it — without recharging, and without reclaiming it. From there session
  TEARDOWN is the only thing that removes it: a later shared QoS 2 SUBSCRIBE reuses the
  same generation and consumer, which ends the path's UNUSED status but deletes nothing
  and refunds nothing. So the durable consumer on `$MQTT_out` persists until teardown. Its
  MaxAckPending charge is process-local and lasts the life of the provisioning process —
  the broker's, for a never-expiring session — and a restart drops that charge with the
  process while leaving the durable behind. The reclaim also adds ONE
  more arbitrating CAS site — plus whatever re-attempts and rejection probes its own
  failures invoke — to the exposure of the same-base leader-transition overwrite below;
  the exception's class and window are unchanged, its frequency is not.
- **The packet's two unwinds discard this generation's assignments BEFORE the CAS that
  arbitrates whether they still own them**, so a cleanup save lost to a remote winner that
  PRESERVED the generation leaves that winner's started QoS 2 delivery discarded —
  terminated for a session that is still live, which is the [MQTT-4.8.2-6] outcome applied
  to the wrong owner. `restoreQ2AffineUnwind` cannot undo it: the discard is monotonic by
  design. The settlement reclaim does NOT have this exposure — it prepares its unwind with
  `disposeNow` false and discards only once the cleanup record is confirmed — but the
  defect CLASS predates it and remains in `rollbackSubscribePacket` and the packet's
  unused-path unwind, both filed in `docs/ROADMAP.md`. Session teardown never had it:
  `claimTeardown` wins a durable ownership marker first, and only a WON claim discards.
  Disposing late is recoverable rather than stalling, which is what makes the reclaim's
  order safe: `materializeQ2AffOnBehalf` discards, source-acks and reaps a claim whose
  record no longer names its generation.
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
- **CLOSED by F-029: the create is conditional, so base 0 is no longer excluded.** A
  first-ever record write now carries the expected-last-subject-sequence header at 0,
  where it means "the subject must currently be empty". An ambiguous create therefore
  flows through the ordinary arms and is settled by RE-ATTEMPTING it, exactly as any
  other base is. What remains at base 0 is not specific to it: an absent record whose
  re-attempt is refused 10071 is still classified foreign fail-safe, for the reason
  residual 7 states.
- **A record found BELOW the attempt's base is classified foreign fail-safe**, not proven:
  sequences are monotonic within a stream incarnation, so that shape implies a reset or
  re-provisioned store. Nothing is adopted and nothing is written there.
- **A FOREIGN unconditional write can supersede this broker's conditional write, and
  that is not fixed here.** The route is an UNCONDITIONAL foreign write, which carries
  no expectation for any check to compare (a competing CONDITIONAL write off our base is
  excluded — see the same-base boundary below). **The direction matters, because the
  reverse is not reachable.** If the unconditional write is admitted first it is recorded
  in the stream's in-flight state, so our later expected-sequence re-attempt is REFUSED;
  and if it has already applied, the moved subject sequence refuses the re-attempt too.
  What is reachable is the other order: our re-attempt is admitted, and the unconditional
  write lands afterwards and overwrites it — the record then holds a body this broker
  never wrote, while the local session believes its own write settled. This is the
  pinned per-subject-CAS
  linearization contract failing (`docs/ARCHITECTURE.md`, "JetStream behaviors this design
  depends on"), and it fails identically for EVERY arbitrating CAS this broker makes: the
  definite-rejection probe's re-attempt of current memory, the repair retry's exact-byte
  re-send, and any ordinary save. What this change contributes is FREQUENCY, not the
  window — the resolver now writes where it previously only read. Closing the window is out
  of reach (production NATS is external, and the only apply-time-rechecked precondition
  compares a stream-wide value), so what `$MQTT_sess` carries instead is a **parent
  pointer** that makes the outcome recognisable. See the next section for exactly how much
  that buys.
- **This broker's original and its re-attempt cannot both commit, and the design no
  longer rests on tolerating it if they did.** Both are bridge-authored conditional
  writes off one base, so the same-base boundary below excludes the second. The
  tolerance described here is RETAINED as ordinary defensive depth, and it is NOT
  coverage of the unconditional route: an external replay supplies no second
  acknowledgement for this broker to adopt, so the reasoning below does not
  transfer to it. Both landings would carry IDENTICAL bytes, log order makes the
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
  **10071** is classified foreign, and that is the residual: an unconditional foreign
  write carries no expectation for any check to compare, so a body that lands on the
  subject can still be read as another owner's. (For a competing CONDITIONAL write the
  route is closed — see the same-base boundary below.) Narrowed from "absence is a
  verdict", not closed.

### A forked session record is RECOGNISED, and its recoverable grant REPAIRED

`$MQTT_sess` records carry `WID`/`PWID`, a parent pointer, so a survivor that provably
**CLAIMS the parent** of the record this broker was acknowledged for stops being
reported as a takeover: the client is told **0x89 "server busy, try again later"**
instead of 0x8E, which invites the reconnect 0x8E forbids. Five classified paths consult
it — the definite rejection probe (bound and unbound), the ambiguity resolver, the two
CONNECT-time saves, and the persist observation's eviction worker — plus the CONNECT
rollback readback. See `docs/ARCHITECTURE.md`, "Recognising a forked session record".

What it does NOT do:

- **The persist-observation route now classifies, asynchronously, and that leaves its own
  residuals.** The eviction runs on a per-client worker that loads and classifies the
  surviving record before acting; on a body claiming our own parent it answers 0x89
  and leaves the
  displaced incarnation's delayed-Will outbox records pending. What that does not close:
  - a **lost observation is no longer lost**, and neither is a refused eviction: the
    observation channel is the session-record watch, and the committed-read backstop
    on the periodic pass re-reads every resident session's record and re-fires any
    trigger no decision consumed (`evictDecided`). What that costs is LATENCY —
    convergence is bounded by the next backstop pass that reaches the session, not by
    a fixed interval, because the pass is serial and does blocking JetStream work. See
    `docs/ARCHITECTURE.md`, "The session-record watch";
  - the watch itself can still **duplicate or drop** deliveries (an ephemeral
    `AckNone` push consumer, no flow control, delivery state not quorum-replicated);
    duplicates are inert (max-watermarks plus coalescing) and drops are the backstop's
    business. Nothing recreates a deleted or inactivity-reaped watch consumer under a
    live manager: the watch stays absent and the backstop alone converges until the
    next manager start;
  - the MOVED-record and lost-observation subcase is closed outright, not "once a
    rollout completes". Every broker is built from this tree (see the Compatibility
    boundary), so there is no older path to be asymmetric with. It does **not** close
    F-036, which stays open for the bound deleted-record case below;
  - a record that was **deleted** rather than moved is handed to the ownership marker,
    which converges an UNBOUND session; its bound outcome is **0x8E** when it converges
    at all (see the next bullet, which is why F-036 stays open). Absence is a real
    ownership fact (only a
    party that claimed the record at our sequence can remove it), but for an
    expiry-driven deletion the accompanying Reason String — "session taken over by a
    new connection" — is imprecise: no new connection need exist. The disposal is
    right either way and the client's reconnect correctly gets a fresh session; a
    gone-specific vocabulary value is filed as its own roadmap bullet;
  - **the deleted-record shape does NOT converge a bound session that never sends
    another packet.** Unlike the moved-record shape — where the eviction worker
    disposes a bound session itself — arming the marker hands the case to the record
    rejection settlement, whose unbound half refuses while a client is attached and
    whose bound half runs pre-dispatch on that connection's own readLoop. A
    conformant client sends a PINGREQ within its Keep Alive and therefore converges
    within about 1.5 intervals, but a client that negotiated **Keep Alive 0** and then
    goes silent has no such bound, so it can stay resident indefinitely after a lost
    teardown observation. This is the packet-driven property the bound settlement has
    always had (`docs/ARCHITECTURE.md`, "A definite record-CAS rejection is
    outstanding work"), now reached by one more shape; closing it needs a periodic
    bound-session disposal that does not depend on client traffic, which is filed as
    its own roadmap bullet rather than claimed here;
  - a **serving window**: between the observation and the worker's decision the
    displaced client keeps being served for the worker's latency (a record read plus
    bounded retries). Any record write it triggers is CAS-rejected; deliveries in the
    window are the same exposure a lost notification always had;
  - a **lost newer observation can leave an older verdict's vocabulary to apply** — the
    worker decides from knowledge at least as new as every observation actually
    delivered before its decision, never newer;
  - a **clean-start** fork sibling CONCEDES here, and the reason is the write-lineage
    audit rather than the generation. The classifier used to concede on the re-minted
    generation before it ever compared parents; it no longer consults the generation at
    all, and briefly `clear()` carried the claimed record's identity across the re-mint so
    the sibling arrived PARENTED. That inheritance, and the three others like it, were
    deleted: a create is parentless, so a clean-start sibling and a parentless
    create pair both concede at arm 2's `ackPWID` conjunct. See R-create below for
    the direction and the evidence. The one-hop limit applies as everywhere — including
    through an UNRESOLVED local attempt: a save whose store landed ambiguously leaves
    the acknowledged pair stale behind a free fence, so a remote sibling of that
    unadopted attempt concedes to 0x8E (and the takeover-path Will cancel), exactly as
    before this change, while a sibling of the acknowledged record itself still
    classifies forked; a stale pair can only narrow the verdict, never manufacture a
    false forked one;
  - the **timer-cancel** consequences are unchanged in shape: both verdicts cancel the
    session-expiry timer (the record is owned remotely either way), which still bounds
    what the rollback path's preserved timer is worth;
  - a **bound connection's never-enqueued delayed Will is still lost on eviction**, on
    both verdicts: eviction detaches `sess.c`, the close's teardown gate then fails, and
    a delayed Will is never enqueued — the same shape as the foreign disposition's filed
    behavior. What this item narrows is only the destruction of already-durable outbox
    state;
  - an **interrupted takeover-verdict cancel scan** leaves a durable partial state —
    earlier records cancelled, later ones pending, no resume marker (pre-existing);
  - on the forked verdict the preserved outbox record can **still be cancelled at
    `FireAt`** by the lineage-blind watermark/session-record fences (filed separately);
  - **shutdown-time observations are dropped** once the dispatch gate closes or the
    watermark map is nilled;
  - **warm-up**: a record left in a developer's JetStream store before the recognition
    work carries no lineage; the first save over it stamps `{WID, PWID: ""}`, which the
    classifier refuses, so only the SECOND save yields a classifiable acknowledged pair.
    This is a property of the stored record, not of the writer — every broker runs this
    tree — and the same two-save warm-up applies after any state reset.
- **The recoverable subset of the erased grant IS repaired**, and the rest is not. What
  the reconnect re-asserts is the DELTA between the record this broker was acknowledged
  for and its parent: plain membership with its v5 options, both consumer identities of a
  multi-level filter, the drains an acknowledged UNSUBSCRIBE staged, the
  cancelled-register verdicts, and the PUBREL consumer where the survivor has none. The
  four recorded rejections of a repair are answered rather than waived: it is
  generation-anchored, so a capsule from an incarnation a teardown retired can never
  match; nothing replays bytes, because the payload is a semantic delta between two
  record-sourced snapshots; there is no worker and no repair write, because the only
  applier is the client's own CONNECT on its readLoop holding `sessLocked` and the accept
  transaction; and the write is the accept-save CAS itself, inheriting its conditional
  arbitration, rejection classification and re-drive. See `docs/ARCHITECTURE.md`,
  "Repairing a recognised fork". What it does NOT repair:
  - only a fork of a record **this broker authored**, and never the first record after a
    teardown re-mint: a restored record has no known parent content here, so no delta is
    computable and nothing is minted. A generationless or parentless record is repaired by
    NOTHING;
  - only on **this broker** and only while the survivor it conceded to is still the
    current record in the same incarnation — the capsule is memory, so a restart forfeits
    the repair, and any write since the verdict makes the anchor refuse permanently;
  - only where the survivor's **recorded grant EQUALS the retained baseline** for that
    filter. A filter whose recorded content differs, and two drains for one sid whose
    persisted content differs, are left to the survivor and never repaired: the record that
    is actually durable wins. Equality is not evidence that the survivor's author decided
    nothing about the filter — only that it recorded nothing this broker can see, which is
    the repair-authorization residual stated below. The non-repair is counted in the merge's
    log line, never guessed at;
  - **one losing branch, not the union of all recognised losses**: the first reconnect
    whose accept CAS lands advances the record identity and invalidates every other
    broker's capsule;
  - reaching the **cardinality bound** (256 capsules per account manager) forfeits the
    oldest client's otherwise repairable state. Two ordinary outcomes leave a capsule
    behind — a client that never reconnects here, and one whose reconnect merged, because
    a merge deliberately does not consume it — so the bound is reached by correct entries,
    not only by stale ones;
  - **two drains for one sid whose persisted content differs are refused, so a
    per-sequence progression is not repaired.** The CLAIMS are safe either way: the base
    was already draining, a committed drain adds no fresh claims and completion only
    removes them, so the survivor still names everything it says is owed. What is not
    repaired is a fact ABOUT one of those claims — a PUBREL intent, a packet identifier, a
    store attempt or an in-flight register sequence — that our branch acknowledged and the
    survivor's did not. The visible consequence is the survivor's own: recovery treats a
    delivery whose intent it does not hold as still in the PUBLISH phase and may resend
    the payload, which is the state the survivor's branch would have reached with no fork
    at all. Refused rather than merged because nothing orders the two acknowledgements;
  - a session whose record exceeds **64 KiB** (`mqttGrantBodyMax`) retains no bytes and is
    therefore not repairable at all — and the same bound applied to the SURVIVOR refuses the
    whole merge for that pass, because a survivor whose grant cannot be materialised cannot
    show its conflicts and nothing may be re-asserted over it. The capsule outlives the
    refusal (a re-driven pass against a readable sibling can still merge) and dies through
    the ordinary anchor refusal once a landed save makes the record this broker's own. The
    repair holds two record bodies per resident session, and that is bounded rather than
    trusted: a very large subscription set
    forgoes the repair instead of doubling its footprint. The grant is materialised from
    those bytes only on the fork path, never on a save — an earlier revision built it on
    every save attempt under the session lock, which measured 2.5 ms and ~1 MB per attempt
    at 1000 filters;
  - a repaired **QoS 2** membership whose PUBREL provisioning then fails is
    accepted-but-dead: `processSubs` runs after the success CONNACK. The same shape as
    F-050's, not a new one;
  - **shared-subscription membership is excluded until F-050 closes.** The success CONNACK
    precedes shared restoration, so a re-asserted group membership would be durable while
    a per-filter restore failure leaves no join and no consumer. Repairing into that
    ordering would extend an open finding rather than respect it.
- **The verdict does not prove the survivor erased us.** A teardown claim is a full-body
  CAS off the same base, so a sibling can merely have become current after `clear()`
  deleted our record. What is established is only that the survivor is not demonstrably
  a successor of ours.
- **The withheld verdict can publish a Will a genuine takeover would have suppressed**, on
  the definite-rejection bound path: the foreign settlement calls
  `mqttDiscardWillOnTakeover` before disposing, while the 0x89 route runs ordinary close
  bookkeeping, which marks the Will owed and publishes it. This is what
  `errMQTTSessionOwnershipUnsettled` has always done — it is the "nothing arbitrated this"
  path — but the forked verdict reaches it more often.
- **The ambiguity path's treatment is unchanged in every other respect**, including that
  `disposeForeignStateLocked` consumes the parked deferred disconnect, which can drop the
  only copy of a Will and an original expiry deadline, and that retained `uncertainLanded`
  continuations are not cleared. Both are PRE-EXISTING behavior on the foreign path, not
  regressions from this item — the forked verdict reaches the disposition it would have
  reached anyway — and both are filed separately.
- **A preserved rollback timer can miss its record once, and the sweep is what catches
  it.** When the timer a forked rollback leaves armed fires, a surviving sibling that is
  still `DisconnectedAt == 0` makes `loadExpiringSessionRecord` report not-due, the one-shot
  entry is consumed, and nothing re-arms *at that moment*. It is not stranded: the periodic
  `sweepSessionExpiries` (15 minutes) stamps any finite-expiry record that claims a
  connected client with no resident session and arms a timer from it. The residual is the
  DELAY — up to one sweep interval plus the expiry — and the sweep's own origin gate, which
  in a cluster leaves such a record to the broker named in `Origin`, so a record written by
  a broker that is now down waits for it to return. Preserving the timer is nonetheless
  strictly better than the `rollbackSuperseded` behavior it replaces, which CANCELS it.
- **The bound 0x89 close leaves the session resident with its marker armed.** For a
  never-expiring session the object survives the close and the state terminates after one
  failed pre-CONNACK reconnect (whose accept save CASes against the forked record and is
  rejected, evicting the object), or sooner if the periodic unbound settlement runs first.
  That reconnect is silent: `c.mqtt.sess` is not assigned until after the accept save.
- **The one-hop limit is reached by ordinary operation**, not only by exotic histories.
  Once a landed save advances the acknowledged pair from `{a,p}` to `{c,a}`, a delayed
  sibling `{b,p}` of the PREVIOUS acknowledged record classifies as unrelated and concedes
  — which is precisely the fork this mechanism is about, missed because the base moved
  first.
- **Detection is LIMITED by an unstamped stored record, not disabled by it.** A save over
  a record that carries no lineage still writes a fresh `WID`, so siblings formed from
  that stamped base are detectable. What concedes is a fork based on the unstamped record
  itself, until one save establishes a new base.
- **Takeover versus takeover at one base is arbitrated by the record CAS.** F-029 made
  every record write conditional, base 0 included, so two frontends writing off one base
  produce exactly one winner and a definitely-rejected loser that re-drives onto the
  winner's record — and the same-base boundary below makes that unqualified for two
  bridge-authored conditional writes rather than true only within a term. What is NOT
  recognised is the pair produced by a write no CAS can exclude — an unconditional
  foreign one — where both records exist and the loser's is
  erased: F-029b closed it by having a create
  inherit the identity of the record whose deletion emptied the subject, and the
  write-lineage audit deleted every such inheritance because each was reachable equally by
  a false fork. A clean-start side concedes for the same reason. A side whose observation
  was lost still decides nothing until the backstop reaches it.

  **A CREATE IS PARENTLESS, and the recognition it used to supply is GIVEN UP.** The
  write-lineage audit deleted every source a create drew a parent from: the
  generation-retirement watermark's `RWID`, an adopted marker's own `PWID`, `clear()`'s
  stamp of the record its claim held, and the `$MQTT_origin` session-interval register
  with the stream itself. The reason is stated below and is the whole finding of that
  audit — every route by which the inheritance produced a verdict was reachable equally
  by a FALSE fork, and there was no route on which it was reachable only by a true one.

  There are two failure DIRECTIONS. A **missed recognition** concedes to the takeover
  vocabulary and is exactly the behavior that preceded F-029b. A **false fork** reports
  the fork vocabulary — reason code 0x89, its Reason String, the log line and the close
  reason travel as one value — where the takeover vocabulary would have been reported,
  and PRESERVES a delayed Will the takeover path would have cancelled; that is the
  dangerous direction. The audit's result is that the create side now has only the first.

  - **R-create — the parentless create pair is not recognised.** Two records both carry
    no parent — so arm 2's `ackPWID != ""`
    conjunct concedes from either orientation and the erased side answers 0x8E about a
    takeover nothing arbitrated, cancelling its delayed Will on that path. This is the
    general form of what F-029b closed and this audit reopened, and it subsumes the
    former R0 and its sub-routes entirely: a record-absent create with no usable
    watermark, a genuine first-ever interval, a client whose retirement history was lost,
    and the transient watermark read all reach the same concession by the same conjunct,
    with no interval identity to diverge, replay or regress.

    Pinned in both orientations by
    `TestMQTTParentlessCreateConcedesFromBothOrientations` across all three production
    create paths, and end to end on the wire by
    `TestMQTTCreateForkE2EAnswers0x8EToTheDisplacedClient`.

    **Its cost is not vocabulary alone.** The conceding side reports 0x8E and cancels its
    delayed Will, where a recognised fork reports 0x89 and destroys no durable Will state.

    **The residual is conditioned on EVIDENCE, not on a rollout.** It stands unless a
    route is found that produces a true create-pair verdict without also producing the
    false ones below. Two refutations stay recorded so they are not re-proposed. A
    **reserved parent token** stamped on the record-absent arm was designed and REFUTED,
    because an absence is not a fact: `$MQTT_teardown` can be recreated alone, stamped
    with the current session epoch, and validate clean with no historical fences (see
    "Stream prerequisites"), and administrative deletion and partial restore do the same.
    Every other row of this design degrades to conceding when it cannot prove something;
    that token degraded to a false affirmative. **Minting a retirement watermark at a
    first create** is refused for its own reason: a watermark carrying a fresh generation
    would fence the session being created, and would replace the standing watermark under
    `mseq` ordering, unfencing a generation a real teardown retired.

  - **The FALSE-FORK routes the deletion closes**, recorded because each was a live
    defect and because they are the evidence for the paragraph above. Every one needed a
    create carrying an INHERITED identity that had outlived the interval it named, and
    each produced the dangerous direction:

    - a register whose sequences a restore or a live stream recreation replayed or
      regressed, handing two intervals one identity (the former R0c);
    - a create whose watermark read was separated from its admission by a complete remote
      lifecycle, so it stamped a retired interval's identity into the current one (R1);
    - a regressed `$MQTT_teardown` slot presenting an older interval's identity (R2),
      downstream of the stated decision-time-monotonicity residual;
    - a delayed same-base proposal outliving an interval (R3, below);
    - an out-of-band deletion of a live record (R4, below).

    The first three cease to exist with the inheritance and are not restated. R3 and R4
    are, because their routes survive parentlessness with a changed VERDICT rather than
    disappearing.

  - **R3 — a delayed same-base proposal that outlives an interval. CONDITIONAL: every
    consequence below holds IF this interleaving is reachable, and this item does not
    settle that — see the closing note of this bullet.** Suppose a child write admitted
    against record `P` applies after `P` was properly retired and deleted and a create
    `X` took its place. `X` no longer inherits `P`'s identity, so the lineage
    verdict is a CONCESSION rather than a fork — `X`'s broker reports 0x8E "session taken
    over" where it reported 0x89, and **performs the takeover-path Will cancellation the
    forked verdict used to skip**, destroying durable Will state on a takeover nothing
    demonstrated. That is the same direction as R-create and R4 and it is stated here
    explicitly, because the pre-audit text recorded the opposite consequence for this
    route. What does not go is the route or its other
    costs — all equally conditional on the supposition above, and all PRE-EXISTING
    rather than introduced here: the survivor wins the
    single slot either way, so `X`'s granted state is lost and unrecoverable; condemning
    the survivor by the retirement fence is destructive where it added state; the F-036
    stranded-session route applies; and a delayed STATE-CHANGING save can have its client
    acknowledged for membership the fence subsequently deletes — a conditional false
    acknowledgement that is a property of the delayed-proposal window and the fence.

    **What this item settles about R3, and what it deliberately does not.** The same-base
    boundary below is about two bridge-authored CONDITIONAL writes taken from ONE base.
    R3's interleaving is a different shape — it involves a DELETE and a create at base 0 —
    so the boundary neither states it nor closes it. The framing this route used to carry,
    that it is an instance of two same-base conditional writes both applying, is RETIRED,
    because that shape is excluded. **Whether R3's interleaving is reachable at all is NOT
    settled here**: an adversarial review argued it is excluded by proposal ordering and
    the in-process per-subject guard, and this item did not verify that against the pinned
    server. R3 therefore stands as previously documented, pending an analysis that owns
    it, and nothing in this change is evidence for or against it. Both writers reach
    whatever remains identically: the batch's apply-layer behaviour is measured
    (`test/depproof/README.md`, R-batch) and its refusals are the same code —
    `ProposeMulti` carries `proposeLocked`'s captured-term comparison verbatim, and a
    batch commit derives its expectation through the same
    `checkMsgHeadersPreClusteredProposal`.

  - **R4 — out-of-band record deletion.** `$MQTT_sess` must have `DenyDelete == false`
    because the broker's own deleter needs it, so an operator deleting a live record
    leaves the standing watermark untouched. The route is unchanged and its FALSE-FORK
    half is gone with the inheritance; what remains is the concession. Say the interval's
    create is `C{PWID: ""}` and an acknowledged save made it `S{PWID: C}`. An operator
    deletes `S`; the next create is `N{PWID: ""}`. Against a broker acknowledged for `S`
    that is a CONCESSION — `N` carries no parent, so nothing matches and the erased side
    answers 0x8E with the takeover-path Will cancellation. Against one still acknowledged
    for `C` it is equally a concession now, where the inherited identity used to make it
    a false fork. Eliminating either would need durable evidence of the record actually
    deleted rather than the last one retired, which no in-broker fence can supply for a
    deletion the broker did not perform.

  - **Not a recognition route, and it survives this deletion independently: the
    rollback's own teardown claim can be regressed by a write it cannot exclude.** The
    teardown claim is a full-body CAS at the candidate's sequence. Its RE-FOUNDED route is
    an UNCONDITIONAL foreign write, which carries no expectation for the check to compare;
    a competing CONDITIONAL successor off the same base is excluded by the same-base
    boundary below, so the cross-term framing this residual used to carry is retired.
    **Only ONE ordering is reachable, and the symmetry the cross-term framing carried does
    not survive with it.** The claim is CONDITIONAL, so it cannot be the one that lands
    last: if the unconditional write was admitted first it is in the stream's in-flight
    state and the claim is refused, and if it has already applied its new sequence refuses
    the claim too (the same admission rule stated for the re-attempt above). The reachable
    order is claim-FIRST, then the unconditional write, which evicts the marker and leaves
    the hollow survivor — the retirement is recorded, so the survivor is condemned by the
    retirement fence, and a client whose save succeeded can lose its resumable state. That
    arm alone is what the retirement watermark exists to protect against, and it is
    untouched by this correction; what goes is only the claim-LAST consequence, which
    would require an admission path bypassing both refusals, and none is documented.
    Parentless creates are not its justification and do not narrow it — the claim
    is a record write of its own, not a create — which is why it is stated here rather
    than carried out with the create-side routes. This is the SAME claim-apply window
    every other `clear()` caller carries — see "decision-time monotonicity" in
    `docs/ARCHITECTURE.md`, "The generation-retirement watermark". Narrowing it means
    durable evidence of the interval a claim overwrote, which is new durable state for
    all seven callers.

  **What the AUTHORED-RECORD fork retains, and why.** Two CAS writes off a base this
  broker actually held still classify as siblings, and `sess_fork_repair.go` still
  repairs the grant one erased.

  The reason previously stated for that retention was an evidence gap in the accepted
  CONNECT's writer, and **that gap is now closed**. Every one of its Raft-level facts
  holds — at the pinned v2.14.5 no Raft-level field preserves "these entries came from
  one `ProposeMulti`"; entries are pushed individually, popped as whatever prefix
  exists, regrouped by size thresholds, and each group rechecks leadership on its own —
  but the consequence drawn from them was false. Batch identity is carried in the stream
  entry encoding and enforced at apply, so a committed prefix applies **nothing**, and
  the accepted CONNECT's fact and record commit together or not at all. The R-batch row
  is measured (`test/depproof/README.md`), and the bridge's own use of the batch is
  asserted at the store boundary by `TestMQTTAcceptBatchReplicatedIndivisible`.

  **The same-base boundary (authoritative; other sections reference this one).** State it
  in three parts, because collapsing them is what produced two wrong conclusions in a row:

  1. **Closed.** Two BRIDGE-AUTHORED CONDITIONAL writes on `$MQTT_sess` taken from one
     base cannot both apply. Every bridge-authored production writer of the record
     subject sets `Nats-Expected-Last-Subject-Sequence` — the accept batch, the
     non-accept save, the teardown claim and the expiry sweep's stamping — and the
     admission trichotomy then closes it: in the same term the second is refused as
     in-process contention; if its expectation was read in a term that has ended it is
     R-old, closed by the captured-term comparison; if it is read and proposed in the new
     term, the leader admitting it must still expose the earlier applied state, which is
     R-new. F-030 established that trichotomy and Part A supplied the writer it was
     missing. This is why no statement here may still explain live behaviour by "two
     writes off one base can both apply".
  2. **Still open.** The same durable END STATE — a foreign body in the one slot, off a
     base this broker held — remains reachable, because an **unconditional** write is not
     a write the trichotomy speaks about at all. `test/depproof/README.md` scopes the
     proof to writes CARRYING a per-subject expectation; a headerless direct publish to
     `$MQTT.sess.<domainTk><cidHash>` carries none. Nothing validates record provenance
     at restore: `createOrRestoreSession` checks JSON, client ID, reserved membership,
     generation, the affine pair, teardown/retirement and overdue state, then adopts
     `WID`/`PWID` exactly as given. The reserved-namespace guards deny `$MQTT.>` to MQTT
     clients; they are not a guard against permitted NATS publication.
  3. **Untouched**, and the reasons differ per stream — do not collapse them.
     **Deletes** carry no per-subject expectation, so no admission closure speaks about
     them. **`$MQTT_qos2assign` and `$MQTT_inflight`** await their own
     consequence audits — F-033 and F-034 respectively — which are open. **`$MQTT_qos2in`'s
     ran (F-032, closed 2026-08-17)**: every write there carries its expectation, so it
     needed no lineage, and what it found instead was a hold purge carrying no condition at
     all — F-062 (closed 2026-08-18 by giving the purge a proven cutoff) and F-036, neither
     of which this admission closure speaks to.
     **`$MQTT_teardown`** is not one of those: it carries its own separately documented
     residuals (the retirement watermark and the single slot), stated where they arise.
     **`$MQTT_origin` is not in scope at all**: the register was deleted by the
     write-lineage audit and the stream is REFUSED at startup, so there is nothing about
     it left to retire. Nothing above retires a statement about any of these.

  **So the machinery is retained, and on part 2 rather than on the batch gap.** The
  classifier's predicate `found.PWID == ackPWID && found.WID != ackWID` establishes that
  the surviving body CLAIMS the same parent — never that it is provably descended from
  it, which unvalidated restore forbids anyone from concluding. That is still exactly the
  claim needed: a body claiming our parent does not establish a takeover, and reporting
  one as a takeover would cancel a delayed Will that is still owed
  (`sess_rejected_write.go`, foreign arm) and skip the repair capsule.

  **What the re-founded route costs the REPAIR, stated as a residual.** The repair's
  conflict predicate (`mqttSurvivorDecidedGrant`) re-asserts our delta for a filter only
  where the survivor's grant EQUALS the retained parent's — read as "the survivor made no
  decision about this filter". Under the route this audit retired (two legitimate brokers
  racing off one base) that inference was sound, because the survivor was another broker's
  record. Under the route that remains, the body is whatever an unconditional publisher
  wrote, and equality with the baseline is no longer evidence about intent. Three things
  bound it, and none of them makes it an invariant. The predicate is conservative in the
  only direction that matters: anything the survivor states DIFFERENTLY is left alone and
  the non-repair is counted and logged, so a deliberate divergent grant is never
  overwritten. The re-assertion is applied by the client's own CONNECT under the accept
  save's CAS, not by a background writer. And an actor able to publish an arbitrary body
  to `$MQTT.sess.<domainTk><cidHash>` can already set that session's state directly, so
  the repair grants it no capability it did not have. What is NOT claimed is that the
  survivor made no decision — only that it recorded none this broker can see. Repair
  authorization is therefore weaker than classifier retention, and deliberately so: the
  classifier's conclusion is negative (no takeover is established) and needs no provenance,
  while the repair acts, and acts on an inference from body equality.

  Two tests carry that, and they are not interchangeable.
  `TestMQTTAuthoredForkStillClassifies` pins the classifier's SEMANTICS; it constructs
  lineage values and calls the predicate, so it says nothing about live reachability.
  `TestMQTTForkedRecordE2EAnswers0x89AndPublishesTheWill` is the retention oracle: it
  lands a valid-shaped record with an unconditional store and proves the live connection
  reaches the classifier, is answered 0x89, and publishes its Will.
  `TestMQTTForkedRecordE2EConnectRebuildsFromRecord` proves the capsule restores real
  delivery. Together they are what stops every concession above from being satisfiable by
  a classifier that had simply been removed.

  Both remaining bounds are unchanged. **No false verdict can resurrect state**: a
  capsule minted by the persist-observation route can never be APPLIED to a create,
  because the anchor requires generation equality and a create carries a freshly minted
  generation. And the **surviving record is condemned by the retirement fence wherever it
  carries a generation the fence names** — R3 by construction, R4 only when the survivor
  happens to carry a retired generation, since both the CONNECT fence and immediate
  survivor condemnation test generation equality.

  What remains structurally excluded is narrower than "no missed detection", and after
  this audit it is narrower still: every pair of creates concedes, whether or not they
  belong to one interval, because neither carries a parent. What no interleaving can
  produce is a FALSE fork between two creates, which is the direction the deletion buys
  and the reason it was taken.
- **The other four arbitrated streams have no lineage at all** — `$MQTT_willq`,
  `$MQTT_qos2in` (both its hold subjects and their `.rel` siblings),
  `$MQTT_qos2assign` and `$MQTT_inflight` — so the same leader-transition overwrite is
  unrecognisable on each, with a different consequence per stream. One independently
  checkable roadmap bullet each, naming that consequence.
- **The Raft window itself is not reproduced by any test.** What the tests stage is the
  DURABLE OUTCOME — two records off one parent, the later surviving under
  `MaxMsgsPer: 1` — with an unconditional store. Not reproduced: pre-proposal
  admission against stale applied state, the cleared in-process map, two success
  PubAcks to distinct brokers, and a second election. The replicated-JetStream tests
  pin that the pair survives Raft, a broker handoff and a node restart, which is a
  different claim and is stated as one.

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
  6. **The protection is per OWNING broker** (see the last bullet): a session's bound
     split-brain is closed by whichever broker owns that session, and no broker acts on
     another's residents.
  7. **The AT-BASE false foreign verdict is CLOSED at every CAS base, base 0 included;
     one fail-safe residual remains.** The uncertainty resolver no longer clears an
     ambiguity on a plain load at the base sequence: it RE-ATTEMPTS the attempt's exact
     bytes, so a late-committing proposal loses that arbitration instead of being read
     later as another owner's record. The **unconditional create** used to be excluded
     from that — a first-ever write carried no CAS header, so re-sending it would have
     overwritten whatever was there rather than arbitrating. **F-029 made every create
     conditional**, so base 0 is settled the same way as any other, and two brokers
     creating one session from nothing are arbitrated rather than one erasing the other.
     That is now unqualified for two bridge-authored conditional writes — a create is
     conditional too, base 0 meaning *the subject must currently be empty* — see the
     same-base boundary below. What remains is a parentless record arriving by a route no
     CAS excludes (an unconditional write, or an out-of-band deletion then a create),
     which the loser cannot tell from an ordinary takeover; closing that needs a durable
     identity for the empty-subject interval.
     Still open, and NOT covered by the closure:
     - **an absent record whose re-attempt is refused 10071** (nothing in process for the
       subject). That refusal does not prove the ORIGINAL proposal is dead, and an
       unconditional foreign write is excluded by nothing, so a body landing on the
       subject afterwards is still classified foreign — evicting
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
      - **The notification write is still unbounded**, per (9) and (11), though the
        persist observation's eviction worker no longer holds any lock across it. The
        old overlap with a gate-held probe is gone — the worker OBSERVES the gate and
        defers to a holder — at the price that the deferral can lose the eviction
        (bounded retries, then a refused exit).
      - **In-process, so no wire, stream or record state is involved** (see (6)). Every
        broker is built from this tree (Compatibility boundary), so the claim is not
        qualified by a rollout.
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
      an in-process fence, and no wire, stream or record state is involved either way.
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
- **Protection is PER SESSION, not per account.** No stream, subject, record field,
  header or cross-broker message is added; the probe is an ordinary session save. A
  broker settles the rejections of the sessions IT owns, and the pre-dispatch gate
  protects those sessions — which is the whole account only because every broker runs
  this code (Compatibility boundary). The contender in process is another broker's
  ordinary write either way, which is why the probe retries a CAS instead of trusting a
  read.
- **The never-expire disconnect stamp has outlived its consumer.** It was a Will-fence
  fix: a never-expiring session's close recorded the disconnect time so a later write
  could not read as a live newer connection and cancel an owed delayed Will. That fence
  is GONE — delayed-Will suppression is now decided from the permanent acceptance facts
  and reads no session record at all — so for a never-expiring session the stamp is
  currently inert, every remaining `DisconnectedAt` reader being an expiry path that
  pre-filters on never-expire. The mechanism is retained rather than removed, and this
  entry says so plainly rather than restating a rationale that no longer holds; removing
  it touches the close paths, the rejection probe and waiter priority, and is its own
  change. The description below is of what the code still does. The stamp is taken
  inside a RECORD TRANSACTION when one can be had, which is what orders it: `sess.mu`
  alone leaves a body an off-readLoop writer had already marshalled with
  `DisconnectedAt = 0` free to
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

### Delayed-Will suppression: permanent acceptance facts, and what still escapes them

An accepted CONNECT records one permanent, dated acceptance fact on `$MQTT.accept.>`,
committed in ONE JetStream atomic batch with its session record on `$MQTT_sess`. A
claimant suppresses a delayed Will only for an acceptance of that EXACT client, in this
incarnation, strictly newer than the connection that created the Will, and dated BEFORE
the Will fell due. See `docs/ARCHITECTURE.md`. Its boundary and residuals are:

- **Two same-base claimant writes across a leader transition: RETIRED for a conforming
  `$MQTT_willq`.** Two residuals were stated here — claimant DIVERGENCE, where a reclaim
  overwrites a licensing decision, and terminal REGRESSION, where it overwrites a
  committed `done` — on the premise that the expected-sequence check is pre-proposal and
  the in-process map is cleared by a transition, so two bridge-authored conditional
  writes taken from one base could both apply. F-030 disproved that premise against the
  supported server.
- **What is refused.** A write conditioned on base `S` can only reach the stream
  after a committed `done` has replaced `S` by one of two routes. **R-old** — an
  operation whose captured proposing term outlives that term — is refused by Raft's
  comparison of the captured term against the current one (`nats-io/nats-server#8370`,
  in the 2.14.5 floor), with two discriminating mutations and a control run on the
  pre-fence generation. That route is closed. Seven further shapes are refuted in
  writing in `test/depproof/README.md`; post-start mirror/source drift is classified
  outside F-030 rather than left unnamed or falsely refuted.
  **R-new** — a newly elected leader admitting a CURRENT-term write — is refused by a
  readiness envelope and then by the per-subject expectation a serving leader
  reconstructs from its own applied state. The local fixtures and discriminating
  mutations exercise those boundaries; the pinned source identifies them; and a NATS
  maintainer independently confirmed that in-flight ownership and
  applied-before-serving guarantee correct header enforcement across elections
  ([`nats-server#8469`](https://github.com/nats-io/nats-server/issues/8469#issuecomment-5304075553)).
  Four repaired false-positive paths mean the harness remains a canary rather than the
  sole proof; the independent layers are why the route is closed.
- **Why the DIVERGENCE class closes with R-new.** Its ordering differs — the
  first write is a licensing decision rather than `done` — but the admission question is
  identical, and it reduces to the same inventory. For a second write taken from the same
  base to be admitted after the first: in the same term it is refused as in-process
  contention; if its expectation was read in a term that has ended it is R-old, which is
  closed; if it is read and proposed in the new term, the leader admitting it must still
  expose the earlier applied state, which is R-new. What the record CARRIES does not
  change the stream-level admission and sequence mechanisms that refuse it.
- **What the closure says nothing about.** `$MQTT_sess`, `$MQTT_qos2in`,
  `$MQTT_qos2assign` and `$MQTT_inflight`: the server mechanisms are stream-independent,
  but each of those findings owns its own consequence. **F-033 and F-034 stay open until
  each is audited on its own terms.** F-032 has since been audited and closed, producing
  F-062, which closed on 2026-08-18. It also does not claim the two backstops below
  the proved mechanisms — an entry admitted anyway is discarded by apply-time log order,
  and beneath that by the store's refusal of any sequence that is not the next one — but
  neither is mutation-proved and neither is part of any claim.
- **Post-start source configuration remains outside the closure.** `$MQTT_willq`
  validation rejects sources at manager initialization, but validation is a snapshot. An
  operator can add a source while an initialized manager keeps serving; its foreign
  writes carry no bridge CAS expectation and can replace the one-slot subject. The next
  manager initialization refuses that shape, but the live configuration-drift interval
  is not bounded by F-030's conditional-write contract.
- **A duplicate is still possible where no terminal evidence exists.** Everything above
  is about a COMMITTED `done` being taken back. It leaves the sanctioned duplicate below
  exactly where it was, and that is the honest boundary: the broker never discards
  completion evidence it holds, and it still republishes when it holds none.
- **The sanctioned at-least-once duplicate, stated by its real boundary.** That
  boundary is **external publication with no durable evidence of completion**. A crash
  is one way to reach it and not the definition, and there are TWO sanctioned triggers,
  each with its own test:

  1. **Lease expiry** with the publish externally done and `done` uncommitted. Recovery
     cannot tell a crash from a pause, a partition, or a live broker stalled inside its
     own process; all four look identical to it.
  2. **Enqueue ambiguity**, with no lease expiry involved at all: retry exhaustion
     leaves an UNCONFIRMED create, `publishWillFailSafe` publishes immediately, and a
     record that did land fires again later
     (`TestMQTTWillOutboxEnqueueExhaustionAfterLandedWriteDuplicates`).

  Republishing is the correct bias in both, because the alternatives are silence about a
  Will that may be owed, or durably cancelling one that may already have gone out.

  **A live broker reaches this without failing.** `mqttInitiateMsgDeliveryJSA` fans the
  Will out to subscribers first — irreversible, external — and then waits on
  `mqttSubsBarrier` before storing the QoS 1/2 copy; that wait is bounded by SHUTDOWN,
  not by the lease. The barrier is held for a whole `processSubs`, which creates a
  JetStream consumer per filter, so a CONNECT session restore with a few dozen
  subscriptions on a cluster timing requests out holds it past the 30s lease. A peer
  reclaims the still-`publishing` record, reuses the persisted decision and publishes
  again. **One stall yields two publications, not unboundedly many.** `mqttSubsBarrier`
  is per account manager, so the stalled broker's `processSubs` does not stall the peer:
  the peer's worker runs through, commits `done` and deletes the record. A third
  publication needs each successive claimant to ALSO outlive its own freshly minted
  lease or fail terminal reconciliation — possible, but a separate condition rather than
  something the first stall causes.
  Bounding the barrier is not the fix — the fan-out precedes it, so abandoning there
  would skip the store that protects the racing subscriber and trade a duplicate for a
  LOSS. `docs/ROADMAP.md` carries end-to-end phase-aware ownership as the real fix.

  The permanent acceptance facts did not introduce this: a slow barrier always let a
  peer reclaim and republish. What they changed is that the peer now reuses a persisted
  decision instead of rescanning, which makes an already-permitted trace DETERMINISTIC
  rather than adding a new one. **This is the only duplicate class left, now that the
  terminal regression above is retired**, and the line was always evidence rather than
  liveness: here `done` was never committed and the system genuinely lacks proof of
  completion, whereas the retired class had `done` COMMITTED and then taken back —
  evidence it already held, discarded.
- **Acceptance-fact storage is unbounded and client-driven.** One permanent record per
  ACCEPTED CONNECT, on a stream that cannot carry global limits (they would evict live
  session records) and whose `MaxMsgsPer: 1` does not apply because each fact has its
  own subject. Any credential permitted to CONNECT therefore grows the account's disk
  permanently through a reconnect loop, and nothing refuses or throttles it. Filed as a
  `docs/ROADMAP.md` §0.3 blocker with a candidate reclamation rule; stated here because
  the change ships with the exposure, and a disclosure is not a bound. The claimant's
  own walk is narrower than the history, but only within one incarnation: for a Will
  whose origin epoch is current it starts at `OriginSessionSeq + 1` and stops at the
  cutoff, so it reads only facts written after the connection that enqueued that Will.
  For a Will from ANOTHER incarnation the walk starts at the beginning of the stream,
  because that origin sequence names a store that no longer exists, so its scan depth
  is the whole current history.
- **A fact survives a CONNECT that never receives its CONNACK.** The batch commits
  before the CONNACK is written, and several exits between them refuse the connection.
  The acceptance is then durable and will suppress the prior Will even though the client
  never learned it was accepted — the same family the pre-existing pre-CONNACK
  disclosure covers, now reached through the fact rather than a watermark.
- **`At < FireAt` compares two brokers' clocks, in both directions.** A skewed
  accepting broker can date an acceptance before a Will that in truth fell due first
  (suppressing a Will that was owed), or after one that in truth preceded it (firing a
  Will a reconnect was entitled to suppress). No mechanism here bounds skew; the
  comparison is between the accepting broker's `At` and the enqueuing broker's `FireAt`.
- **An acceptance still UNCOMMITTED when the decision is taken cannot suppress, even
  though it qualifies.** Eligibility is dated by wall clock (`At < FireAt`) while
  evidence arrives in durable commit order, and no reader can see an uncommitted write.
  Worked interleaving, needing neither a leader transition nor clock skew: a Will falls
  due at `FireAt`; a CONNECT samples `At < FireAt` and begins its accept batch; the
  claimant claims the record, samples cutoff `U`, walks the client's facts through `U`,
  re-samples and finds no new fact for that filter, licenses `(publish, U)` and
  publishes; the accept batch commits afterwards at a sequence above `U`. The fact now
  satisfies every qualification term — exact client, current incarnation, dated before
  the Will fell due, above the origin sequence — and nothing will ever read it for this
  Will, because the persisted decision is deliberately never re-derived. The claimant
  CHASES the cutoff to shrink this to whatever commits after its final sample (see
  `docs/ARCHITECTURE.md`), which is a narrowing and not a fence.

  The remainder is irreducible under three premises this design holds: eligibility stays
  `At < FireAt`; a decision must be reached once the Will falls due; and an uncommitted
  write can be neither observed nor prevented from committing later. Two executions —
  one with no qualifying acceptance, one with a qualifying batch still uncommitted — are
  identical in every durable observation through the licensing CAS, so the claimant must
  decide the same way in both, and publishing is right in the first and wrong in the
  second. Closing it means changing a premise: dating eligibility by the fact's own
  durable confirmation instead (which is a specification change, not a synchronization
  fix, since a reconnect that genuinely occurred during the Will Delay would then be
  ineligible for having stored too slowly), blocking a due Will until all writers
  settle, or a cross-subject arbitration JetStream does not offer here.
  **No temporal bound is claimed.** An ambiguous acceptance write re-sends its ORIGINAL
  `At`, so settlement can outlive the first request by a wide margin; the statement is
  logical, not a latency budget. `TestWillAcceptanceAfterTheFinalCutoffIsMissed` pins the
  direction, and its recovery half is
  `TestWillRecoveryReusesThePersistedDecision` — if the first test ever starts
  suppressing, the residual is closed and both it and this entry are stale.

- **Administrative loss reverses direction.** Pruning facts, or recreating `$MQTT_sess`,
  removes suppression evidence, so an obsolete Will FIRES rather than a due Will being
  lost. That is the safer direction of the two, and it is the only direction available:
  nothing here can lose a Will by losing a fact.
- **A pathologically slow store can still defer a Will indefinitely, but not wedge
  one.** Scan progress is persisted by the claim renewal, so each pass resumes where
  the last left off and a finite history is always finished. What is NOT bounded is a
  store so slow that a single fact read outlasts the whole scan budget: no renewal
  lands, no progress is recorded, and the Will defers again next pass. That is a
  store-availability condition rather than a protocol one — nothing else works either —
  and under THIS condition the Will is deferred, never lost or spuriously published.
  A slow acceptance WRITE is a different condition with a different direction, covered
  by the uncommitted-acceptance residual above; this bullet is about slow fact reads and
  claims nothing about writes.
- **Early cancellation is eager, never authoritative.** The claim pass may suppress a
  not-yet-due Will as soon as a qualifying fact exists, because that half of the
  predicate is monotone. The opposite verdict is not: "nothing qualifies" is never
  persisted and never licenses a publication before `FireAt`. The phase is budgeted and
  cursor-fair, so under heavy churn it can fall behind, which delays a suppression that
  the claimant would make anyway at `FireAt` — it cannot change the outcome.

### Session teardown: arbitrated across brokers, and fenced by a permanent watermark

Discarding a session (clean start, a v5 DISCONNECT with expiry 0, session expiry,
takeover-discard) now runs under a durable **teardown claim** on the session record
and deletes only resources named after the **retiring session incarnation**
(`internal/mqtt/sess_teardown.go`, `sess_incarnation.go`). A broker that loses the
claim deletes nothing; the release is a sequence-targeted delete whose outcome is
classified by re-reading, never assumed; and a CONNECT that can interpret the record
finishes a claim it finds before starting fresh, so a crashed teardown self-heals
rather than leaking. A record the restore boundary refuses is not adopted by the
CONNECT at all: a reserved-namespace record with a valid generation is still finished
by the expiry path and the orphan sweep, but one with no valid generation is adopted by
nothing and persists with its consumers until an operator recreates the streams — the
pre-existing consequence of the generation refusal, since a generation-scoped deletion
has no name to use.

A teardown that WINS its claim records a permanent per-client **generation-retirement
watermark** on `$MQTT_teardown` before it deletes anything, so the fact survives the one
thing the record cannot: an unconditional foreign write evicting the marker off the
record's own subject. Three readers act on it — the winner's own
release-superseded arm, which condemns a same-generation survivor with a fresh marker;
the CONNECT restore, which discards a retired record instead of resuming it hollow; and
the orphan sweep, which condemns one with no CONNECT at all. A FOURTH was deleted by the
write-lineage audit: the record-absent create, which inherited the retired record's
identity as its own parent (F-029b). Its `RWID` field is retained with no reader, because
what it states — which record a retirement attributes its deletion to — is what F-036 and
the vanished-record vocabulary may need. Nothing ever reaps a watermark. See
`docs/ARCHITECTURE.md`, "The generation-retirement watermark".

What the repair is, exactly: **truthful termination, never restoration.** The deleted
consumers, holds and in-flight registers are gone; what the readers guarantee is that
the session is not served as though they existed. A client whose record was condemned
gets Session Present false on its next connection, and its QoS 1/2 messages held by the
deleted durables are lost — which is what a session discard means, and why the window
mattered.

Residuals and hard requirements:

- **The hollow interval is bounded by observation, not by an instant.** Between the
  foreign write applying and a reader condemning it, the survivor's broker still
  answers for the session: the winner's arm runs immediately, but a winner that crashed
  after recording the retirement leaves the repair to the next CONNECT or to the sweep,
  which needs a stable observation across its grace plus a later pass. Nothing is lost
  by the delay that was not already destroyed by the deletions.
- **The fence names only the LATEST retirement.** An older retired generation escapes it
  only through a same-base proposal that outlived a whole intervening session lifecycle
  (teardown, a new session, its teardown), and the single slot can itself be regressed
  by a `$MQTT_teardown` CAS admitted before a leader transition and applied after a
  newer one. Both are stacked instances of the documented apply-time CAS hole — strictly
  narrower than it, and not widened by any reaping, since nothing reaps a watermark. A
  condemnation claim admitted before a leader transition can likewise apply over a
  fresh-generation successor's record; that resolves as the accepted takeover
  convergence (the successor's broker observes the move and concedes, its client
  reconnects with Session Present false, and generation-scoped deletions cannot touch
  the successor's own resources).
- **A finish can outrun the survivor's own observation.** Whoever finishes the
  condemnation marker deletes the record, and a bound, silent Keep Alive 0 client on the
  survivor's broker that dropped the marker observation then faces an ABSENT record — the
  F-036 shape, tracked as its own §0.2 blocker
  (`docs/findings/ha-arbitration.md#f-036`), which this route is now listed under. The
  sweep defers its finish by a pass to narrow the case it owns; a CONNECT adoption
  finishes immediately by design.
- **`$MQTT_teardown` recreated alone loses every open fence.** It stamps the current
  session-stream incarnation epoch, validates clean and admits empty, so a generation
  retired before the recreation is no longer fenced. The other direction is refused rather
  than admitted: `$MQTT_sess` recreated alone leaves watermarks from a different
  incarnation, and startup fails with the coupled recreate-as-a-unit guidance. Watermark
  storage is permanent and bounded by the client-ID population.
- **An orphaned PUBREL durable is now reaped, and the PUBRELs it held go with it.**
  A `$MQTT_PUBREL_<idHash>_<gen>` consumer on the interest-retained `$MQTT_out` whose
  session generation is absent from the sweep's evidence — the persisted records plus the
  sweeping broker's own residents, which is what "authoritatively absent" means here and
  which does **not** observe a session live on another broker whose record was removed
  (F-036's open false-absence inference) — was previously referenced by nothing and
  accumulated forever; the orphan sweep now deletes it after a complete reference walk,
  a full stable-observation grace, a final authoritative record re-read and an
  instance re-lookup. The undelivered PUBRELs it was holding are dropped with it, which
  is correct in every state those checks can observe — no observable session could
  deliver them and their packet identifiers died with the session. Two windows are
  narrowed rather than closed, because a JetStream consumer delete is by name and
  unconditioned: a record save can land after the final re-read (no leader transition
  needed — a broker pausing between the consumer create and the record save suffices),
  and the durable can be deleted and recreated after the instance re-lookup. Both are
  the same bound the affine reaper already has. The consequence of losing that race is
  bounded rather than silent: the owning session's next resume verifies its PUBREL
  durable and recreates an absent one, so what is lost is the PUBRELs it held — the
  outbound QoS 2 completions those exchanges owed are not re-sent.
- **A session record without a generation is rejected, not resumed.** Records
  written before generation-scoped naming existed carry no `gen`, and a CONNECT that
  finds one fails with recreate guidance rather than falling back to hash-only
  names. That fallback is not a weaker form of the same safety: such a session has
  no identity to scope an arbitration with, so its inbound QoS 2 release could not
  be arbitrated against a session discard, and the discard would blind-purge a held
  payload whose releaser had already claimed it — silent loss, and strictly worse
  than the ordering deviation the claim exists to fix. The bridge is pre-release and
  its `$MQTT_*` schemas carry no compatibility contract, so the remedy is to
  recreate the development store. A record carrying a subscription in the reserved
  `$MQTT.>` namespace is refused on the same terms and for the same reason (see the
  namespace section of `docs/ARCHITECTURE.md`).
- **A refused restore can defer a due session expiry to a later sweep pass.** Every
  refusal on that path returns while the client ID is still registered in the
  in-process `sessLocked` map, and `expireSessionFromRecord` yields to that
  registration without re-arming the one-shot timer it has already consumed. A
  session whose deadline falls inside that window is therefore not expired at its
  deadline. It is not stranded — the periodic `sweepSessionExpiries` re-arms an
  absent timer for a due record — but the DELAY is not bounded by one sweep
  interval either: `reconcileResidentRecordState` runs ahead of it on the same
  goroutine and can hold the pass up (the unbounded-walk residual above). The window
  is pre-existing and shared by every refusal on this path rather than specific to
  one; deferring indefinitely would take a client holding a CONNECT in flight across
  every pass, which gains it nothing, since every attempt is refused.

### Inbound QoS 2 PUBREL is at-least-once across the unresolved move interval

The destructive move window that used to lose messages here is closed. The QoS 2
hold record on `$MQTT_qos2in` is a two-state machine
(`internal/mqtt/qos2_move.go`): a create-only CAS writes `held`, PUBREL delivers
the message onward **first**, and only then does a CAS at the held record's exact
sequence replace it with a terminal `delivered` tombstone. A later PUBREL that
finds the tombstone answers `0x00` without re-delivering.

The guarantee, stated exactly:

> No silent loss on any PUBREL path **of a session whose ownership does not move under
> it**. Once the terminal state commits, suppression
> of re-delivery is indefinite and independent of the deduplication window, the
> broker's lifetime, and the nats-server's. Across the *unresolved* interval
> between the onward store and that commit, the durable (QoS 1/2 subscriber) path
> is exactly-once only while deduplication survives; otherwise it is
> at-least-once. The live core-NATS fan-out is at-least-once in that same interval
> and cannot be made otherwise.

**What a teardown of an incarnation a peer is still serving can and cannot do.**
*Derived from `docs/findings/ha-arbitration.md` F-062, which is authoritative for the
mechanisms and their arms; what follows states only what a reader of the guarantee needs.*
It can no longer destroy an acknowledged PUBLISH that no removal was entitled to reach. Every
hold removal carries the highest sequence its remover could prove it was entitled to destroy,
and the `discarded` marker records as its fence the cutoff its writer was entitled to purge
through — an entitlement, not evidence the hold is gone, since the marker is written before
the purge — so a LATER exchange
on the same packet identifier proves the marker is not about it, reaps it and proceeds instead
of being answered `0x92` with its hold released.

**Which cutoff each PURGING arm uses is NOT uniform, and neither is what licenses a
destruction on each.** There are four arms in the analysis and three of them purge: the arm
that waits out a release destroys nothing at all, and re-competes instead. This document
states neither the mapping nor the criterion, because every paraphrase of
them attempted here has been wrong. F-062 carries the authoritative mapping and the per-arm
criterion; read it there. The one consequence a reader of this guarantee needs, and the only
one stated here: **"accepted after some teardown enumerated" is not by itself a safety
property.**

Neither remedy makes the claim exchange-exclusive; it is not, and must not be described that
way.

Four residuals follow from that, and they are three different kinds. **Two are ordinary
in-contract leaks** (the first and second below). **One is loss but requires record
corruption**, so it is outside this finding's condition. **One is not reachable in contract
at all** and is a fail-closed disposition rather than a hazard. None is conditioned on a
rollout:

- **A spared hold above the cutoff.** `STREAM.INFO`'s `State` and `State.Subjects` are two
  separate store reads, so a hold can appear in the enumerated map while its record sits
  above the returned last sequence. It survives the teardown and is left to the orphan
  sweep. Leak, not loss. Note what the pair does not license: the subject map carries
  per-subject counts, not sequences, and is sampled after `State`, so it is not evidence that
  the subjects returned sit below the watermark. The watermark's only use is to bound a
  destructive purge.
- **A stranded post-enumeration tombstone.** An exchange completing between the
  enumeration and the purge leaves its terminal tombstone above the cutoff, so the bounded
  purge spares it. Leak; the orphan sweep's unclaimed-hold arm reaps it, and a next PUBLISH
  on that identifier takes the existing reap-and-retry path.
- **A corrupt but readable fence** can over-purge up to its own value on the arm that
  adopts a pre-existing marker. This one *is* loss, and it requires record corruption,
  which is outside the condition above; nothing here bounds a corrupt readable fence. The
  `MaxUint64` refusal only stops it degenerating into an unbounded purge.
- **A refused watermark leaves a partially torn-down session.** If a walk cannot obtain a
  usable cutoff it fails the clear rather than reporting the session destroyed, which
  leaves the record marked and refuses that client ID's next CONNECT until an adoption
  succeeds. Fail-closed and self-healing, and **not reachable in contract** — the last
  sequence never rewinds under a delete, a replacement, a filtered purge, a compact at or
  below the tail, or expiry — so it is a disposition rather than an operational hazard.

Every sequence identity above, the fence included, is scoped to **one stream incarnation**;
recreating `$MQTT_qos2in` is an all-brokers-stopped operation (`docs/ARCHITECTURE.md`,
"Compatibility boundary"). Two neighbouring consequences of the same condition (a delivery
after discard, and the orphan sweep condemning a live remote generation whose record is
absent) belong to **F-036**, which is still an open alpha blocker; the cutoff widened the
first of them, since a spared hold stays serviceable by that peer.

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
  of them can win **while a record occupies that subject**. So a discard that observes no
  claim is not followed by a delivery **of the exchange it enumerated**, and the finalizer
  can no longer read a purge's absence as proof the exchange completed. Two limits on that
  sentence: the enumeration is a one-shot snapshot, so a hold on a subject it never saw is
  never competed for and CAN still be delivered after the discard (F-036); and the claim
  subject is reused by successive exchanges, so exclusivity is per-occupancy rather than per
  identifier for all time. What makes that second limit harmless is not the claim but the
  fence a `discarded` marker records, which lets a later exchange prove the marker is not
  about it (F-062). What the claim does not order is the wire: a
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
  is still in flight" is not provable. It is small (roughly 150 bytes, on a subject
  scoped to the retiring incarnation), and its reach is **not confined to the exchange it
  was written for**: a peer still serving that incarnation can accept a LATER exchange on
  the same packet identifier and read the same marker. What bounds that reach is the
  **fence** the marker carries — the highest sequence its writer was entitled to destroy —
  so an exchange whose hold sits above it reaps the marker at its exact sequence and
  proceeds, rather than being answered `0x92` (F-062 mechanism 2). It is retired by the
  periodic orphan sweep
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
  re-reads the hold as absent, and reports success. For the exchange the marker was
  written for, nothing is delivered and nothing is lost either way — only the reason
  code is more optimistic than the marker-backed one. A LATER exchange is not reachable
  this way: the sweep's purge is bounded to the exact record it observed and re-confirmed,
  so a hold accepted after that observation is spared rather than destroyed, and its own
  PUBREL reaps the marker because its hold sits above the fence. See F-062.
- **A `releasing` claim whose broker died is retired the same way**, hold first.
  This does not prove a paused process can never resume; it preserves the ordering
  that already governs one. A releaser that has not passed its authoritative
  re-read delivers nothing, and one already beyond it is inside the post-claim
  delivery-ordering residual above. Age cleanup is not exactly-once coordination.
- **A superseded PUBREL is answered `0x92` after its message was in fact
  delivered.** When a later exchange has taken the same
  `(client, packet identifier)` subject — a client may reuse an identifier as soon as
  its exchange completes, and the subject is generation-scoped so this is never a
  different incarnation — the finalizer abandons without mutating
  anything and reports the identifier as gone. No loss, and the later
  exchange's payload is untouched.

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
surface that previously covered five streams to all eleven, with the same
consequences the `$MQTT_qos2in` precedent established:

- **A nonconforming stream fails broker startup**, with an error naming the
  stream, the offending setting, and the recovery. Nothing is repaired or
  updated in place.
- **A stream mutated after initialization splits the account's brokers.**
  Nonconformance that is already present when a broker starts is refused by every
  initialization alike, so no broker serves the account. A config mutated *after* a
  manager initialized is the case that splits: that manager is returned to later
  connections on its original validation decision and keeps serving, while any
  initialization afterwards — another broker, or the same one restarted — refuses,
  surfacing as CONNECT failures on the refusing broker only. The recovery for both is
  **stop-all**: stop every broker for the account, recreate the named stream per the
  error's guidance, then restart. The errors offer no in-place fix, and some name a
  coupled unit — `$MQTT_sess` is recreated together with `$MQTT_teardown`, both at once,
  or the incarnation epoch is minted against stale watermarks.

  **The two-stream rule is a PROCEDURE, not a guarantee that every partial recreation
  fails closed**, and the difference matters to an operator deciding whether an unsafe
  recovery would be caught. `$MQTT_teardown` carries the session epoch in STREAM METADATA
  and so is what can detect a recreated `$MQTT_sess`. The complete rule is two clauses:

  - **`$MQTT_sess` recreated** — refused if and only if an OLD `$MQTT_teardown` survives
    beside it, which aborts account-manager initialization. Recreate both together and
    startup ADMITS — and every client's **permanent CONNECT acceptance history goes with
    the old stream**, since the facts live on it. A delayed Will enqueued before the
    recreation can then no longer be suppressed by the acceptance entitled to suppress
    it, and it fires.
  - **`$MQTT_sess` kept** — `$MQTT_teardown` may be recreated, and is ADMITTED: brokers
    start normally while every open generation-retirement fence is silently gone.

    So the recovery is specified as both at once rather than left to what a validator
  happens to catch. `$MQTT_origin`, once the third member of that unit, is no longer
  defined by this broker: an account still holding it is REFUSED at startup with delete
  guidance rather than validated. This is not a rolling
  upgrade. A *conforming* deployment restarts exactly as before —
  default broker-created streams are conforming, and so are supported administrative
  variations such as a source on `$MQTT_rmsgs`.
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
  support floor is nats-server 2.14.5, which always understands `AllowMsgTTL`.
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

### The retained index is not arbitrated by sequence, and a lost application is not repaired

Retained state is durably arbitrated: `$MQTT_rmsgs` holds exactly one record per
topic (`MaxMsgsPer: 1`), a write evicts the record it replaces at store time, and
an empty body is the deletion authority. What every broker actually SERVES from is
a volatile index — `as.retmsgs` plus the `rmsCache` copy — and that index is not
arbitrated. Its three writers (the retained stream's per-broker ephemeral `AckNone`
consumer callback, the publishing broker's synchronous local seed, and the
decode-failure eviction) each apply what they observed without comparing it to what
the index already holds.

Delay and loss are different. A delayed application eventually arrives and the
index settles on whatever it applies last. A **lost** one never arrives: the
consumer is `AckNone`, so nothing is redelivered and no writer runs.

What repairs a loss depends on whether the subject is still IN the index, and the
two cases are not alike:

- **An indexed subject is repaired at subscribe time.** Any later matching
  SUBSCRIBE — wildcard or exact — puts the subject through
  `loadRetainedMessages`, which reads the stream once the cached copy has
  expired. A tombstone read there calls `removeRetainedMsg(subject, 0)` and the
  entry goes; a replacement read there refreshes what is served. So these arms
  are bounded by the retained cache TTL plus the next matching subscription, not
  unbounded. The load does not call `addRetainedMsg`, so the index's stored
  SEQUENCE is not refreshed and stays stale until a later application.
- **An absent subject is repaired by nothing.** Serialization enumerates
  `as.retmsgs`, so a subject missing from the index is never a candidate for a
  wildcard, and an exact filter's forced authoritative load populates the cache
  without making it deliverable. There is no periodic repair, so the subject
  stays undeliverable on that broker until some later application for it happens
  to arrive, the broker restarts and replays the stream, or the F-059
  reconciliation exists.

Filed as [F-059](findings/runtime-correctness.md#f-059); the observable residual
is:

- **An absent entry suppresses replay for wildcard AND exact filters.** A missed
  or reordered add, or an over-eager eviction, leaves the subject out of the
  index. `serializeRetainedMsgsForSub` matches candidate subjects against that
  index and returns immediately when it is empty, so the exact filter's forced
  authoritative stream load fetches the record into the cache without making it
  deliverable. That forced load narrows only the window in which a just-published
  record's callback lands during the SUBSCRIBE's own round trip. **This is the
  arm with no bound**: when the add was LOST rather than delayed, a retained
  message that exists in the stream and is served correctly by every other broker
  is simply never replayed by this one, and no subscription repairs it.
- **A missed or reordered replacement serves the OLD value**, to wildcard and to
  exact subscribers alike — a lookup returns the cached record without reading the
  stream. A *delayed old* add can actively restore a stale value over a newer one,
  since the add overwrites the sequence and the cache entry with no comparison.
  Bounded: once the cached copy expires, the next matching subscription reloads
  the stream and serves the current record. The index's stored sequence is not
  refreshed by that load and stays stale until a later application.
- **A missed or reordered tombstone serves a deleted message**, to wildcard and
  exact subscribers, under the same bound — the reload reads the tombstone and
  removes the index entry, so this arm self-heals at the next matching
  subscription after cache expiry.
- **Two mitigations are deliberately absent.** The `RD` delete-notification
  protocol was an additional best-effort removal path for a remote broker whose
  tombstone delivery was lost; it was removed with the rest of the development-era
  retained compatibility ([F-056](findings/resolved.md#f-056)). That widens the
  tombstone arm's window — a lost tombstone now waits for cache expiry plus a
  matching subscription rather than possibly being retired sooner by the
  notification — without adding a failure class or making the arm unbounded. The
  decode-failure eviction that same change added is itself unconditional, so a
  delayed corrupt-record callback can evict a newer valid entry, landing in the
  absent-index arm above. The F-059 fix has to arbitrate all three writers and add
  a repair for absent entries, which is the case no subscription reaches.

Supported current-format retained behavior is otherwise unchanged. Records the
broker cannot decode now fail closed everywhere — warned with the subject and the
error, and evicted rather than silently ignored — where the development-era JSON
fallback used to accept some of them; a record carrying a valid `Nmqtt-RFlags`
header from a sourced foreign account is still accepted, and malformed *optional*
headers (properties, expiry) are still ignored rather than rejected. Legacy
development records (a JSON body, or a `-1` marker tombstone) and mixed old/new
development fleets deliberately change behavior per the compatibility boundary and
are not supported topologies.

### A connection that never sends CONNECT is closed; a connected one may still be silent forever

MQTT 3.1.1 / 5.0 [3.1.4]: if the Server does not receive a CONNECT Packet within a
reasonable amount of time after the Network Connection is established, the Server
SHOULD close the connection. The broker implements this SHOULD. A connection that
sends nothing, or an incomplete CONNECT, is closed after `-connect-timeout`
(default 5s, `MQTTNB_CONNECT_TIMEOUT`); the window is absolute rather than
restarted per byte received, and the bound is fail-closed — a value of `0` or
below is refused at startup rather than read as "unlimited". See
`docs/ARCHITECTURE.md`, "Connection liveness bounds".

The CONNECT that races the expiry is refused before any authentication is
attempted, so a connection the deadline closed never dials a per-client NATS
connection and never binds session state. CONNECT authentication itself is
unchanged: credentials are still adjudicated by the configured client dialer.

**The residual is on the other side of CONNECT**, and it is pre-existing rather
than introduced here: a client that completes CONNECT with **Keep Alive 0** is
bounded by nothing afterwards, because the keep-alive read deadline is armed only
while the negotiated value is nonzero. The consequence for ownership convergence
is stated above ("A silent client with keepalive 0 converges only at unbind"); the
consequence here is simply that this bound covers the pre-CONNECT window and no
other. `-keepalive-max` is the operator surface that caps a v5 client's requested
Keep Alive.

This bound is not a denial-of-service defense and is not documented as one. It
makes an idle connection's lifetime finite; a peer can still hold roughly
`accept rate x window` connections open, and `-max-conns` remains the absolute
cap.
