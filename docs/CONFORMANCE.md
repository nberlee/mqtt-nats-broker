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

Both are tracked as release blockers in `docs/ROADMAP.md`.

### Started QoS 2 delivery to a shared-subscription member

When a client unsubscribes from (or leaves) a Shared Subscription while the
broker has already started sending it a QoS 2 message, that message does not stay
with the client the exchange began with. `releaseSharedConsumer` drops the
leaving member's pending QoS 1 and QoS 2 state, so the message is re-offered to
another group member, and it is dropped outright if the leaver was the last
holder of the group durable.

Redistribution after a connection loss is permitted for QoS 1. It is not a
general override for UNSUBSCRIBE, and a redistributed QoS 2 message does not
satisfy the requirement to complete the delivery to the chosen client
([MQTT-3.10.4-3], and the chosen-client affinity of MQTT 5.0 [4.8.2]). Resolving
it needs session-affine ownership of a started delivery.

Plain (non-shared) subscriptions have their own open item: an unacknowledged
QoS 1/2 PUBLISH is currently dropped on UNSUBSCRIBE. The decided fix is a durable
drain; see `docs/ROADMAP.md`.

### Inbound QoS 2 PUBREL is not idempotent across the hold-record move

`mqttProcessPubRel` removes the QoS 2 hold record before storing the message for
onward delivery. Two windows lose the message rather than duplicating it:

- the delete is committed by JetStream but its response is lost, or the process
  crashes immediately after the commit;
- the delete succeeds and the onward store then fails.

In both cases the hold record is gone with nothing delivered, so the client's
PUBREL retry is answered `0x92` (packet identifier not found). Reversing the
order would trade this for duplicate onward delivery, which QoS 2 forbids; a
recoverable move protocol is required.

What *is* handled: a hold lookup that fails for any reason other than "not
found", and a hold delete that fails before JetStream acted on it, both suppress
the PUBCOMP and close the connection instead of reporting `0x92`, so the client
retries against intact session state.

A hold record that was already removed between the lookup and the delete is
reported as `0x92` and the loaded copy is not delivered. Both removers are
possible — a concurrent PUBREL for the same packet identifier, which does deliver
onward, and a session discard, which purges hold entries without delivering — so
this is a choice between two wrong outcomes rather than a delegation of the
delivery. Delivering the loaded copy would risk duplicating a concurrent PUBREL's
delivery or resurrecting a message from a discarded session, which QoS 2 must not
do; `0x92` states accurately that no session state for that identifier remains.
When the remover was a session discard the message is dropped, which is a
property of discarding the session, not of this branch.
