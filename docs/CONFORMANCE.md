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

## Supported-alpha boundary for shared subscriptions

The alpha supports shared subscriptions through granted QoS 1. By production default, a
shared filter requesting QoS 2 receives SUBACK Granted QoS 1 (`0x01`) and is persisted as
QoS 1; messages published at QoS 2 may consequently be delivered to that shared
subscription at QoS 1. Non-shared QoS 2 remains supported. MQTT 5 continues to advertise
Shared Subscription Available, because shared QoS 0/1 is supported.

The session-affine shared-QoS-2 implementation remains in the tree behind a programmatic
test option that the production command does not expose. It is **not** part of the alpha
conformance claim. Every affine residual and deviation below is retained as an experimental
implementation record, not as an alpha release gate, unless the same section separately
demonstrates a non-affine route.
Other finding implementations may break an affine-only route without taking that repair
into scope; ROADMAP chapter P gathers the fallout for a future investment decision.

Experimental persisted state has no compatibility promise. Disabling the gate, moving it
across an upgrade or rollback, or restoring it into another build is unsupported. Detection,
automatic QoS-2-to-QoS-1 persisted-state downgrade and migration readers are not provided.
Before leaving the experiment, operators must use the complete nine-stream bundle reset in
`docs/ARCHITECTURE.md`, which also discards supported MQTT state in that account.

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
  30 s). Ordering is preserved on THIS path (ascending stream sequence, Spec5 §4.6) — the
  citation was [MQTT-4.6.0-1], which governs CLIENT retransmission rather than server
  ordering, so it is corrected to the section; the claim is about the withheld-delivery NAK,
  and it does **not**
  extend to the experimental affine shared-subscription path, where F-071 records a
  parked same-topic ordering violation (see Known deviations) — and
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

### Authorization of the authenticated identity — CLOSED, with residuals

F-079, F-080 and F-081 were three directions of one defect: the broker moved
delivery and persistence onto the full-rights service connection while keeping
upstream's client-connection permission hooks as inert stubs, and it never
learned the authenticated identity's NATS account or permissions at all. It now
asks, on the client's own connection, with `$SYS.REQ.USER.INFO`. The mechanism
is in `docs/ARCHITECTURE.md`, "Auth passthrough"; what follows is only what the
broker still does not promise.

What changed, against the measurements that opened these findings: a publish the
account denies is no longer stored in `$MQTT_msgs`, no longer retained, no
longer held for QoS 2 and no longer acknowledged as success; credentials from
another NATS account no longer authenticate; and a wildcard subscription
overlapping a narrower subscribe deny no longer receives QoS 1/2, retained or
affine shared QoS 2 deliveries on the denied subject.

**Residual 1 — the permission snapshot is taken at CONNECT and never
refreshed.** An ACL changed while a client is connected is enforced immediately
on the routes NATS owns (QoS 0 on a user topic, and SUBSCRIBE admission) and
only after the client reconnects on the service-owned routes (QoS 1/2 delivery,
retained replay, and every durable store). There is no NATS notification for a
permission change, and polling would put a round trip on every delivery while
still not being atomic with a later service publish. Bounded instead by the
connection: the snapshot dies with the per-client NATS connection.

**Residual 2 — an MQTT connection does not survive a per-client NATS
disconnect.** Because the snapshot's validity is the connection's validity, a
disconnect or close on the per-client connection closes the MQTT socket. A
transient NATS outage therefore disconnects MQTT clients that would previously
have stayed connected. This is deliberate and it is the fail-closed direction:
before, such a client stayed connected with its QoS 0 traffic silently broken
and its cached rights still authorizing durable stores.

**Residual 3 — an MQTT 3.1.1 publisher is not told why.** 3.1.1 has no
per-publish refusal code. [3.3.5.2] permits either a positive acknowledgement or
closing the network connection; the broker closes the connection, so the client
learns that something was refused but not which publication or why. MQTT 5
clients get PUBACK/PUBREC reason 0x87 with a reason string. Note this
deliberately differs from the reserved-`$MQTT.>` refusal, which still answers a
3.1.1 client with a success-shaped acknowledgement; both are legal, and matching
that behaviour here would have preserved the very outcome F-079 reported.

**Residual 4 — the security principal is account plus client ID.** Any
credentials valid in the account may resume any session in it by presenting its
client ID, and a QoS 2 exchange held under one identity is completed under
whatever identity presents the PUBREL. MQTT 5 [4.3.3] / [5.4.2] recommends
authorizing access to the Client Identifier itself; doing so would require
persisting the authenticated identity with the session, which the compatibility
boundary in `docs/ARCHITECTURE.md` rules out. Stated rather than enforced.

**Not a residual, two deployment requirements - and the second is UNCHECKABLE.**
The MQTT identity must be able to publish `$SYS.REQ.USER.INFO` (a blanket publish
deny on `$SYS.>` refuses every CONNECT and cannot be carved back with an allow,
because NATS evaluates deny before allow). It must also be **denied publish on
`$MQTT.>`**, and the broker cannot verify that: the identity reply is an unsigned
account subject whose request is observable to an account subscriber, so a
principal able to publish there can answer for any client, and every test of the
claimed permissions is one that forger controls. The deny is therefore a security
prerequisite of auth passthrough, not an enforced invariant. It is the same deny
the reserved namespace has always required, and for a stronger reason: such a
principal can already forge session records directly. See `docs/ARCHITECTURE.md`,
"Auth passthrough".

**Not a residual, a mode.** With `-auth-passthrough=false` there is no
per-client identity, so no per-client enforcement exists: every client acts as
the service identity. The mode assumes that credential is trusted and suitably
privileged.

**Residual 5 - a refused CONNECT is a bare socket close.** Every refusal above
routes through `authViolation`, which closes the connection and sends no CONNACK.
So a client refused for a cross-account credential, an unidentifiable identity or
an expired one learns only that the socket closed, and cannot tell that from a
network failure. That is [F-061](findings/runtime-correctness.md#f-061), which
owns that path and is a post-alpha candidate; it is recorded here so the closure
above is not read as claiming more than it delivers.

**Residual 6 - a denied QoS 2 PUBLISH costs one JetStream read.** MQTT transfers
ownership of a message at PUBREC, so a later PUBLISH on the same packet
identifier must be answered from the existing exchange whatever subject it names.
The broker therefore cannot refuse a denied QoS 2 subject without first reading
that identifier's hold to see whether an accepted exchange is there. An
authenticated but publish-denied client can drive one bounded read per such
packet. It is one read, no write and no retry, and it requires valid credentials.
A session-scoped occupancy cache would remove it and is deliberately not built:
it would be a second source of truth about which identifiers are occupied.

**Residual 7 - the account check is a check, not a transaction, and this is a
CLASS rather than one window.** Every gate, guard and Will discharge requires the
service connection to be bound to the account the identity was discovered
against, and the check is repeated immediately before each durable write - the
`$MQTT_msgs` store, the retained store, the derived Sparkplug publication and the
QoS 2 hold CAS. None of those checks is atomic with the write it guards, and
there is more than one layer that can hold a write in between:

- nats.go's reconnect buffer on the service connection, which holds a publish
  across a reconnect;
- the JSA send queue (`mqttJSA.sendq`), which is drained later by the internal
  service client and carries no record of the account that authorized each entry;
- the per-client connections' own reconnect buffers, which can hold a live
  fan-out (not durable state) across a reauthentication.

So a write authorized against one account can, in principle, be emitted after the
service connection has come back bound to a different one. Closing that
absolutely needs the authorized account carried through the transport and checked
at the socket, which is a transport change this item did not take. It is filed as
F-085 in `docs/ROADMAP.md` §0.5, with the stopping rule stated there.

**Removing one layer was tried and reverted**, and the reason is worth recording:
disabling the service reconnect buffer narrowed nothing - the send queue behind it
holds writes just the same - and introduced a reachable regression, because a QoS
0 Will's publish then fails outright on an ordinary service blip and the outbox
worker finalizes a Will that was never fanned out. The hazard needs an operator to
repoint credentials mid-flight; the regression needed only a blip.

**Residual 8 - a Will can still be lost when its authorized account is
unavailable, but only in the cases named here.** A Will refused because
`deliverWillRecord`'s account fence cannot establish the destination is now
RETAINED in memory and published when that same account returns
(`will_blip_retry.go`), so an ordinary NATS blip no longer destroys it. That was a
regression this work introduced and it is fixed rather than accepted: an outage is
an UNKNOWN, and answering it the way a DENY is answered destroys a Will the client
was entitled to.

What remains lost, and each of these is a deliberate bound rather than an
oversight:

- **A broker restart or shutdown.** The retention is in memory, and it cannot be
  otherwise: the only durable park is `$MQTT_willq`, which is exactly what is
  unreachable while the service connection is down. Shutdown reports every Will it
  is still holding at error level rather than publishing it, because the
  destination is still unestablished and publishing into an account nobody has
  re-identified is the failure that would be worse.
- **The capacity bound.** A broker holds at most `mqttOwedWillMaxBytes` of Wills
  across an outage; beyond that the Will is refused and the loss is logged with a
  running count. A byte bound rather than a count, because the cost is the
  payloads.
- **An account that never comes back.** A Will whose account is replaced rather
  than restored stays parked and is never published into the new one. It is
  released at shutdown with a log line.
- **A delayed Will whose enqueue could not reach `$MQTT_willq` at all.** Narrowed:
  an enqueue refused because the service account is unestablished, or is not the
  one the Will was authorized against, is now RETAINED and its ENQUEUE
  re-attempted when that account returns (`will_delayed_retry.go`) — it is not
  published, because publishing a delayed Will skips the acceptance-fact
  arbitration and a still-future one would go out before its deadline. It carries
  the original `FireAt` and an immutable `WillID`, so every re-attempt is the same
  create-only CAS. What is still lost here: a broker restart or shutdown, its own
  separate capacity bound, and an account that never returns — the same three
  bounds as above, on a separate queue with its own budget so a delayed backlog
  cannot starve the immediate one.

Only the fence's own refusal is retried. A generic delivery error is not, because
it can follow a partial effect - a fan-out that reached some subscribers, a store
that landed - and re-attempting it would duplicate that. Such a Will is reported
as lost rather than retried.

**The delayed enqueue classifies by EXIT, not by error value, and that distinction
is what keeps this safe.** An attempt whose store retries are exhausted and whose
immediate-publish fail-safe then refuses the destination returns the SAME sentinel
a pre-effect refusal does - but its create may already have landed, so it is
TERMINAL rather than retainable. Retaining on the error value would re-attempt an
obligation that may already be durable and write a second record for one Will.

**Two exits still publish a delayed Will without arbitration, and both are
pre-existing.** `enqueueDelayedWill`'s fail-safe publishes when the record is
refused by its own writer validation, and when the store attempts are exhausted
without confirming. The Will Delay is sacrificed rather than the Will, deliberately,
because the record is UNCONFIRMED and not proven absent. The cost is four things,
not one: the sacrificed delay, the bypassed suppression arbitration, a possible
DUPLICATE publication where a record did land, and a possible delivery failure.
Anchoring the deadline to the close and routing every parked delayed Will through
the outbox send MORE expired actions down that path, so the population widens even
though the mechanism is untouched. The writer-validation exit is an invariant
assert no production caller can reach - every one supplies valid generated
identities, the startup-validated stream epoch and a nonzero session baseline -
though tests reach it deliberately.

**A degraded suppression boundary when an owed Will has no close observation.** The
Will Delay is measured from the observed close. A Will that reaches a producer OWED
but with no observation recorded is an invariant violation; it is enqueued with its
delay measured from that moment and the violation is logged, rather than being
discarded. Suppression-boundary correctness is then lost both ways (an acceptance in
`[trueFireAt, now)` can wrongly suppress, and one in `[now, trueFireAt)` would
wrongly fail to), and the true boundary
cannot be reconstructed from a state that never recorded it. The producers enqueue such
a Will as **due now** rather than restarting its delay: it is already owed, so a fresh
full delay would hand back a suppression window it has already spent.

**The close-to-observation interval is not bounded.** The Will Delay runs from the
observed close, which removes the teardown gap that used to sit inside the deadline —
the session gate, a clean-session discard and two JetStream round trips. It does **not**
bound how long the close takes to be observed. Closing the socket ends a failed write
but does not bound the read loop's outstanding packet work (it processes packet data
before handling the read error), and it interrupts neither a gate wait nor a JetStream
operation inside packet processing. So **deadline correctness is not delivery
promptness**: this removes a restarted delay and a wrong suppression boundary, and
bounds neither teardown latency before the enqueue nor publisher blocking after it. The
write deadline does not cover this interval.

**Whole-second truncation against the session expiry deadline.** The close stamps that
date session expiry are whole seconds while `FireAt` is nanoseconds, so a Will can fall
due fractionally after the session's own persisted expiry deadline. Pre-existing, and
neither introduced nor widened here.

**A takeover before the enqueue still destroys a delayed Will whose delay had already
elapsed.** `mqttDiscardWillOnTakeover` discards every non-owed-now disposition with no
dated test. Anchoring the deadline makes the defect *nameable* without closing it, and
the one-comparison fix is refuted — see the roadmap bullet split out for it, which
carries the verified interleaving and the arbitration gap.

**Residual 9 - an affine QoS 2 copy whose group cannot be derived is withheld at
a bounded rate, not disposed of.** The affine delivery guard recovers its queue
from the claim's group durable, because its own subscription carries none. A
durable this broker did not generate leaves the guard unable to say whether the
delivery is denied - and an unknown is not a denial, so the copy is refused
WITHOUT being acknowledged rather than consumed on a verdict that was never
reached. A permanently malformed claim therefore redelivers indefinitely, paced
by the existing refusal backoff. Withholding is the fail-closed direction;
acknowledging would destroy a message no one judged.

**Residual 10 - a delayed Will whose account never comes back accumulates.**
`deliverWillRecord` refuses a record whose `Account` is not the one the service
connection is currently bound to, and leaves it in `$MQTT_willq` rather than
finalizing it, so that a broker pointed back at the original account still fires
it. The cost of that choice is the case where it never is: an operator who
permanently repoints the service credential at a different account leaves every
record authorized against the old one retrying for as long as the stream keeps it.
Nothing publishes them and nothing deletes them. Dropping them instead is the
alternative, and it is worse in the direction that matters - it discards a Will
the client was entitled to have published, for what may be a transient
misconfiguration. There is no legacy-record case here: every writer in the tree
stamps the account it was authorized against, so a mismatch is a live operational
change and not a persisted shape from an earlier build. See the compatibility
boundary in `docs/ARCHITECTURE.md`.

**Residual 12 - two of the four Will discharge sites read the authorization
decision without a fixture that proves it.** The decision is captured at CONNECT
and read at four sites: the immediate path, the delayed-outbox gate, the
lost-session-gate route (`mqttPublishOwedWill`), and the uncertainty-parked gate
in `sess_uncertain.go`. A per-site mutation census - forcing each site's verdict
to `true` in turn and running the whole suite, tagged and untagged - establishes
which of those reads are load-bearing in test and which are only asserted in
prose. The immediate site and the delayed-outbox site each fail their paired
fixture under that mutation. **The other two survive it**, so for those two the
claim rests on reading the code, not on a falsifiable test. Both are the same
one-line read of the same captured flag, and reaching them needs a record-derived
eviction or a session-record ambiguity respectively - neither drivable from a
connection. An attempt at the first was written, passed with a live control, and
was then DELETED, because the mutation showed it was reaching the immediate site
instead: a fixture that names a route it does not take is worse than an
acknowledged gap. Filed as F-086 in `docs/ROADMAP.md`.

**Residual 11 - an allowed shared member that joins late waits out the refusal
backoff.** A group message offered to a member the account denies is handed back
paced, on the delivery-count backoff, because a sole denied member otherwise loops
against `MaxDeliver` of -1. The cost is on the other side of that trade: once the
backoff has settled, a member who joins the group afterwards is not woken by the
join and is served on the next scheduled re-offer, up to the 30s cap. Nothing is
lost - the message is still there and still offered - but its delivery to a late
joiner can be delayed by up to that cap. Waking the consumer on a join would remove
the delay and needs per-message refusal state that survives nothing; the bounded
wait was taken instead, and it is stated here rather than implied.

### Outgoing DUP on a first transmission — CLOSED (F-072)

**Status.** Closed 2026-09-09. The mechanism below is implemented, including the redesign
that closed the crash window a holistic review found (a surviving marker used to be read as
proof that nothing was transmitted, while the transmission was recorded only by a
fire-and-forget delete issued after the socket write). The required validation and four
holistic review rounds passed; round 4 approved after sweeping every identifier-release
path.

The outgoing DUP flag is derived from **durably recorded send intent for this identifier —
the PUBLISH may have been written** — and from nothing else. `trackPublish` no longer reads
the JetStream delivery count for it (`dcount > 1` was the defect: a copy offered and refused
before tracking raises that count without any PUBLISH reaching the client). The durable fact
is the **unsent marker** — a sibling record created with the in-flight register in one
atomic batch and consumed, by a CONFIRMED delete, after the frame is admitted and BEFORE the
first byte is written — so DUP is `mqttPending.transmitted`, set under the session lock by
the section whose intent landed and by a PUBREC rollback, and seeded on resume from the
marker's presence (`inflight_marker.go`, `docs/ARCHITECTURE.md` "Outbound in-flight
registers"). Both acceptance tests hold unchanged: a pre-`trackPublish` refusal sends DUP=0
on the next offer although its delivery count is above one, and a broker that dies after
the pair's confirmation and BEFORE intent is recorded re-offers the delivery DUP=0 under the
register's own identifier after recovery.

**Accepted conservative behaviour, stated rather than claimed away.** A broker that dies
AFTER intent is confirmed and before the socket write leaves a durable state identical to
one that wrote and died before the acknowledgement. Recovery therefore answers both the same
way: the same identifier is re-offered with **DUP=1** and is HELD — neither Message Expiry
nor an UNSUBSCRIBE drain's withdrawal nor a Maximum Packet Size refusal may discard it as
never sent; on those three paths only the client's terminal acknowledgement or the
session's destruction releases it. That retention is scoped to those paths: the terminal
DENIAL path (`releaseTransmittedIdentifier`) and a consumer purge still release an
identifier without receiver completion, which is F-088's filed subject — an open alpha
blocker this item neither closes nor worsens. The durable record
proves intent, not a physical write, and this document does not claim otherwise. A spurious
DUP=1 on a PUBLISH the client never saw is not a conformance failure: §3.3.1.1 makes DUP a
"possible redelivery" signal, never a receiver-side dedup instruction, and QoS 2 is
protected by the packet-identifier exchange regardless. DUP=0 on a genuine
retransmission —
the behaviour this replaces — is the wire-visible error.

**Cost accepted with it.** A Maximum Packet Size refusal of a RECOVERED possibly-attempted
exchange retains the exchange whole — entry, register, source, identifier — because refusing
this attempt proves nothing about earlier ones and the receiver may hold the identifier. The
message is re-offered and refused at AckWait cadence until the client acknowledges it or the
session ends; it occupies one in-flight slot for that long. The same holds across a
restart for a possibly-attempted exchange whose source has meanwhile been acknowledged or
reaped by its Message Expiry TTL: recovery reserves the identifier rather than consuming the
register as complete, and nothing can be re-offered for it, so it too is held until the
client acknowledges or the session ends. **And it holds on the LIVE resume reap**, which
until F-100 released such an entry outright on source absence alone: the identifier and its
Receive Maximum slot are now retained there on the same evidence, because a message leaving
`$MQTT_msgs` says what this broker can no longer do and nothing about what the client is
holding. The cost is one identifier and one slot per such exchange, in place of a slot freed
under a live obligation — and freeing it was the worse direction, since a later allocation
of a *different* identifier then transmits while the first exchange still stands.

**Two qualifications on that hold, neither conditioned on a rollout.** A consumer
teardown's purge no longer ends it: since F-101 `purgeConsumerPendingLocked` reserves a
possibly-attempted entry rather than deleting it, and the void staging before it records a
never-transmitted verdict only for a register that is actually proven unsent. The
reap's reservation, unlike recovery's, **keeps** its source mapping, so drain enrolment can
claim a vanished source; the claim floor runs before `recoverDrains` and gets a pruning
opportunity in the same resume, but a claim staged by a live UNSUBSCRIBE after the reap has
none before the next resume, and neither is a guarantee. And **cross-broker authority over
a live incarnation's identifiers remains F-098's**: this gate reads local cached state, so
it narrows what a single broker releases without establishing what a peer has consumed.

An exchange PROVEN unsent is still discarded and
completed as before. An intent delete whose result is ambiguous writes nothing and releases
nothing; the next offer re-attempts the same exact delete — and "ambiguous" includes a
generic delete failure, since the store may have recorded the deletion before failing.

**Stated residuals this loop found at HEAD, filed rather than fixed here.**

- **F-101 — CLOSED, and both routes now release only on proof.** A teardown's purge and
  the void staging before it both ask `provenUnsentReleasableLocked`, so a
  possibly-attempted exchange keeps its identifier and its capacity charge, keeps its
  durable owner register — uncollected, so the ladder cannot destroy the slot the exchange
  needs to complete — and gets no never-transmitted verdict. A retained send, which has no
  register and no source to reason from at all (R6), is released at its ABANDONMENT
  instead, the one site that proves no bytes were written, and
  `dropUnredeliverableRetained` is deleted so a resume decides nothing about retained
  state. A staged verdict also fences its own register in `beginSendLocked`, or a callback
  already past the subscription barrier could transmit after the verdict was staged and
  make it false. What each route costs is stated below.
- **F-101's own accepted cost, stated rather than concealed behind F-102.** When a consumer
  teardown overlaps an OPEN outbound PUBLISH send section, the teardown preserves the entry
  and its durable owner register and schedules no later release — it cannot decide an
  outcome that has not happened yet, and a release it cannot perform under its own lock is
  not performed elsewhere. If first-send intent then fails DEFINITELY before any write,
  this **can** retain one packet identifier and one capacity charge despite proven
  non-transmission, and repeated occurrences **can** exhaust the session's outbound QoS 1/2
  capacity. A fresh recovery over a `Lost` source restores the surviving marker-present
  register rather than consuming it, so the hold **can** persist for the session's
  remaining lifetime. Two paths can still end it, and neither is live/restart agreement: a
  later same-object resume can release the surviving LIVE entry through F-100's
  proven-unsent reap, which a fresh recovery cannot because it installs no ack subject
  (F-102's asymmetry); and the Maximum Packet Size callbacks keep their own independent
  proven-unsent disposal, whose failed register delete can leave a pair a fresh
  `Lost`-source recovery reserves. F-101 accepts this availability loss and does **not**
  provide prompt reclamation for this window.
- **Charging a reservation against a NEW connection's Receive Maximum is conservative
  broker policy, not a spec requirement.** MQTT 5.0 §4.9 states that the send quota and
  Receive Maximum "are not preserved across Network Connections, and are re-initialized
  with each new Network Connection... They are not part of the session state". This broker
  counts `len(pendingPublish)`, which is stricter, and deliberately so
  (`qos12InflightForCap`).
- **A transmitted, unacknowledged RETAINED exchange holds its identifier and its capacity
  charge for the rest of the session's life, and enough of them exhaust the session.** This
  is the retained route's own cost and it is separate from the open-section one above. A
  retained send has no JetStream backing (R6), so it can never be redelivered; and a client
  resends only unacknowledged PUBLISH and PUBREL, never a PUBREC (MQTT 5.0 §4.4), so for a
  QoS 2 replay whose PUBREC was lost no acknowledgement will ever arrive either. The
  reservation therefore has **no release edge at all** short of the session ending. A
  subscription matching five retained QoS 1/2 messages over a flaky link can leave a session
  at Receive Maximum 5 unable to deliver any further QoS 1/2 publication indefinitely, across
  any number of same-object resumes. F-101 accepts that in preference to the alternative,
  which is re-minting an identifier a QoS 2 receiver is still holding awaiting PUBREL
  ([MQTT-2.3.1-4]). Closing it needs durable state for a retained send, which F-091
  deliberately did not build.
- **A retained frame whose disposition never runs, or runs after its hold has been swept,
  keeps its identifier.** The release is at the abandonment, and
  `releaseRetainedSendAbandonedLocked` returns early when the hold is gone — rebind sweeps
  a predecessor's owner-scoped inert holds before the displaced connection is closed
  asynchronously, so a late abandonment can find no hold and leave the entry reserved even
  though no bytes were written. Fail-closed and accepted: the alternative is releasing an
  identifier on something other than the proof, which is the defect F-101 removes.
- **F-102 — a recovery reservation over a Lost source has no release edge.** An entry the
  reap legitimately releases as proven-unsent leaves its durable pair behind. If that
  source's consumer is gone, both classifiers return **Lost**, not Complete — deleting a
  consumer on an interest-retained stream deletes what it held — so recovery restores the
  entry instead of consuming it, with its marker and mapping but **no ack subject**. The
  reap only ever scans entries that have one, so nothing re-examines it and no consumer
  remains to offer the source; the client never received this exchange, so its
  acknowledgement is not a release either. Not introduced by F-100: HEAD reaches the same
  state from a strictly wider release.

**Residual, not a defect.** When a message that WAS transmitted is redelivered under a
FRESH identifier — the drain re-offering a completion whose acknowledgement save failed,
after the PUBACK already released the original identifier — its first send under the new
identifier carries DUP=0: that identifier's durable pair records no transmission, and
`drainClaimed` no longer forces DUP (the source having been claimed is not the same fact
as this identifier's PUBLISH having been sent). This is honest per identifier and harmless
to a conforming receiver — §3.3.1.1 says DUP is not a receiver-side dedup signal, and
QoS 2 is protected by the packet-identifier exchange regardless.

**Second residual: a pair whose register delete never lands is re-offered as unsent.**
Recovery's drop loop deletes an unverdicted register fire-and-forget and reaps its marker
only where the sibling reads authoritatively absent, so a register delete that fails
permanently leaves the **pair** intact, and a resume before the sweep restores that
identifier as never-transmitted and re-offers it with DUP=0. The orphan-marker sweep is
not a backstop for this shape: it reaps a marker whose register is absent or belongs to
another exchange, and it likewise preserves a same-exchange pair, so it cannot delete the
marker while the register survives beside it. The consequence is bounded to a duplicate
delivery flagged as a first transmission, which §3.3.1.1 already tolerates (DUP is not a
receiver-side dedup signal) and which QoS 2's packet-identifier exchange protects
outright. Terminal cleanup is the path where this is NOT tolerated — it confirms the
register delete before reaping, and reaps nothing if that delete cannot be proven.

The experimental affine QoS 2 path shares the one derivation site, so it inherits the fix;
its own parked ordering defect (below) is unrelated. See
[F-072](findings/release-investigations.md#f-072). **This does not license a release: F-088
remains an unconditional alpha blocker.**

### Same-topic ordering on the affine QoS 2 path — PARKED EXPERIMENTAL DEFECT

**This remains a demonstrated defect, not a residual or a fixed item.** It is retained as
the experimental implementation record for F-071. The 2026-09-02 scope decision removed
the affine path from the supported alpha, so this is no longer an alpha blocker; it is
gathered under ROADMAP chapter P and becomes actionable only after a decision to invest in
shared QoS 2.

Same-topic QoS 2 messages delivered to a member of a Shared Subscription through the affine
path can reach the client **out of publication order**. This is **not** a bare MQTT
violation: MQTT-4.6.0-5 is conditional on an *Ordered Topic*, MQTT-4.6.0-6 requires ordered
treatment only for **Non-shared** Subscriptions, and §4.8.2 does not extend it to Shared
Subscriptions. Nor does this file promise ordering globally — its only
ordering statement is scoped to the Receive Maximum prompt-release path. What exists is an
**INTENDED implementation property** in `docs/PORTING.md`, stated for EVERY path: each
topic-filter+QoS stream is meant to ride "a single ordered carrier ... on a single dispatch
goroutine". It sits under *Testing strategy*, justifying why three upstream ordering tests
were rewritten, so it is load-bearing for those. The affine path adds a SECOND carrier —
copies through a `$MQTT_Q2AFF_*` consumer on `$MQTT_out` — and so **violates the intention
rather than being excluded from it.** Its parked status is a product-scope decision, not a
claim that the behavior is acceptable or conformant with that intended property.

Reproduced from a **SINGLE publishing Client on one connection**, each QoS 2 handshake
completed before the next was sent — 2 inversions in 120 attempts under concurrent load. In
both, exactly one arbitrary message was transmitted first with DUP=0 while every other
followed in publication order with DUP=1. The evidence is the broker's own packet identifier,
minted immediately before the synchronous register gate and enqueue, on a subscription
`nats.go` dispatches with a single goroutine — so the harness cannot be the source.

An earlier version of this entry rested on a capture whose eight messages came from eight
DIFFERENT publishing Clients, which §4.6 does not order. That claim did not follow from its
evidence and was withdrawn; the single-publisher reproduction above replaced it.

Scope, as far as it is established: the affine shared-subscription delivery path. Not
observed on plain subscriptions, and no claim is made either way about them. Rate is
load-sensitive — 1 in 240 attempts under concurrent `full-suite` load, 0 in 40 unloaded,
which does not establish that load is necessary.

**Cause untested.** Three candidate mechanisms are already REFUTED — an AckWait
redelivery,
a fixture defect, and a register-store race between sibling callbacks — each by evidence
in
the tree. Since a subscription's callbacks are serialized, the reordering must already
exist
in the order JetStream OFFERS the copies. See
[F-071](findings/release-investigations.md#f-071) before proposing a fourth, and do not
weaken the ordering assertion that caught it.

**A passing experimental suite means F-071 did not trigger in that run**, not that this
parked defect is closed. A failure is accumulated under ROADMAP chapter P and does not
invalidate the supported-alpha conformance result.

### Orphan reconciliation retains session-generation state without retirement proof

**F-036 unit 2.** Every orphan-sweep arm that can still destroy live session-generation
state now requires a valid retirement watermark whose generation EQUALS the candidate's,
and without that proof it RETAINS. The reason is that the pass's own evidence cannot
support the destruction: a complete `$MQTT_sess` walk plus this broker's residents
establishes that nothing THIS BROKER can see names the generation, which a session live on
ANOTHER broker with no record satisfies equally — and a resume preserves the generation, so
such a session still uses those exact names. Destroying its state is payload LOSS.

**Three resource classes are retained where no matching retirement is observable:**

- a NONTERMINAL inbound QoS 2 hold on `$MQTT_qos2in`, carrying a full application payload,
  together with the `discarded` fence that is not written for it;
- an absent-generation in-flight register on `$MQTT_inflight`;
- a `$MQTT_PUBREL_*` consumer on `$MQTT_out`, with every interest-retained PUBREL it
  holds.

**The retention is PERMANENT and UNBOUNDED, and it is not conditioned on a rollout.** The
watermark is a single slot per client, so once a later retirement replaces it the earlier
generation can never again be proven retired: retire G1, then create and retire G2, and
G1's state is unreclaimable for the life of the store. A restart, or eviction from the
bounded positive-only memo, has the same effect for a generation whose watermark was
observed but is no longer discoverable. A **valid watermark naming a DIFFERENT generation
authorizes nothing**, which is the ordinary steady state for any client torn down more
than once.

**Cleanup that does NOT need the proof still runs**, so this is not a blanket retain: once
the payload is authoritatively absent, an unchanged exact-sequence release claim is
deleted, and a terminal tombstone is reaped (confirmed delete, then an authoritative
absence read, then the claim). Neither can destroy a payload, and neither eliminates the
pre-existing post-proof paused-releaser residual recorded elsewhere in this document.

**One arm is deliberately UNGATED and out of scope**: `discardAbsentWinnerClaim`, which
commits `Discarded` for an affine winner whose record is absent. It is affine assignment
mutation, and F-036's supported-scope ruling of 2026-09-02 accumulates affine-only
regressions under ROADMAP chapter P rather than widening this finding. The census of gated
arms is today's enumeration and is **not** claimed to stay complete: the structural
inventory guard that would have claimed that was designed, reviewed three times and
deleted, because a primitive-only AST inventory cannot catch a new unsafe CALLER of an
already-inventoried destructive helper.

**The retained population also costs recurring WORK, and only part of that is bounded.**
The per-candidate revalidation round trips are bounded per pass, by a budget with a cursor
keyed by the durable name — a rotation, so a persistent candidate is still eventually
visited under a finite or serviceable churn rate. What is NOT bounded is the discovery
around it: the consumer listing, the candidate collection and the sort all still grow with
the `$MQTT_out` population. That is this sweep's pre-existing shape rather than something
this change introduced — `consumerList` already walks the whole population, the affine
leak accepted below included — and paging that shared enumeration is filed as its own
roadmap item. Fairness against an arrival rate that permanently exceeds the budget is also
not claimed.

**Why this is accepted.** A leak is preferred to a loss, which is the same trade course E
took below. No bounded variant exists from the facts available: a dead generation with an
absent record and a live remote generation with an absent record are observationally
identical to this pass, and waiting longer does not distinguish them. An exact bound would
need permanent per-generation retirement history — trading unbounded payload retention for
unbounded retirement metadata — or a distributed liveness lease with a substantially
larger failure and partition model. No stream gains a `MaxAge` or a message limit for
this, because their bound is implemented by deleting the acknowledged payload.

### The orphan sweep no longer reclaims an affine consumer by absence, and the leak is accepted

**F-036 course E.** `deleteOrphanQ2AffConsumers` was removed outright, so the sweep's
candidate-reclamation route no longer deletes a `$MQTT_Q2AFF_*` consumer. What replaced
it is nothing: the route is gone rather than guarded.

**Scope of that statement, precisely.** It is about the reclamation route, not about
reachability from the sweep. Teardown-marker adoption still runs `deleteTeardownTargets`,
which deletes `t.q2AffDur`, so a sweep pass can still cause an affine consumer delete. That
route is pre-existing and unchanged, and it is **not** claimed to be race-free: a won
`claimTeardown` authorizes proceeding with lifecycle teardown relative to the claimed record,
but it does not fence a stale same-affine-generation peer's `beginQ2AffOp` and create-only
claim CAS, so `discardQ2AffClaimsFor`'s scan can be outrun. That window belongs to **F-036**,
with the proof-to-ack residual below behind its second half.

**Why there is no guarded version.** `$MQTT_out` is interest-retained, so deleting an
affine consumer destroys the acknowledged QoS 2 payloads it holds, and every positive
authorization for that delete was sought and refused. A retirement proof is structurally
unavailable — `recordGenRetirement` writes the **session** generation while the consumer is
keyed by an **affine** generation that outlives incarnations. Consumer metadata has the
same mismatch. `NumPending == 0` is an instantaneous observation, not a fence. Removing
only the absent-record route still lets a stale owner create a fresh nonterminal claim
after the verification scan, because `discardQ2AffClaimsFor` installs no fence against
future claim creation. And the two guards that read as defenses were not: a record re-read
cannot distinguish "no session" from "a live session whose record is absent" (F-036's own
premise), and the `ConsumerInfo.Created` instance fingerprint cannot see an **adoption**,
since provisioning is a create-or-update that leaves it unchanged.

**What leaks is an unbounded CORRELATED population**, not a consumer alone:

1. the `$MQTT_Q2AFF_*` consumer;
2. its interest-retained copies on `$MQTT_out`;
3. the claims on `$MQTT_qos2assign` whose copies remain present. `reapable` requires
   **both** `SourceAcked` and the copy to be authoritatively absent — "a copy that still
   exists keeps its claim, whatever else is proven" — and the old delete is what used to
   make those copies absent. The surviving `discardAbsentWinnerClaim` keeps **creating**
   this population, so it is not merely a pre-change shape.

There is **no automatic consumer backstop**: the affine consumer's `InactiveThreshold` is
deliberately zero and `$MQTT_out` refuses any `ConsumerLimits.InactiveThreshold`, so
nothing ages it out.

**It is not categorical, and the qualifiers are load-bearing.** Successful teardown
(`deleteTeardownTargets` still deletes `t.q2AffDur`), rollback and a successful unwind
remain authorized deletion routes; a consumer reaching one is still collected, and its
claims become reapable **once `SourceAcked` is also proven** — a consumer delete removes
only the copy-side blocker. **Adoption is not a deletion route**, but it is not inert
either: a completed delivery after it (`mqttFinishQ2AffineDelivery`) records completion,
confirms the copy acknowledgement and reaps the claim, and
`settleRetiredAffineRegister` can do the same on its own retired-register evidence. The
claim sweep also revisits such a claim every pass, though it cannot itself remove a
present copy.

**One class has no backstop at all, and it is stated as a class rather than a list**:
every authorized cleanup route that **fails** after dropping the durable or in-memory
reference, with no retry anchor. Its known members are the unused-path unwind's `dispose`
failure arm, `finishQ2AffineUnwind`'s failed consumer delete, and provisioning rollback's
failed minted-consumer delete. All three previously depended on orphan collection; each
now logs that the consumer is retained and must be removed by hand.

**Why this is accepted.** The alternative is an unclosed destruction path for
acknowledged messages, which also manufactured F-064's claim-without-copy precondition.
A leak is preferred to a loss, and F-036 has since taken the same trade for the general
session classes — see "Orphan reconciliation retains session-generation state without
retirement proof" above. **F-036 itself is CLOSED** (unit 1 on 2026-09-02, unit 2 on
2026-09-03); "A retirement refuses rather than disposing under a live owner" below states
what each unit closed and the residuals they left.

### A retirement refuses rather than disposing under a live owner

F-036's unit 1 landed the incarnation completion lease
("The incarnation completion lease" in `docs/ARCHITECTURE.md`), which closes the
**incarnation admission invariant** — including the single-broker clean-start silent
loss of an acknowledged QoS 2 message, where a displaced PUBREL derived its hold
subject under the re-minted generation and answered a success-shaped PUBCOMP for a
payload the same teardown destroyed. The generation is now re-minted only under a
permit proving no client is bound and the incarnation's owners have drained.

**Unit 2 has since landed**, closing the two-dimensional PUBREL classifier and sites 1
and 3 on the lease identity unit 1 supplied, so **F-036 is CLOSED**. It introduces exactly
THREE new residuals: the no-licence half of the deleted-record shape (in the
deleted-record bullet below), the orphan-reconciliation retention leak with its unbounded
discovery cost (its own section above), and the **take-to-durable-handoff window** on the
licensed retirement's Will — an abnormal exit after `mqttTakeWillLocked` has consumed the
Will but before it is published or durably enqueued loses it. That third one is deliberate
rather than overlooked: the Will backstop on that route recovers an exit BEFORE the take
and is a harmless no-op after it, and closing the remainder would need a
claimed/completed handoff with a stable outbox identity, refused as disproportionate for a
window no comparable route survives either — `reparkRefusedDiscard` already documents that
a crash there drops a parked Will, and the eviction worker's guard accepts trigger loss.
Production has no post-take abnormal-exit source on this route, and a real uncaught panic
ends the process. "Three" counts what unit 2 ADDS, not the total: several PRE-EXISTING
residuals in the same area are
unchanged by it and are deliberately not absorbed into it, the most important being the
**post-proof paused-releaser** residual on the inbound QoS 2 release path. That one is
narrowed but NOT eliminated: the classifier catches every retirement already durable when
it runs, so what survives is the suffix after it — a releaser that has passed the
classifier and then pauses can still fan out while a peer completes and reaps the same
exchange. It is stated where it arises, and unit 2 makes no claim on it.

Three stated consequences of the mechanism:

- **A clean-start takeover is REFUSED while the incumbent has work outstanding.** A
  wedged owner — a route blocked in a synchronous socket write, or one waiting out a
  JetStream request's own timeout — holds its
  lease until the drain ceiling, and the ceiling's disposition is to reopen
  admission, leave the incumbent bound with its Will, socket, expiry and disconnect
  state untouched, and refuse the contending CONNECT. A v5 contender is TOLD, with a
  CONNACK carrying reason **0x89** and a Reason String, because the disposition is
  "retry" and a bare socket close would leave that inference to the client; 3.1.1 has
  no such code and gets the close. The client retries
  and succeeds once the work completes. Availability degrades; nothing is disposed
  under a live owner, which is the outcome every earlier attempt at this finding
  foundered on. The displaced socket is deliberately NOT closed by that arm: closing
  it would be a takeover effect with no successor established (F-084).
- **A displaced packet parked at the admission boundary is answered by nothing.** If
  the transition completes, the parked route is cancelled and its connection closed
  without a protocol response — the connection is being displaced, and the client's
  own CONNECT elsewhere is what it is waiting on.
- **A clean session's discard can be DEFERRED past the disconnect.** `[MQTT-3.1.2-6]`
  requires the discard, and it is still performed, but when the incarnation has not
  drained by the ceiling the obligation is parked and completed later — by the drain
  itself or by the next CONNECT for that client ID, which finishes it before doing
  anything else. In that window the session is resident but **non-resumable**: it
  reports no session present and its state is discarded before any reuse. What a
  client observes is a slower CONNECT, never resumed state from a session it asked to
  have discarded.

**A JetStream timeout abandons the WAITER, not necessarily the queued operation.** The
retirement waits for local execution — a route, a delivery callback, a timer fire — and
owes no wait beyond that owner's return. An operation already handed to the transport is
not held back, and what keeps it safe is a property of its target: an incarnation-scoped
or per-consumer-unique name a successor cannot recreate, an exact stream sequence, a
record write whose timeout marks the write uncertain (which refuses the discard until a
readback classifies it), the documented shared-group residual, or ordinary application
semantics for a user subject.

The teardown-owned release of shared group durables is **outside** this boundary and
unchanged: it is retirement work created after the displaced incarnation has drained,
not an effect derived by an admitted displaced route, and the general
scan-then-delete race on a deterministic group name remains exactly the
shared-subscription residual recorded above.

### Shared-subscription QoS 2: affinity holds, against a non-atomic proof

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
`$MQTT_out`'s interest retention deletes the copies it still held, and delivery
of those QoS 2 messages can no longer be PROVEN. Because the affine side of an
assignment is recorded **before** its copy is acked, that outcome is
distinguishable from a *proven* delivery in the durable state alone — no broker
has to have witnessed the consumer disappear — and each affected assignment is
marked `Lost`: a terminal state that is deliberately **not** reapable, so the
record survives for an operator to find instead of being written off as a
completed delivery.

**`Lost` is what the broker can prove, not what necessarily happened**, and the
difference matters to anyone acting on the marking. The write order above is
sound for the JetStream copy acknowledgement and **not** for the client's PUBREC,
which is answered earlier: `mqttProcessPublishReceived` stores the correlated
PUBREL before it reaches `mqttFinishQ2AffineDelivery`. So a client may already
have received and acknowledged the PUBLISH while only the copy acknowledgement is
unproven. That was a reporting defect rather than a deviation — see the paragraph
below on why nothing is redistributed and the client is not deprived — and it is
now **corrected**: the stored PUBREL names the assignment it completes, and
resolving that reference on resume records `PubRecProven`, which outranks `Lost`.
The five residuals that remain are stated below.

**Corrected by F-033's audit (2026-08-18): the marking is not automatic.** `Lost` has
exactly one writer in the tree, `recoverQ2AffClaims`, and it runs on **the owning
session's own resume**. While the record is live and names the same generation, the
orphan sweep skips every claim of that generation by design, so an assignment orphaned
by an out-of-band consumer destruction is marked `Lost` only if that session comes back.
A session that never resumes — which a session expiry interval of `0xFFFFFFFF` permits —
leaves it unmarked. That was **F-064**, and it is now **partly closed**: for an
assignment that never reached `materialized`, a contender taking the group redelivery
restores the destroyed consumer from the winner's record and materializes onto it, so
the source is released without the owner. For an assignment that DID reach
`materialized`, nothing changes — a replacement copy there could become a second
PUBLISH for an exchange already past PUBREC — so its source is still re-offered
forever, because the group consumer sets no `MaxDeliver`. That remainder is F-064's
**residual 8**, and the sentence above still describes the resumed case only.

**F-065, closed: a reaped claim no longer strands its copy — with a re-scoped
guarantee.** What is guaranteed: no reaper deletes a claim on a copy reading older than
its own last write; no actor writes an incarnation-specific fact onto a claim
incarnation it did not observe; and every copy a materialization attempt proves present
is withdrawn once that attempt proves no claim of that copy's own incarnation remains to
authorize it.

What is NOT guaranteed. This is a RE-SCOPE of the same kind F-063's re-scope
records. The withdrawal is a **compensation, not an arbitration** — the claim lives on
`$MQTT_qos2assign` and the copy on `$MQTT_out`, and JetStream cannot commit a write to
both together (its atomic batches are per-stream), so no claim-side state can arbitrate a
copy-side store. The compensation can therefore be lost three ways, and in each the copy
is RETAINED rather than collected:

1. the attempt crashes between proving the copy present and withdrawing it;
2. the withdrawing delete is unconfirmed — **whether it failed definitely or
   indeterminately**, because no confirming read follows it;
3. the attempt's final claim observation is itself indeterminate, which retains the copy
   by design rather than guessing at it.

Two later events remove such a copy: the winner's ordinary session teardown, which
deletes the affine consumer and takes its retained copies with it; or, where a LATER
incarnation now holds the claim subject, the delivery gate disposing the superseded copy
on its next redelivery — a positive fact about a record just read, not an inference from
an absence. **A third used to exist and no longer does**: an orphan-sweep pass deleting
that consumer once no session record referenced it, removed by F-036 course E because no
positive fact could authorize it. Where neither of the two occurs — a session that keeps
running, never resumes, and whose claim subject no later incarnation claims — the copy is
retained until an
operator removes it or F-066's stream-identity remedy supplies the fact that would
license collecting it. A retained copy is invisible to the client: it is never
published, because the delivery gate refuses a copy no live claim of its own incarnation
authorizes.

**That residual is now CLOSED, and what replaced it is narrower.** On the
successful-PUBREC path, if the incarnation-fenced `AffineDone` write fails, the affine
copy remains unacknowledged and the register is durably rewritten as retired; the
immediate source-state probe sees the copy still live and removes neither. For a client
that completes PUBCOMP, disconnects before AckWait redelivery and keeps a non-expiring
session that never resumes, the live record used to preserve all three — the
`$MQTT_qos2assign` claim through its affine reference, the retired `$MQTT_inflight`
register through its separate session-generation reference, and the copy on the
referenced consumer — unbounded in count and bounded only in kind. Orphan reconciliation
now collects that triple: the register is durable evidence of a successful PUBREC, so
under a live reference the sweep records `AffineDone` from it, deletes the copy at its
exact sequence with a confirmed delete, reaps the claim and deletes the register last.
Terminal claims of a live generation are likewise reaped rather than left to an owner
resume that may never come.

**What remains open in that area, stated with its direction.** Two populations:

- **A `materialized` assignment whose copy is gone** still waits for its owner. That is
  F-064's residual 8, unchanged, and re-storing a replacement copy is refuted twice in
  the dossier.
- **A NONTERMINAL claim of a live generation whose source is authoritatively gone** is
  reconciled only by its owner's resume, and that is a proof rather than an omission. No
  fact an owner-independent pass could write there is both true and terminal:
  `AffineDone` would assert a delivery that never happened, `Lost` is unprovable without
  the payload and is never reapable, `SourceAcked` alone leaves the claim nonterminal,
  and `Discarded` is false while the session is live — and committing it makes a
  concurrent materializer withdraw an unpublished copy, because that materializer stores
  the copy BEFORE its own phase CAS, so no phase guard can see the store. So the claim,
  and any copy a late materializer gives it, are retained until the owner returns.

**A residual on the owner's own path, narrowed rather than closed.** `recoverQ2AffClaims`
does settle that nonterminal population, and its window is the final copy read to the
CAS — bounded RETRIES, not one round trip, since the transition may repeat load-and-CAS
up to three times. Within that window a dispatched materializer can store a copy the
transition then discards. The ordering (source read first, copy read LAST) and a
retry-time phase veto narrow it; nothing closes it, because the claim and the copy live
on different streams and JetStream commits to one stream at a time. It is narrowed
further in practice by `moveStartedSharedQoS2` dropping a message whose Message Expiry
Interval has elapsed BEFORE it elects or stores anything, so the actor that could store
the copy refuses to when the source expired.

**And a stated limit on shape refusal, in the direction it actually runs.** Every checked
transition and reap this reconciliation performs refuses a record failing
`q2AffShapePrecheck` — the sweep shape validator, the generation floor and the
direct-decision identity floor — inside the CAS loop, before any mutation. Those reject
the enumerated structural defects; they do NOT reconstruct the order in which this
broker's writers may set these fields. A self-consistent record that no supported writer
emits may therefore pass and be interpreted. Under the compatibility boundary no such
record exists, because no release produced one. The converse is not assertable: an
invariant of the form `AffineDone ⇒ materialized` would refuse a LEGITIMATE record, since
the delivery gate's `Discarded` arm records `AffineDone` before its phase arm is
reached.

**F-063, closed 2026-08-19: the broker no longer releases a source against a store
response.** Because `$MQTT_out` is interest-retained, a copy stored after its consumer is
gone succeeds and keeps nothing; the broker used to acknowledge the group source on the
strength of that store, so the message was released from the shared group having never
existed on any consumer, and the distinguishing write order above could not help because
no delivery had run. Every route to that acknowledgement now proves the copy with a
presence check on the copy subject and authorizes the release from
`copyPresent ∨ AffineDone ∨ Discarded ∨ Lost ∨ SourceAcked`, refusing otherwise. A claim
left nonterminal by that refusal is **F-064**'s to resolve; refusal preserves the only
recoverable asset, which is the unacked group delivery.

**The stated residual: a consumer deleted between the final proof and the ack.** The
guarantee is conditioned on a fresh but **non-atomic** observation, and that wording is
exact rather than cautious. The claim read and the copy read are separate JetStream round
trips, the copy lives on a different stream from the claim, and JetStream's
`CONSUMER.DELETE` carries no precondition field at all — so no finite check binds copy
existence to the acknowledgement that follows it. The interleaving, worked:

1. A broker proves the copy present on `$MQTT.out.q2.<hash>.<gen>.<claim-hash>`.
2. On a claim that was not already `materialized`, the broker then commits the
   `claimed`→`materialized` CAS — a load-and-CAS with a three-attempt retry budget
   (`mqttQ2AffClaimAttempts`), each attempt a further round trip.
3. An operator, or any other client of the JetStream API, deletes
   `$MQTT_Q2AFF_<hash>_<gen>`. Interest retention takes the proven copy with it.
4. The broker's `confirmAck` commits, irreversibly releasing the group source.

**The window spans step 1 to step 4, and step 2 is part of it.** How much of step 2
depends
on the route, and only ONE route omits it entirely:

- The contender's foreign-materialized arm never enters `materializeQ2AffCopy`, so its
span
  really is the copy lookup and then the ack.
- An owner re-verifying an already-`materialized` claim still calls `advanceQ2AffClaim`,
  which **loads the claim** before finding the phase unchanged and skipping the write. The
  CAS is a no-op; the load is not.
- A fresh or on-behalf materialization pays the full load-and-CAS, retried up to three
  times.

An earlier draft of this section wrongly called the whole thing "one round trip that
nothing inside the broker can widen", and its first correction still wrongly gave the
already-materialized owner route the ack alone. What the
change actually replaces is an acknowledgement resting on a store response, or on evidence
gathered before the store, or on the phase alone; what it does not do is make the span
zero. Such an assignment is left `materialized` with no copy and no `AffineDone`, which is
exactly the state `recoverQ2AffClaims` records as `Lost` on the owner's next resume — so
the outcome is retained as evidence rather than laundered, subject to the resume caveat
above (F-064).

That discrimination holds **while the owning session record still exists**. If the
record disappears first — the session expired, or a broker died without tearing it
down — a legitimate teardown deleting the consumer and an out-of-band destruction
of it look identical, so orphan reconciliation records `Discarded` (which
MQTT 5.0 [4.8.2] permits) rather than `Lost`. A `Lost` flag already committed is
never automatically removed except by a `PubRecProven` a surviving PUBREL still
licenses (below); what cannot always be recovered afterwards is a loss
that occurred before the record vanished and that no broker had yet recorded. A
discarded assignment is itself retained until the source is proven closed, so
reaping it can never let a redelivery elect a second winner.

**What remains in this section**, therefore: the out-of-band consumer destruction above,
whose `Lost` marking depends on the owner resuming (F-064's residuals 1 and 8), and
F-063's non-atomic span between the final copy proof and the source ack. Both are stated
with their mechanism rather than a bound they do not have.

**The false loss report is CLOSED, and what remains of it is stated here.**
`mqttProcessPublishReceived` stores the correlated PUBREL before it records
`AffineDone`, so PUBREC → broker stop → out-of-band consumer destruction leaves
`materialized ∧ ¬AffineDone ∧ copy absent` for a message the client already answered.
The owner's resume used to record that as `Lost`, which was untruthful — never a
deviation, since `Lost` releases the group delivery without re-offering the message and
the handshake completes from the independently persisted PUBREL, but wrong in what it
told the operator, and it left a claim that was never reaped. (Writing `Discarded`
instead was never available as a shortcut, and the reason is phase-sensitive: where a
correlated PUBREL proves PUBREC it is merely untruthful, but for the undifferentiated
shape, where nothing proves the client was reached, it would deprive a client still owed
the PUBLISH and `[MQTT-4.8.2-4]` does bite.)

The stored PUBREL now names the assignment it completes, as a bound
`(claim subject, copy subject)` pair, and `reconcileQ2AffPubRelProofs` resolves that
reference on resume before any loss can be inferred. A resolved reference records
`PubRecProven`, which outranks `Lost` in either arrival order, so even a `Lost` that won
a cross-broker race is corrected. PUBCOMP will not acknowledge such a PUBREL away while
its proof is unrecorded, and it keeps the packet identifier reserved while it refuses. A
duplicate PUBREL record under an identifier already tracked by a different record is
discharged before it is acknowledged, for the same reason. The loss diagnostic is emitted
only for a verdict that actually landed, so a proof winning the race no longer produces a
loss report with no loss behind it — and a proof that arrives AFTER a verdict was recorded
and logged retracts that report explicitly, since it also reaps the record that would
otherwise contradict it. The loss report itself is worded as provisional and the two lines
carry no guaranteed order, because the record rather than the log is the authority.

**Five residuals remain, each with its mechanism rather than a bound it does not have.**

- **A whole-bundle reset destroys the proof.** The PUBREL and the copy identity it
  names live on `$MQTT_out`, so a reset takes both, and a surviving `Lost` on the claim
  can no longer be corrected. Recreating `$MQTT_out` ALONE is no longer a supported
  operation and is refused at startup (F-066); what remains is the accepted-data loss a
  reset causes by construction, which coupling never fixed.
- **The reconciliation is not atomic with the consumer deletion.** Teardown records every
  stored PUBREL's proof before deleting the consumer that retains them, but a broker still
  serving the retiring generation can store a proof-bearing PUBREL AFTER that scan's last
  read and BEFORE the delete. That record dies with the consumer, and a `Lost` already on
  its claim is then uncorrectable. The window is narrow — it needs a stale broker
  completing a PUBREC inside the teardown's own delete interval — but it is real, and it
  is the ordinary late-proof case rather than the malformed-reference one below.
  **Closing it requires a durable mechanism, and a cheap one was refused rather than
  shipped.** A store-then-verify read of the generation-retirement watermark is REFUTED and
  must not be re-proposed: `$MQTT_teardown` holds one replaceable record per client and
  `recordGenRetirement` advances on `MarkerSeq` alone, so a teardown of one generation can
  run while the slot names another; the verifying read is not crash-persistent; and the
  arm it would take does not clear an existing `Lost` anyway. Three candidate closures
  remain, each its own piece of work: a stale-serving predicate for the retiring AFFINE
  generation — which F-036's closure does NOT supply, since its licence names the SESSION
  generation and the affine one outlives incarnations — after which the existing
  scan-before-delete suffices; a
  retention handoff that keeps proof records independent of the consumer; or a claim-side
  "PUBREL owed" intent with register-driven recovery.
- **A teardown carrying a MALFORMED carrier still loses it.** Session teardown now
  reconciles every stored PUBREL's PUBREC proof into its assignment BEFORE deleting the
  consumer that retains those records, and fails closed on a walk it could not complete or
  a claim it could not read — so the ordinary late-proof-then-teardown sequence no longer
  strands a `Lost ∧ Discarded` claim. The exception is a reference this broker does not
  write: it names no assignment that can be identified, so nothing can be recorded and no
  retry could ever succeed, and failing closed there would wedge the session's teardown
  forever. Such a record is destroyed with its consumer, with a diagnostic. The claim it
  could not name is `Discarded` regardless, which MQTT 5.0 [4.8.2] permits.
- **A permanently unreadable claim, or a malformed carrier, defers rather than
  resolves.** Recovery writes no loss verdict for that incarnation at all — an inference
  drawn from a search that failed is a guess — and PUBCOMP withholds its acknowledgement,
  holding one packet identifier and one Receive Maximum slot until the record becomes
  readable. Fail-closed, and an availability cost rather than a correctness one. Only
  this broker writes those headers, so a malformed one is corruption or direct backend
  manipulation, never something a client can send.
- **An authoritatively ABSENT claim is acknowledged, not withheld.** That is not
  fail-closed and is not claimed to be: with no record there is nothing for a later
  resume to report untruthfully, and withholding would hold the exchange forever on
  something that can never reappear.

**Residuals of on-behalf restoration itself.** A contender restoring a destroyed
consumer refuses, storing nothing and acking nothing, on: a record whose `Q2Aff` is not
exactly the canonical provisioning shape or whose variable fields are out of bounds; a
record carrying a teardown marker; an indeterminate consumer lookup or stream read; a
nonzero `ConsumerLimits.InactiveThreshold`; an unconvergeable immutable mismatch; and a
second storage conflict after one bounded retry. The owner's own recovery
additionally declines to retry while `$MQTT_out` carries a nonzero
`ConsumerLimits.InactiveThreshold`, because the retry would arm a deletion timer on
a durable the refused create had left untouched — so an operator who mutates that
limit after startup keeps today's inert provisioning failure rather than gaining a
reapable durable.

**That read is not atomic with the retry, and the remaining window is stated here
rather than narrowed again.** It requires all three of:

1. the owner's initial create meets the exact `MemoryStorage` immutable conflict
   against an existing affine durable, so the recovery is entered at all;
2. `$MQTT_out`'s `ConsumerLimits.InactiveThreshold` changes from zero to a
   **positive** value after the recovery has read the stream and before its retry
   is applied — which `validateOutStream` refuses at startup, so it can only be a
   stream mutated afterwards;
3. the retry is applied server-side, arming the durable's deletion timer, but its
   response is lost, so the recovery still reports failure.

If the client then stays disconnected through that threshold, the durable and the
copies it holds are reaped. **This is the same fate an ordinary owner create
already meets under that post-startup mutation** — it is not a new class of
debris. What the window removes is an accidental preservation that the storage
conflict itself was providing, and which came bundled with a session that could
never resume. Closing it is not available: JetStream's consumer create carries no
precondition, so nothing can fence the update against a concurrent stream change.

Three of the contender-side refusals leave debris worth naming. The teardown check
narrows but does not close its race, because consumer
creation has no precondition on the record's sequence, and **nothing reclaims a
recreated affine durable by reference any more**: F-036 course E removed
`deleteOrphanQ2AffConsumers`. A stream update landing
between the inactivity pre-check and the create yields a nonzero effective threshold
that the returned-config arm refuses; a positive one leaves a durable carrying a
deletion timer, which that same limit later reaps, while a **negative** one leaves a
timerless durable that nothing reaps on a threshold, since there is none — ordinary
teardown deletes it, and there is **no sweep fallback behind that**: if the session
never reaches an authorized teardown the durable is retained until an operator removes
it, which is the course-E leak stated above. And restoration can still
keep nothing while `$MQTT_out`'s configured duplicate window has an entry for the
copy's stable `Nats-Msg-Id` from an earlier no-interest store; the validator waives
`Duplicates`, so that bound is operator-configured.

Not this, despite the heading this section carried until F-063 closed:

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
  which affine generations are still referenced, and `reconcileOrphanQ2AffClaims` is
  suppressed for every session in the account. (It also suppressed affine consumer
  deletion until F-036 course E removed that route; the retired affine **register** arm is
  not behind the gate.) Since
  nothing removes the record, that is permanent, and it is logged as a warning naming
  the recovery on every pass. **Inbound QoS 2 holds, outbound in-flight registers and
  PUBREL consumers keep being reclaimed**: the record's identity and generation are
  sound, so the references it contributes to those classes are complete. A record
  whose *identity* cannot be established — undecodable, unattributable, or carrying a
  malformed `Gen` — is the stronger case and still aborts the whole pass. Either way
  the recovery is the same: stop the account's brokers and reset the bridge-owned
  `$MQTT_*` streams — all nine, which are one recreation unit.

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
  - A filter subscription restoration that fails no longer leaves an accepted
    session behind it: restoration runs before the success CONNACK and a filter
    it cannot install **refuses the CONNECT** (F-050, see
    `docs/ARCHITECTURE.md` "The restore gate"). The residual that used to sit
    here — a filter skipped after the CONNACK, swept, and unavailable until the
    next CONNECT — is retired with it. What replaces it is narrower and is
    listed below: a filter that can never be restored refuses every resume.
  - **A filter that can never be restored refuses every resume.** The reachable
    cause is a subscribe permission revoked since the filter was saved: the raw
    subscription install fails deterministically, so each `Clean Start = 0`
    CONNECT is refused. That is deliberate — the alternative is a Session-Present
    connection that silently never delivers, which is the defect — and it is
    recoverable two ways: the operator restores the permission, or the client
    connects with `Clean Start = 1`, which discards the session and succeeds. A
    v5 client is told the reason (0x83, naming the filter); a 3.1.1 client sees
    only the close. Each refused attempt also restarts the session's expiry clock
    from its own close, as any ended network connection does, so a client that
    keeps retrying keeps the session alive.
  - **A shared QoS 0 message can be swallowed during a resume's restoration.** A
    restored shared subscription holds NATS queue-group interest from the moment
    it is installed, and nothing may be transmitted until the verdict OPENS, so a
    QoS 0 group message selected for that member in the window is dropped where
    another live member would otherwise have received it. QoS 0 only, at-most-once
    and best-effort. The window is the pre-CONNACK restoration — O(filters)
    JetStream round trips and, under auth passthrough, possibly one flush per
    filter — **plus the interval between the CONNACK write and the open**, which is
    a channel close and therefore negligible but not zero. The window ends at the
    open rather than at the CONNACK deliberately: opening first would let a woken
    delivery race the CONNACK write and reach the client ahead of it, violating
    [MQTT-3.2.0-1]. The alternatives were worse: buffering is a new unbounded
    mechanism, a NATS subscription's queue group cannot be joined late, and
    installing the subscription outside the verdict makes a QoS 0 PUBLISH the
    client's first packet, violating [MQTT-3.2.0-1]. For a **plain** filter the
    same drop happens today, because no raw subscription exists in that window at
    all.
  - **A QoS 1/2 delivery withheld by a REFUSED resume can lose its
    never-transmitted marker to a rebind, and then waits out AckWait.** (The
    marker loss and the delayed redelivery are current; the **DUP consequence is
    HISTORICAL** — see the closing note on this residual.) The refusal wakes every
    withheld delivery so it can record itself, but it then returns and releases the
    session's ownership gate, so a reconnect can bind before a woken callback runs. That
    callback is stale — the marker is scoped to the connection that withheld the delivery
    — so nothing records it. The message stays owed and is redelivered, but it **waits for
    AckWait instead of being NAKed promptly forward**. Its first client-visible
    PUBLISH used to carry DUP = 1; **that half is CLOSED (F-072, 2026-09-09)** and
    is stated here as the historical diagnosis. DUP is now derived from the durable
    unsent marker at one site, so a stale woken callback can no longer make a first
    transmission carry it. **The lost prompt NAK and the AckWait delay remain open**
    and are this residual's live content, as do the ordering residual and the
    displaced-broker window below.

    Two remedies were built and withdrawn across three review rounds: draining the
    woken callbacks (a counter is not an admission barrier — a callback can be
    descheduled before it registers, or arrive after the drain saw zero, and
    delivery interest stays live until teardown), and deciding on the durable
    rather than the connection (removes the race but not the lateness, and the set
    of places a durable can live is neither complete nor sequence-precise). Closing
    it needs a real callback-admission boundary, which this item does not justify.

    **F-072 owned the DUP half and CLOSED it (2026-09-09)** — deriving the MQTT
    flag from the JetStream delivery count was its defect, and DUP now follows the
    durable per-identifier send-intent fact; see "Outgoing DUP on a first
    transmission — CLOSED (F-072)" above. What remains here is **not** that: it is
    the lost prompt NAK, stated as F-050's own residual. The population is new to this change:
    before it, a restore failure never refused, so no marker was owed on that path.
  - **A shared QoS 1/2 delivery loses that marker even without the rebind race.**
    The ordinary close of the refused connection runs `purgeSharedPending`, which
    removes withheld markers for shared durables along with the pending entries and
    their identifiers — deliberate behaviour that stops a resume NAK-stealing group
    messages. Kept as a separate statement from the race above because it does not
    depend on any interleaving. Fixing it means changing what a disconnect does to
    shared pending state, which is not F-050's.
  - **A timed-out affine gate wait fell into F-072's population, since CLOSED.** An
    affine QoS 2 copy that reaches its callback before the CONNACK waits, and a wait
    that runs out returns the copy unacknowledged. **The diagnosis that follows is
    historical.** The affine path used to derive DUP from the JetStream delivery
    count, so the redelivery could carry DUP on a first transmission — which was
    F-072's defect and is **CLOSED (2026-09-09)**. DUP is now the durable
    per-identifier send-intent fact at one derivation site, and no path derives it
    from the delivery count. The wait itself still stands; waiting rather than
    dropping is what keeps the ordinary case out of the population at all.
  - **A displaced broker can acknowledge and serve, and the window has no upper
    bound.** Nothing at the CONNACK proves this broker still owns the record: the
    accept CAS arbitrated one instant, restoration only aggregates local failures,
    and an unchanged lifetime, an already-recorded affine consumer and ordinary
    shared restoration all write nothing. So a peer can take the record while this
    broker restores, and this broker still sends its CONNACK and serves until the
    eviction closes it. Moving restoration before the CONNACK does not change that
    consequence — under the old order the same interleaving served the client from
    an earlier CONNACK onward, and one interleaving strictly improves, since a
    displaced broker whose restoration fails now refuses instead of silently
    dropping a filter. What it does add is one QoS 0 loss shape inside that
    window: a group message selected for the displaced member is dropped rather
    than transmitted to its stale connection. Convergence rests on the
    session-record watch and the record-state backstop; the backstop's interval
    (15 minutes by default) is the **first opportunity**, not a bound — a busy or
    refused pass defers the decision to a later one. F-036 owns the consequence
    and its remedy is what would bound it.
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
- **A drain carrying a past-PUBREC intent holds its filter (F-094).** A QoS > 0
  re-SUBSCRIBE to that filter is answered with `0x80` until the drain is retired,
  because adopting it would drop the drain's per-sequence state and let the
  still-unacknowledged source be re-offered as a fresh PUBLISH of an exchange the
  client has already answered. A **QoS 0** re-SUBSCRIBE is unaffected, and a drain
  holding only PUBLISH-phase claims is still adopted normally.
  - **No backlog retention is guaranteed during the refusal.** An unclaimed fresh
    delivery that reaches the drain callback is acknowledged outright and lost; but
    a barrier-skipped drain has no source subscription and a saturated
    `MaxAckPending` can stop messages reaching the callback at all, so stored
    messages may equally accumulate. The broker promises neither outcome.
  - **The release is retirement, not a client action.** A failed PUBREL recreate or
    an incomplete resume scan defers it to another resume, and a client that never
    acknowledges its remaining drained deliveries can hold the filter refused for
    the life of the session — though only where those claims also survive
    reconciliation, since a resume that proves a source absent prunes the claim and
    retires the drain with no client action at all. An **unusable-identity** hold is
    operator-only and no resume repairs it.
  - **At fork repair the refusal is not retried.** A held sid refuses the two
    destructive rows of the transition table, and the merge counts the whole filter
    as a conflict, so the candidate change — including otherwise applicable
    companion and option changes — can remain unapplied once the accept save lands.
    The client must re-issue the operation. Same residual class as the existing
    `draining -> draining, differing` conflict.
- **Drain retirement proves disposal before releasing the sid (FIXED), with three
  stated residuals.** Retirement now releases the sid and closes the delivery path
  under **one** `sess.mu` hold — `sub.mqtt.closed` is set together with the map drop —
  so an already-dispatched delivery callback cannot pass its checks after map removal
  and reach ordinary PUBLISH processing. That closes the disposal route to the same
  duplicate PUBLISH F-094 closed through adoption. The drain object is **terminally**
  marked, so neither a SUBSCRIBE rollback's shallow snapshot, nor the resume's restore,
  nor the two-map re-insertion helper, nor an inactivity-timer clear can hand it back to
  this session object. The consumer delete is issued with a wait and **classified**:
  deleted, confirmed already absent, or unknown. Reconciliation's direct consumer-absent
  removal refunds its local consumer charge on a proven-landed save.

  Three residuals remain, and each is a real limit rather than a restatement:

  - Reclamation of a durable whose delete could **not be confirmed** is *eventual*, not
    prompt: the orphaned-consumer sweep reclaims it on a later pass that completes its
    consumer listing and its record walk and whose own delete succeeds, and it
    deliberately spares a consumer that any session record — a fork sibling's included —
    still names. There is no retry loop and no reaper of its own. The unknown outcome is
    reported so an operator can see it. The consumer's still-pending messages, both
    re-offers of exchanges the record no longer claims and fresh messages on a filter the
    client unsubscribed from, go with it; no client is owed either, and deleting the
    consumer does not destroy a message another consumer still holds interest in.
  - Terminal retirement is scoped to the retiring **session object**, not to the
    incarnation: a record restore builds a new session object while preserving the
    generation, so a fork sibling's record that still names the drain can rebuild it on a
    fresh `*mqttDrain`, and a delete the retiring object issued can land on that rebuilt
    consumer. This is **pre-existing and unwidened** — the previous unconfirmed delete had
    the identical exposure, and confirming the request adds no reconstruction route and no
    deletion authority. Closing it would need a retirement fact a *sibling's* record could
    see, which no record of ours can supply.
  - An inactivity-timer clear already in flight when a drain is retired can land after the
    disposal, recreating the durable under a retired name. It is absent from the retiring
    object's maps and delivery path, so that object delivers nothing on it, and the same
    sweep reclaims it once no resident object and no session record reference it — under
    both qualifications above.
- **A live SUBSCRIBE reclaims a drain that cannot deliver.** A resume can leave a
  drain with no delivery subscription — CONNECT withholds source interest from
  **every** QoS 2 drain when PUBREL subscription setup fails, including drains holding
  only PUBLISH-phase claims that no hold covers, and a restore whose own
  `processQOS12Sub` failed leaves the same shape. A QoS > 0 re-SUBSCRIBE over such a
  drain now installs its delivery subscription and adopts it in one step, with the
  terms the client asked for, or answers the filter `0x80`. Previously the filter was
  granted and then delivered nothing for the rest of the connection, with no log line
  and no reason code.

  The **barrier-skipped** route once listed here was never live and the claim is
  withdrawn: every durable in the PUBREL barrier's skip set is intent-bearing by
  construction, so F-094's hold refuses its adoption for as long as the drain object
  exists.

  The residuals below are deliberate. Two of the original three are now **CLOSED**; what
  remains is filed as its own release-blocking finding rather than fixed here, alongside one
  residual this repair introduces:
  - ~~**A filter that fails after adopting can strand its durable.**~~ **CLOSED for the
    absent-membership case**, which was the release-blocking one, and it was worse than
    "strand": between the adoption and the packet's final update the record named the
    durable **nowhere**, so a sibling broker's orphan sweep deleted a live session's
    consumer with its started deliveries — no crash, no failure, same-version peers, on an
    interest-retained stream. An adopted drain is now **retained and named continuously**,
    in `Cons` where the membership survives and in `Drains` where it does not, and
    `Drains` omission is authorized only by an owned retirement transition rather than
    by a claim count. The post-adoption claim loss is closed too: a restored drain's
    persisted `Started` set is a **floor**, and a delivery a recovered register
    evidences as possibly attempted is enrolled rather than acked away — which also
    stops its packet identifier being held for the life of the session with no
    delivery left to discharge it. Mechanism in `docs/ARCHITECTURE.md` under "An
    adopted durable is named continuously (I8)" and "A restored drain's claim set is
    a floor".

    **Still open, and deferred to its own `docs/ROADMAP.md` §0.3 bullet:** the **CONNECT
    restore** route, where a persisted QoS 0 membership over a live `Cons` entry reaches
    `execDeferredConsumerDelete` **inline** during restoration and destroys the
    outstanding exchanges. That is the mirror case — where the membership key is absent the
    record forgot the durable, and where it is present at QoS 0 the restore destroys it —
    and it needs a restore-side persistence and completion contract of its own.
  - **An already-empty drain reference whose removal write definitely failed is retried
    only by the next resume.** `recoverDrains` revisits it before its no-plans return,
    subject to that pass's own eligibility and skip guards, but there is no periodic pass:
    a connection that never resumes **can** hold that reference, and its consumer, until a
    later adoption or the session's destruction ends it. The failure mode is a retained
    reference, never a lost message.
  - ~~An already-started QoS 2 exchange reclaimed by a QoS 1 re-SUBSCRIBE is
    retransmitted at QoS 1.~~ **CLOSED.** A started exchange's QoS is now a property of
    the exchange: it is carried on the pending entry, sourced from the in-flight
    register's own QoS on recovery, and read under the lock that admits the send rather
    than recomputed from the subscription. The population was larger than this entry
    said — the plain existing-consumer refresh reaches it with no drain, no UNSUBSCRIBE
    and no reclaim — and both directions are fixed. See the exchange-QoS section of
    `docs/ARCHITECTURE.md` for the mechanism and for what it does **not** cover.
  - **A closed-but-present delivery subscription under a live `cons[sid]`.** A QoS 0
    downgrade closes the delivery while deliberately keeping the consumer entry until the
    packet's **final-update boundary** — later than any per-filter occurrence — so one
    SUBSCRIBE packet carrying the same filter at QoS 0 and then at QoS 1 reaches the
    existing-consumer arm with a closed subscription and a deferred delete still scheduled.
- **Scope.** A drain survives because the brokers that read the `Drains` record field
  exclude its durable from the orphan sweep's candidates and hand it to teardown as
  this session's own — and that is every broker serving the account, since they all run
  this tree (see the Compatibility boundary). It is scope rather than a rollout
  condition: a broker that did not read `Drains` would let its orphan sweep delete a
  drained durable and lose its started messages, and there is no runtime capability
  gate that would stop it. Same class as the affine scope above.
- An unbounded drain is deliberate: a client that never acknowledges holds one
  durable per drained filter until its session ends.

Shared-subscription QoS 1 redistribution to another member remains permitted. The
QoS 1/2 → QoS 0 re-SUBSCRIBE downgrade is **no longer out of scope**: it replaces a
subscription rather than removing one, and it now drains that subscription's started
deliveries exactly as a removal does — see "A QoS 0 re-SUBSCRIBE drains rather than
destroys" below.

### A started exchange keeps the QoS it was first transmitted at

A subscription's maximum QoS is an input to a message's **first** transmission and to
nothing else. A re-SUBSCRIBE may replace a filter's maximum QoS at any time
([MQTT-3.8.4-3]) and must not interrupt a publication already in flight, so once
transmitted a message's QoS is a property of that exchange and every retransmission
carries it unchanged. Both directions are honored: a downgrade no longer abandons a
QoS 2 handshake, and an upgrade no longer drives a PUBREC whose slot transition the
CAS refuses.

**Three limitations are stated rather than implied.**

- ~~**A QoS 1/2 → QoS 0 re-SUBSCRIBE is not covered.** That path does not retransmit at
  all: it closes delivery, purges pending exchanges and registers, and deletes the
  consumer, destroying outstanding exchanges outright.~~ **CLOSED for a live
  SUBSCRIBE.** That path is teardown rather than transmission, so it was never the
  exchange-QoS fix's to widen; it is fixed instead by making the downgrade **convert its
  consumer into a drain** — see "A QoS 0 re-SUBSCRIBE drains rather than destroys" below
  and `docs/ARCHITECTURE.md` under "UNSUBSCRIBE drain". Two residuals remain and are
  stated there, not here: a stream-imposed consumer inactivity limit, and the **CONNECT
  restore** path.
- **The replacing allocation's lifecycle is not covered.** Where a register for the
  same delivery already exists, written by another broker, an admitted replacement
  **preserves the occupant's QoS** and a successful confirmation threads the QoS the
  record actually landed holding back onto the pending entry. That does **not**
  guarantee the inherited exchange survives concurrent cancellation or failed-store
  settlement: those are pre-existing lifecycle defects of the replacement arm and are
  consolidated into **F-088**'s bullet in `docs/ROADMAP.md` §0.3, which is the single
  authoritative home for that seam and remains an unfixed release blocker.
- **Shared-subscription QoS 2 is not covered.** An effective-QoS-2 shared delivery
  branches into the affine move before the identifier is allocated, so that route is
  untouched. Shared subscriptions are capped at granted QoS 1 for the alpha, so the
  route is outside supported scope.

### A QoS 0 re-SUBSCRIBE drains rather than destroys

[MQTT-3.8.4-3] requires a SUBSCRIBE carrying a topic filter identical to an existing
non-shared subscription's to **replace** that subscription, and [MQTT-3.8.4-4] forbids
interrupting the flow of publications. So a re-SUBSCRIBE at QoS 0 over a live QoS 1/2
subscription may neither be refused nor made to wait, and the QoS 1 or QoS 2 exchanges
the broker has already transmitted on that filter must still complete.

They now do. The downgrade converts its consumer into a **drain** instead of deleting
it: the durable and its messages survive, the started exchanges are retransmitted and
completed under their original packet identifiers, and the QoS 0 filter is served by the
raw subscription — so a fresh message reaches the client once, at QoS 0, while the drain
discards its own copy. A QoS 2 exchange already past PUBREC completes through the drain's
own PUBREL machinery. The membership at QoS 0 and the drain are written in **one** record.

**What it does not claim, each stated rather than implied.**

- **A stream-imposed consumer inactivity limit disables it.** A drain must outlive an
  offline session, and `ConsumerLimits.InactiveThreshold` on the messages stream gives
  every consumer a delete timer it cannot opt out of — reaping a drained durable takes
  its started deliveries with it, because the stream is interest-retained. Where one is
  set, or cannot be read, the downgrade falls back to the destructive teardown and logs
  what it costs. The identical limitation applies to UNSUBSCRIBE and is stated above.
- **The CONNECT restore path is covered too, by a boundary of its own.** A session can hold
  a QoS 0 membership *and* a live consumer entry for the same filter — what a failed drain
  adoption leaves behind. Resume reconciliation skips QoS 0 memberships, so the restore used
  to reach the existing-consumer arm and delete that consumer **inline**, with its started
  exchanges. It now defers instead, and a restore-side boundary converts the consumer
  into a drain and makes that durable before anything is torn down; `restoreDrains` then
  gives the drain source interest as it does for any record-restored one. A
  stream-imposed consumer inactivity limit disables it there exactly as it does for the
  live path, and the destructive teardown stands. The drain keeps QoS 0 — the value its
  membership actually has — and the retransmission takes each exchange's QoS from that
  exchange's own in-flight register, so a QoS 2 exchange completes through PUBCOMP under
  a QoS 0 membership. See "A CONNECT restore is the third entry point" in
  `docs/ARCHITECTURE.md`.
- **Missing exchange-QoS evidence on a QoS 0 drain.** For a claimed drained sequence whose
  drain delivery QoS is zero, if no identity-matched pending PUBLISH entry supplies nonzero
  exchange QoS, the PUBLISH callback withholds before allocating a packet identifier or
  storing a new in-flight register. This refusal does not acknowledge the source, release
  an existing register, or clear the claim. Missing evidence is not treated as proof that
  the exchange completed; existing PUBREL and recovery paths retain their own authority to
  advance it.

  This can retain state indefinitely. In particular, a restore-converted QoS 0 drain can
  complete a QoS 1 exchange with the client, release its register, and then definitely fail
  the save removing its claim. The restored claim, durable consumer and source message can
  remain retained even after storage recovers and across reconnects or restarts. This change
  provides no automatic repair for that state; clearing the session discards the retained
  session state rather than completing an outstanding exchange. The restore conversion makes
  this retention case reachable for the newly converted population. Positive-QoS drains
  retain their existing behavior.
- **A retransmission's Subscription Identifier and Retain As Published come from the
  membership that survives.** A restore-converted drain has no subscription to copy delivery
  options from, so those two are taken from the record's current values for the same filter.
  Where the outstanding exchange was started under a different grant, its retransmission can
  carry a different Subscription Identifier from its first transmission. The exchange's QoS
  is *not* treated this way — it comes from the exchange's own register — and No Local is
  not applied to a claimed drained sequence at all, because that arm acknowledges the source
  and would destroy a message the client is still owed.
- **A send already admitted past the delivery barrier is not retracted.** Cancelling the
  not-yet-transmitted allocations at the filter's staging keeps that window from widening
  across the packet; it does not close it.
- **A withheld marker is not crash-safe.** The evidence that a cancelled or
  cap-withheld delivery was never transmitted lives in memory, so a process crash loses
  it and that delivery's eventual first transmission carries DUP=1. Pre-existing, and
  unchanged by this.
- **"Transmitted" means what the drain's existing classification means.** Register
  confirmation and actual transmission are not equivalent; the exchanges preserved are
  the ones that classification enrols, and the existing transmission and DUP residuals
  stated elsewhere are retained rather than narrowed.
- **A destroyed downgrade's durable is the orphan sweep's from the confirmed save.** The
  consumer reference leaves `sess.cons` at the packet's final-update boundary, so that the
  record carrying the QoS 0 membership does not name a durable the packet then deletes.
  Between that save and the teardown the durable is named by neither the record nor the
  session, so an orphan sweep may delete it first — including a **peer** broker's, whose
  sparing pass cannot see another broker's resident sessions, and which no CAS fences. That
  is accepted because every route into the window is destructive by construction: the
  pending teardown is the one that would delete the same durable, so the sweep performs the
  packet's own decision early. The acceptance is bounded to that population and does **not**
  extend to an adopted or converted durable, whose exchanges are still owed and which the
  record therefore names.
- **On an AMBIGUOUS landed save the durable loses both prompt cleanup paths.** The packet
  returns before its deferred-delete loop and a destructive downgrade registers no
  continuation, so the record lands carrying the QoS 0 membership while the durable is still
  alive and named nowhere. With the reference already gone from `sess.cons`, neither a
  same-object reconnect nor a restore from the record reaches the existing-consumer arm, so
  convergence is a **successful orphan sweep and nothing else**. Until it runs the durable
  keeps its filter's messages on the interest-retained stream.
  - **Convergence is demonstrated for a broker that is gone, up to a completed graceful
    shutdown.** The cross-broker fixtures stop the owning broker, await its shutdown
    lifecycle to completion, and reap from the peer — in both orderings, sweep before the
    stop and stop before the sweep. An **abrupt** loss of the process is not exercised,
    because it is not constructible in an in-process harness, so nothing here is claimed
    about an ordering only an abrupt death produces.
  - **The session's `MaxAckPending` charge IS recovered, by the resolution rather than by
    the sweep.** Proving those bytes durable runs a refund-only landed continuation for
    every released durable, so the resident session does not keep a charge no consumer of
    its own can spend — a charge nothing gives back would cost a subscription slot for the
    life of the session, refused at the aggregate ceiling. The continuation gives back the
    charge and nothing else: no `Cons` entry returns, and the **durable** is still the
    sweep's. A **forked** or **foreign** verdict discards the continuation, which is
    correct, because the session is then being disposed of rather than kept.

**One deliberate deviation.** When a replacement preserves an occupant's QoS and that
occupant was in fact never transmitted, the resulting first transmission carries a QoS
the current grant no longer matches, deviating from [MQTT-3.8.4-6] / Spec5 [3.8.4-8],
which require the delivered QoS to equal `min(published, granted)`. The replacing writer
cannot prove the occupant was never transmitted — a cancellation may live only as a
nonce in the session record — so the alternative is to destroy the only durable trace of
what the client may already be holding and abandon an exactly-once handshake. A
graded-QoS inaccuracy on one message is preferred to duplication or loss. The trigger is
the replacement uncertainty window. **What is guaranteed is narrower than "for that
exchange's lifetime":** the admitted replacement preserves the occupant's QoS byte, and a
successful confirmation threads the QoS the record landed holding back onto the pending
entry. Whether the inherited exchange then survives at all is F-088's, per the residual
above — concurrent cancellation or failed-store settlement can still destroy or abandon
it.

**And its packet identifier can still be FREED while the client holds the exchange, which
is an unfixed release blocker rather than a deviation this document accepts.** A
cancellation of the *replacing* allocation is recorded against that allocation's nonce, and
once the replacement has landed the record carrying that nonce **is** the inherited
exchange's only durable trace — so the never-transmitted writers match it, recovery can
free the identifier, and `bumpPI` may re-mint it for a different message, against
[MQTT-2.3.1-4]. The route survives a crash between the landing and the confirmation, and it
is also reachable with the verdict saved *before* the landing, in which case no
post-landing teardown pass is involved at all. **F-088 owns this and has not closed it**:
its planning stopped after establishing that the fix needs durable transmission
attribution. **F-072 landed on 2026-09-09, so that contract now exists** — the register's
`unsent` marker distinguishes proven-unsent from possibly-attempted — but F-072
deliberately left the cancellation route, and consuming the contract there is unresolved.
A joint plan attempted it on 2026-09-09 and was ruled not ready; the completion half was
split out as **F-097**, which F-088 now depends on. See F-088's roadmap bullet for the
four counterexamples and F-097's for the completion identity failures. Nothing in this
section promises that reservation is preserved here; the guarantee stated above is
confined to the **QoS byte**.

**A delivery carrying no per-exchange QoS evidence is withheld, not guessed at.** The
identifier stays reserved and the source is not acknowledged. An audit of the removal
sites found none that leaves a source mapping behind a missing entry, so the arm is
believed defensive — but that is an audit result, **not a proof of production
unreachability**, and the behaviour is exercised only by an injected fixture. Where it is
reached the delivery stalls rather than going out at a QoS nothing established, and the
stall is not promised to end at the next resume: only a recovery that rebuilds the entry
from the record clears it.

### Outbound in-flight identifiers survive a restart and a failover

An unacknowledged outbound QoS 1/2 delivery is re-offered under its **original**
packet identifier after a broker restart, a failover, or a resume on a different
broker — not only on a same-process reconnect. [MQTT-4.4.0-1] requires exactly that,
for PUBLISH and PUBREL alike, and it now holds on every outbound path: plain
subscriptions, shared-subscription QoS 2, and drained filters.

The identity is durable. A started PUBLISH-phase delivery has an **in-flight
slot** on `$MQTT_inflight` naming its packet identifier and source delivery, and
the PUBLISH is not transmitted until that record is confirmed. At PUBREC the same
slot is re-phased and remains the identifier's owner through terminal cleanup; the
correlated PUBREL record is durable phase evidence and payload, not a second owner.
On resume both are read back before any source interest exists, so the redelivery
reuses the identifier the client is holding and a fresh allocation cannot collide
with one. See "Outbound in-flight registers" in `docs/ARCHITECTURE.md`.

What this closes, beyond the deviation itself: an allocation could previously mint an
identifier that an unreplayed PUBREL record still owned, because the in-memory PUBREL
map was empty until that record was delivered.

Residuals:

- **Retained-message deliveries are not covered ACROSS A RESTART, and the identifier is
  not either.** A retained QoS 1/2 send has no JetStream backing at all, so it has no
  register and cannot be redelivered after a restart in the first place. A client that
  received a retained QoS 1/2 PUBLISH whose acknowledgement did not reach this broker
  before a restart or a failover gets **no retransmission under the original identifier**,
  and because a retained send has no durable state whatsoever, a broker that restarts holds
  no record that the identifier was ever in use and **may re-mint it** while a QoS 2 client
  is still holding it awaiting PUBREL.
  **The SAME-SESSION half of this is closed by F-101**: a resume no longer drops the entry,
  because the release moved to the abandonment site, which is the only place
  non-transmission is proven. A resumed session therefore holds the identifier and its
  capacity charge for a retained exchange that really was transmitted. What survives here
  is the narrower cross-restart case, where no in-memory entry is left to hold anything.
  Closing that would mean giving a retained send durable state of its own, which is a
  different feature from identifier custody; F-091 deliberately did not do it.
  **What F-091 does guarantee is custody WITHIN one session object**, which is where the
  defect was: the identifier of a buffered or in-flight retained frame survives `clear()`
  and survives the write that follows a `clear()`, so it can no longer be re-minted while
  that write is still in the socket. What is released is the SUPPLEMENTAL retained
  reservation, on exactly two events: the frame is WRITTEN under its own generation, or is
  ABANDONED because no subscription will ever write it. A departure is not itself a release
  — a same-sid replacement departs `c.subs` but its frames and their reservations are
  TRANSFERRED intact to the replacement, because a duplicate-filter SUBSCRIBE still owes that
  replay. The `pendingPublish` entry is NOT released by either: it goes on
  reserving the identifier until the client acknowledges — or, for a frame that will never
  be written, until its ABANDONMENT releases the entry, which since F-101 is what that
  disposition does. The resume-time drop that used to do it is gone. That surviving entry
  is the pre-existing held slot below, not something this change introduces. The
  reservation is memory-only by construction — a crash closes the very socket it protected
  — so nothing here is conditioned on a rollout. The guarantee is about the ALLOCATOR and
  the PUBLISH path: while the reservation is live, `bumpPI` will not hand the identifier
  out and no PUBLISH send section may open under it. That is F-091's boundary;
  the PUBREL admission boundary is supplied separately by F-092 below.
- **F-092 and F-093 are advanced by the assembled seventh increment but NOT fully closed —
  one open item remains (#2, the custody-after-drain stall, below).** The sixth attempt
  built the owner slot, admission, terminal release and F-093's ordered handoff, then
  stopped at a no-ship verdict. The final increment added universal pre-CONNACK custody
  refresh over the positive eligible set, restored admission's current-custody check,
  closed the census, and made terminal deletion conditional on a complete same-exchange
  PUBREL scan with affine-proof-first confirmed disposal and a final freshness reread.
  What follows describes the resulting mechanism; F-092 and F-093 stay unchecked in
  `docs/ROADMAP.md` pending #2, whose ruled fix is recorded as Decision 7 in the work lane.
  A stored PUBREL is no longer transmitted under an identifier another exchange owns. One
  `$MQTT_inflight` record — the owner slot — owns an exchange for its whole life, so the
  identifier has exactly one durable claimant instead of two independently changing durable
  sets nothing could arbitrate between; and no SLOT-BACKED PUBREL bytes go out without
  first ATOMICALLY ACQUIRING a non-revocable action on that slot. A live retained exchange
  is the one exception — it has no register (R6) and so no slot, and is authorized instead
  by the live in-memory owner identity of its incarnation, refused rather than transmitted
  once that identity is gone. A record naming a different
  exchange is refused and, deliberately, **not acknowledged**: AckWait re-offers it, so a
  refusal can never lose a PUBREL the client is owed. F-093 goes with it — a completed
  exchange leaves no `pendingPublish` entry and no `cpending` mapping, so it no longer
  reserves an identifier and a Receive Maximum slot for the life of the session.

  **THE RESIDUAL: an abandoned action holds its identifier for an unbounded TIME.** An
  abandoned action is any acquired action whose holder can no longer confirm its durable
  drain. Finite failures recover — a validation or opener failure aborts before the write,
  an ambiguous acquisition adopts its own draining successor, a recovered panic runs the
  armed abort, a finite CAS or drain failure retries, and a proven generation discard
  retires the holder. What does NOT recover is process termination, a permanently leaked
  goroutine or blocked socket write, and indefinitely unavailable storage.

  There is deliberately no lease, TTL or steal, and that is not conservatism: a broker can
  pause AFTER its own expiry check and resume into the same stale write, and an MQTT client
  cannot validate a fencing token, so the external effect is not fenceable downstream. An
  abandoned action therefore ends only through its own holder confirming the drain, an
  explicit whole-generation discard (clean start or session expiry), or destructive operator
  recovery under the existing all-nine-stream reset boundary.

  **The bound.** Per session it is the actions admitted under whatever limits were in force
  when each was acquired — ultimately the 65,535 per-session identifier space, and NOT the
  session's *current* effective capacity, because Receive Maximum is re-evaluated on every
  CONNECT and a session may acquire many actions under a high limit and reconnect
  advertising 1. **Across sessions there is no global bound**; the total is the sum over
  affected sessions. CONNECT and QoS 0 continue and other identifiers stay usable, but at
  Receive Maximum 1 a single reserved identifier stalls further QoS 1/2 delivery for that
  session — and a reconnect at Receive Maximum 1 may find several already reserved.

  **The custody gap from the sixth attempt is closed at recovery, with one open residual
  after a drain (#2 below).** Before source interest is restored,
  recovery contests every positively eligible publish or past-PUBREC slot and rewrites its
  custody to the current session acceptance. Admission requires that current claim before
  it can acquire an action. The positive set includes a retired `pubrel_ready` slot only
  when a present PUBREL record names its exact source, identifier and exchange nonce: that
  evidence means the client is still waiting for the step admission can acquire. A retired
  flag alone, a cancelled slot, an active action and a terminal slot receive no custody
  rewrite. A slot observed concurrently moving into an action or terminal state is withheld
  rather than overwritten.

  A stored PUBREL with no surviving authority remains a deliberate fail-closed residual:
  it is refused rather than acknowledged, so genuine residue may occupy `$MQTT_out` space
  until generation teardown or operator removal. An absent slot alone cannot prove an
  exchange is over; terminal disposal therefore requires the terminal slot, a complete
  same-incarnation scan, matching owner/source/affine identity, and a final freshness
  reread before the slot is deleted. The source must also be provably non-reofferable
  regardless of whether its `Retired` mark landed: a past-PUBREC phase is sufficient for
  recovery to install source suppression and persist the missing mark while the source is
  live or unknown. A positively lost consumer is distinct from a completed source and
  never licenses PUBLISH-phase reaping, but after PUBCOMP both mean the source cannot be
  offered again. Terminal cleanup retains its phase evidence in every other case. PUBCOMP
  replaces its delivered in-memory entry with a nonce-bound terminal reservation under
  the same lock, so
  allocation cannot re-mint the identifier while those storage round trips run. An
  indeterminate cleanup keeps that reservation and retries through the shutdown-aware
  worker gate until the durable consequence is proven.

  **Related, and narrower — CLOSED.** Custody transfer is still withheld for a slot with a
  live action, so a takeover completes for the rest of the session while that one exchange
  stays non-actionable until the action drains; a holder that already acquired an action
  completes it across a later acceptance, so a successor may see one PUBREL written to the
  predecessor's closing socket for a real past-PUBREC exchange, with nothing durable
  landing under the superseded acceptance. The drain then clears the action fields but
  leaves the predecessor's Claim — and the successor now REPAIRS that at admission rather
  than waiting for a reconnect: a stored PUBREL that passes every admission condition
  except custody moves custody to the proving incarnation, once per delivery, and the
  consumer's own re-offer is the retry. The transmission fence
  (`action CAS -> acceptance proof -> socket write`) narrows the acquisition-to-write
  window but does not close it, so the predecessor-socket PUBREL above remains possible.
  **Recovery is redelivery-driven, so completion has no fixed wall-clock bound**: a failed
  attempt leaves the record unacknowledged for a further AckWait, and repeated storage
  failure means repeated intervals.

  **A storage-progress dependency, stated because it is a real availability cost.** A
  finished past-PUBREC slot whose required terminal advance cannot be resolved now REFUSES
  the resume instead of accepting a connection whose identifier would stay charged with
  nothing left to retry it. While the store keeps failing that refusal repeats: the three
  attempts and the request timeout bound one attempt, not the number of refused CONNECTs.
  A permanently malformed record does not reach that path — the scan routes it to the
  opaque disposition instead.

  **A record stored AFTER the resume's PUBREL scan is still not seen by it (F-099).**
  Recovery now scans the PUBREL records after every batch of its initial register
  enumeration, so an absence can no longer predate the register it is used against. That
  closes the filed interleaving; it does not make the scan atomic, and a peer inside its own
  store-before-phase window can create a record after the scan has passed. The worked
  schedule:

  1. A PUBREC continuation for an identifier is admitted and pauses before its store.
  2. Another continuation stores and proves the canonical record, advances the slot to
     `pubrel_ready`, and completes the source.
  3. PUBCOMP processing hits the indeterminate-before-terminal cut above; its delivery ack
     removes the canonical record, leaving `pubrel_ready` with no record.
  4. The resuming broker loads that ready slot and takes its scan, correctly finding none.
  5. The paused continuation now stores a same-exchange DUPLICATE, after that scan.
  6. The resume infers the exchange finished and advances the slot to terminal; terminal
     disposal's own scan finds and deletes the duplicate.

  **Direction.** The resuming broker destroys a record a peer created for an exchange it had
  INFERRED finished — from an absence plus source completion, which is all it holds; it has
  no evidence of the client's PUBCOMP. Because this schedule does contain a real PUBCOMP it
  does not by itself demonstrate a further uncompleted-client loss. The late record is a
  duplicate, and what establishes that is NOT the peer's failed phase CAS — a live delivery
  can win the direct `publish -> pubrel_sending` arrow before that CAS runs — but the
  pre-existing canonical tuple and phase history: the slot already named an earlier proven
  record and had already left `publish`.

  No scan ordering can close this. The window spans two streams and JetStream atomic
  batches are per stream. It belongs to F-098, which owns exclusion across terminal cleanup
  and PUBREC advancement including the store-before-phase window.

  **A past-PUBREC reservation with no record is reaped only on a resume that proves source
  completion (F-099).** A body a custody contest adopts as `pubrel_ready` takes no
  completion verdict from that pass, because the only evidence available predates its
  adoption; it is reserved as owed. The next resume observes it in its initial register load
  and may reap it — but the verdict requires `mqttInflightSourceComplete`, so a source that
  is LOST or UNKNOWN retains the reservation across every reconnect while it stays so, and
  nothing schedules the resume that would clear it. The cost is one held packet identifier
  and one Receive Maximum slot, not a lost message, and it is the conceding direction.

  **What is NOT repaired, and it predates this mechanism.** There is no general
  successor-side reconciliation of stranded LOCAL PUBREL reservations. If a peer completes
  an exchange and removes the slot and its canonical record before this broker completes
  its own local release path, this broker can retain the identifier and its Receive Maximum
  charge indefinitely — including an empty withheld reservation, and an entry whose
  acknowledgement subject was filled before a transmission that did not happen. A
  same-object reconnect does not guarantee release; rebuilding the session object from
  durable state removes purely local residue, and a whole-generation discard clears it. At
  Receive Maximum 1 one such reservation blocks further outbound QoS 1/2 delivery. It
  requires a completion overlapping a takeover or a recovery, has no trigger in
  single-broker steady state or on a clean takeover, and is not a cost of the inline
  custody repair — the same reservation is reachable without it. A malformed or
  marker-less PUBREL record is never
  interpreted and costs stream space rather than an identifier. A **retained** QoS 2
  exchange's PUBREL is not authorized across a restart — it has no register at all, so its
  authority is the live in-memory owner identity of that incarnation — and its record is
  refused rather than dropped.
- **What IS enforced today, stated narrowly on purpose.** A stored PUBREL carries a
  mandatory `Nmqtt-Q2Owner` exchange identity, and a record whose identity is absent or
  malformed is **withheld** by the delivery callback — never transmitted and never
  acknowledged away, because acknowledging would destroy the only copy of evidence
  nothing can attribute. It reserves no identifier, so the cost is stream space until
  an operator or a generation teardown removes it. A drained exchange's PUBREL intent
  carries a mandatory owner nonce AND the packet identifier the client is waiting on,
  and a record found for that intent must match **both** exactly before the drain claim
  is finalized; a mismatch is treated as no record and the exchange's own is recreated.
  The identifier half was missing until a review round found it: a record with the right
  source and the right exchange under a foreign identifier confirmed the intent, after
  which the original exchange had no PUBREL and no evidence left to rebuild one from.
  Every production write of an in-flight register is validated against one legal-shape
  matrix and **refused rather than written** if it does not conform.
- **An in-flight register's schema is CLOSED, and its shape is enforced on both sides of
  the boundary.** Every path that WRITES one is validated at the single write boundary and
  refused rather than written. Every path that READS one decodes through a single decoder
  that rejects **any unrecognised field**, any case variant of a known one and any
  duplicate member, and then applies the same matrix — so an illegal
  body, or one carrying a field this broker does not define, is **unreadable**, exactly as
  malformed JSON is, and may not authorize a mutation, a deletion or an identifier release.
  The closed schema replaced five declared-but-unproduced fields whose only function was to
  refuse themselves; that was an incomplete closed schema, since a sixth plausible control
  field would have been dropped silently by the JSON decoder and the record read as an
  ordinary legal register.
  Two review rounds were needed to close the read half. The first left the matrix governing
  writes only, so an unsupported shape read back counted as readable; the second found that
  validating it in the recovery scan alone still left the destructive readers — drain
  identity release, the gated reap and generation teardown — trusting an identity-valid but
  illegal body. Enumerating readers was the defect, so the rule is now a property of the
  one decoder rather than a check repeated per site.
  Recovery restores every valid state according to its phase: publish entries return to
  the publish maps, ready/sending states reserve the PUBREL exchange or its action, and
  terminal states finish only through the terminal disposal gate. An invalid or absent
  state is quarantined and cannot authorize a mutation, deletion or release.
  An earlier revision tolerated an absent state as "what earlier writers produced". That
  was rolling-upgrade machinery, which this project's compatibility boundary forbids
  outright, and it was also incoherent: every shared register is claim-rewritten through
  the write boundary, whose validator refuses an absent state, so a tolerated record
  became an unresumable session a moment later. Pre-alpha, with no releases anywhere,
  there is no population to serve — a developer stream carrying older records resets, and
  the nine broker-owned streams are one recreation unit (F-066).
- **A marker-less drain intent does NOT refuse the session record, contrary to the
  plan.** A past-PUBREC intent that nothing can attribute — no usable exchange identity, or
  no packet identifier — is restored and marked
  **non-finalizable** — the same treatment a duplicate register gets, because which record
  the client holds is unknowable — so nothing finalizes on evidence it cannot attribute. The
  documented whole-record read-only refusal is **not built**: it needs an error
  propagated through normal restore, expiry, teardown and fork-repair, and an earlier
  attempt that simply dropped the drain was worse than restoring it, since the session
  was then accepted without a drain whose consumer and pending exchange were stranded.
  The broker does not write such a record — the intent's identity is validated before
  it is saved — so this is a tolerance for records from elsewhere, not a supported shape.
  The PHASE is read from the `PubRel` flag alone. An earlier revision required a nonzero
  packet identifier alongside it, which folded "past PUBREC but unattributable" into "not
  past PUBREC" — restoring such a record as an ordinary claimed source, eligible to have
  its payload redelivered to a client that had already answered it. Whether an intent can
  be acted on is a separate question, and its answer is the quarantine; the drain's durable
  also joins the resume SKIP set, so no source interest is restored for it.
  A **SUBSCRIBE** that names such a drain is refused with a failure code, **fork repair
  refuses the whole filter change**, and each refusal covers **every sid the filter owns**:
  a wildcard's level-up companion is quarantined
  independently of its main sid, and refusing them one at a time adopted the usable one
  before failing, which the next successful save turned into a lost drain. The client can
  restore the filter only by removing the record.
- **Adopting a drain no longer erases an unfinished past-PUBREC intent (F-094, FIXED).**
  A re-SUBSCRIBE over a draining filter, and fork repair's destructive rows, used to remove
  the drain from both maps and persist no `Started` obligations — so the stored PUBREL could
  complete on the ordinary non-drain path while the unacknowledged source was free to
  redeliver as a fresh PUBLISH, duplicating an exchange the client had already answered.
  **Every** past-PUBREC intent now holds its sid, not just the unattributable subset, and
  the refusal is protocol-visible: the client sees `SUBACK 0x80` for that filter. The hold
  is released when the drain is RETIRED — deliberately not "when the PUBREL completes",
  which would be wrong for a finalized intent held beside another outstanding claim. The
  costs are stated in the drain-section residuals above. The two things F-094 did not
  close are now closed by their own changes rather than deferred: delivery readiness on
  adoption, by the live-SUBSCRIBE reclaim, and the disposal route to the same duplicate,
  by retirement's delivery barrier and classified delete. What remains open is not a
  route to a duplicate PUBLISH but the three disposal residuals stated above.
- **A retained QoS 2 exchange's PUBREL surviving a restart is REFUSED, not transmitted.**
  A retained send has no register (R6), so its exchange identity lives only in the in-memory
  owner of its incarnation and that identity is gone after a restart. Admission
  (`retainedPubRelAdmittedLocked_`) now authorizes a slot-less PUBREL only when a live
  `pendingPubRel` entry of this incarnation names the record's exchange nonce AND that entry
  names this generation's PUBREL consumer (`jsDur == pubRelDurable()`, which separates it
  from a recovered source-backed placeholder whose `jsDur` is the source consumer). After
  a restart neither holds, so the record is refused — never transmitted under an identifier
  another exchange may by then own, and never acknowledged away, so an operator can still
  see it. This closes F-092's central hazard in the one subclass where no durable fact can
  settle it. Two earlier revisions of this bullet were wrong in opposite directions: one
  claimed the record was "withheld by the identity rule" when admission was still unbuilt
  (a holistic round disproved it by reading the recovery filter), the other still described
  the unbuilt state after admission had in fact been built.
- **Promptness, not continuity, still depends on the reconnect being same-process.**
  The identifier is restored either way, but the immediate NAK-driven redelivery needs
  a live per-attempt JetStream ack subject, which a recovered entry does not have — so
  after a restart the redelivery waits out `AckWait`.
- **An unaccountable identifier space withholds new QoS 1/2 deliveries rather than
  refusing the session.** If the durable identifier space cannot be fully accounted for
  on resume — the walk failed, a register's body is corrupt, two registers name one
  delivery on different identifiers, or a register's affine QoS 2 pair is not PROVEN by
  its
  own source (contradicted by it, or unprovable because the source could not be read or
  its
  consumer is gone) — the session stops allocating identifiers until a
  later resume reads it cleanly: QoS 0 keeps flowing and reserved exchanges complete,
  but new QoS 1/2 deliveries are withheld and redelivered by AckWait. This is a
  deliberate choice over denying the CONNECT, and over letting a redelivery proceed
  under an identifier that cannot be proven to be the one the client holds. A session
  discard lifts it, since the identifier space it protected is retired with the
  incarnation.
- **A copy whose header names a different assignment than its own subject is refused
  forever, not collected.** Both parts of that pairing are well formed, so nothing can say
  which of the two assignments the message belongs to — and this broker writes no such
  copy, so per the compatibility boundary it is refused rather than interpreted. The copy
  therefore stays on `$MQTT_out`, re-offered on the refusal backoff below, holding one
  `MaxAckPending` slot of the session's affine consumer until the consumer is deleted with
  its generation. Deleting it is the alternative and is worse: the assignment whose copy
  subject it is may be live, and acknowledging the copy away would lose that message and
  leave its claim stranded without `AffineDone`.
- **A copy the broker cannot act on yet is re-offered on a backoff, so a refusal costs
  latency rather than throughput.** Every refusal on the session-affine QoS 2 delivery
  path
  hands the copy back with a NAK carrying a delay that doubles from 100 ms to a 30-second
  cap, derived from JetStream's own delivery count for that message. The first retry is
  therefore prompt, which is what the ordinary case needs (a copy is stored before its
  claim records the phase, so a delivery can legitimately outrun that advance), and a
  refusal that persists — an unreadable or absent claim record, an unavailable backend —
  settles at the cap instead of spinning. The cost is bounded added latency on a claim
  that
  becomes actionable late: up to one cap interval per exchange, once the backoff has
  grown.
  A plain NAK was measured at 9214 re-offers in ten seconds for one copy, which is the
  behaviour this replaces.
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
- **A PUBREL the client never receives holds its identifier for the rest of the
  connection.** A PUBREL transmission reserves its packet identifier until the client's
  PUBCOMP is processed, which is what [MQTT-2.3.1-4] makes the release event and what
  keeps a delayed PUBCOMP from binding to a re-minted exchange. If the enqueue is
  suppressed instead of written — the only case is a packet exceeding the client's MQTT
  5.0 Maximum Packet Size, which a connected client's own success CONNACK makes
  unreachable in practice — no PUBCOMP will ever arrive and the debt is never answered.
  **The cost:** one packet identifier and one Receive Maximum slot, held until that
  connection ends. The debt stops reserving anything the moment the connection is no
  longer the session's, so it does not survive into the next one, and no manual
  intervention clears it. This is the fail-closed direction: the alternative is releasing
  an identifier the client may still hold.
- **A quarantined identifier is held until the session resumes.** When a delivery's
  create-only register CAS meets a LIVE register another writer owns on that same
  identifier for that same source, the identifier is quarantined in place rather than
  abandoned: abandoning it would let the source's next redelivery take a second
  identifier while the rival's register stands, which is the exactly-once deviation
  itself. There is no live-session release. Both durable shapes that could express "this
  identifier is settled" — an observed absence, and a cancellation tombstone — are
  consumable by the rival's own supported lifecycle, so a live release could free an
  identifier whose durable custody it cannot settle. The quarantine is therefore removed
  only by the next SUCCESSFUL resume's final-disposition reconciliation (staged during
  recovery and applied only after the CONNACK and the forced leave have settled, so a
  refused resume never frees it) or by session-generation retirement.
  **The cost:** if the rival completes while this connection stays live, one packet
  identifier and one Receive Maximum slot remain held for the rest of the connection. At
  a Receive Maximum of 1 with the sole slot held, new outbound QoS 1/2 traffic waits for
  disconnect-and-resume; QoS 0 and already-established exchanges are unaffected. The
  identifier is counted exactly once by the in-flight accounting, including after a
  consumer purge unlinks its source mapping, so this is conservative UNDER-utilization —
  the broker sends fewer concurrent messages than Receive Maximum permits, never more.
  It is the same fail-closed price recovery already charges for an unclaimed member, and
  it is operator-visible through the existing lost-arbitration warning. A live release is
  filed as its own finding; any future design for it must prove exclusion against every
  supported writer and recovery lifecycle, not against one racing write.
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
- **A refused discard's obligation is memory-only, so a crash in that window drops it.**
  On a broker running this code, no discard runs on an unclassified
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
  classifying eviction worker evicts the re-added object — correctly, since a record this
  broker may no longer write is not its to delete. **Since F-084 the obligation no longer
  goes with it**: that eviction EXTRACTS the parked action and discharges its Will against
  the original deadline, so what this path loses is the discard obligation, not the Will.
  (The eviction is asynchronous, defers to a held ownership gate or record fence, and can
  be refused outright, which changes when the obligation is lost, not whether.) And the
  helper DECLINING, which needs neither of the other two and whose reachability is
  unknown. Absent all three, the next successful local classification retries the discard
  and completes it. A remote persist has two orderings and neither is a guarantee:
  arriving after the re-add it evicts, and arriving in the window between the caller's
  `removeSession` and the re-add it takes the no-local-session branch and starts a
  reconciliation worker that then finds the object resident and deliberately does nothing
  — so convergence there is eventual, through the uncertainty classification calling the
  object foreign, not immediate.
  - For a v5 DISCONNECT that overrode a non-zero Session Expiry Interval to 0, losing the
    obligation used to be a spec deviation rather than a leak, and **F-041 closed that**:
    the override is now persisted as `Clean = true` with `ExpiryInterval = 0` **before**
    the discard is attempted, so a refused discard leaves a record a reconnect declines to
    resume. What remains on these paths is the leak — the record, its consumers and the
    generation's registers survive with no resident object — and no longer a session that
    reports Session Present true after the DISCONNECT ended it (Spec5 [3.1.2.11]).
    **The reduction is claimed only for a backstop that was successfully persisted.** A
    definite failure of the lifetime write leaves the pre-override terms durable and the
    old deviation stands for that intersection; the close reports it at warning level. And
    for a FINITE override the record instead carries the elapsed lifetime with its
    disconnect stamp, which both resume checks read as overdue — a different route to the
    same refusal, and the reason the stamp is written alongside the pair rather than left
    to the discard.
    The reconnect-race item in `docs/ROADMAP.md` §0 is a separate, purely in-memory
    mechanism and is unaffected.
  - A crash in the CLEAN close's window leaves an expiry-0, `Clean = true` record with its
    consumers: nothing reaps it autonomously (the retry pass needs residency, the expiry
    sweep skips `ExpiryInterval == 0`, and the consumer sweep reads the record's consumers
    as referenced), so only a later CONNECT on the same client ID discards it.
  - **The same crash can lose that close's WILL, because the recovery runs BEFORE the Will
    is published.** The refusal deliberately keeps the Will out of the parked action, so
    the close publishes it by falling through to `mqttHandleWill`, after the recovery has
    parked the obligation and restored residency. The original rationale for that split —
    that a memory-only Will parked on the action would be dropped by a takeover — no longer
    holds as stated: F-084 made every RECORD-DERIVED disposal discharge a parked action's
    Will, so only the LICENSED takeover still drops one, and there by design. The split is
    retained on the narrower ground that an owed-now Will must not wait on a teardown the
    recovery may never complete. A crash in between loses the Will permanently: no durable
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
    stamp a new, LATER disconnect time, and a clustered non-origin broker deliberately
    will
    not — so if the origin never returns, the record can read as connected indefinitely.
    Pre-existing (the failed-disconnect-save residual), reachable through this window too.
  - A CONNECT refused this way asks the client to retry, and a crash after that rejection
    still loses the memory-only continuations the rejection preserved. Strictly smaller
    than
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
  and `reconcileResidentRecordState` performs a synchronous readback per unresolved
  candidate
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

### The DISCONNECT lifetime override is durable, and expiry TIMING is not repaired

F-041 makes a v5 DISCONNECT's Session Expiry Interval override durable: `Clean`
and `ExpiryInterval` are written together at the close, carried across every
park, and applied on both discharge branches. Two timing residuals remain, and
neither is the deviation that item closed.

- **This change adds expiry lateness, without a demonstrated sub-second bound.**
  The close performs one extra record write on the override path, so every
  sample taken after it — the disconnect stamps, the expiry timer — is later than
  it was. The bound is not "tens of milliseconds": the record fence's blocking
  acquisition carries no bounded-wait guarantee and the JetStream API timeout is
  five seconds. Nor is the effect confined to a rounding boundary, because the
  close timer schedules a full relative interval **after** the finite save.
  **It can be client-visible.** With a one-second interval, a stamp at `t.990`
  gives a resident deadline of `t+1.000` while a delayed stamp at `t+1.010`
  gives `t+2.000`; a reconnect at `t+1.500` is overdue under the first and
  resumable under the second, which changes Session Present and the restored
  subscriptions. What holds in the other direction is only this: **for a fixed
  interval, delaying these samples cannot make a computed deadline earlier**, so
  a Will's `FireAt` — anchored at the untouched close observation — is never
  overtaken as a result.
- **A coarse expiry deadline can still precede a Will's `FireAt`, and that is
  pre-existing.** `DisconnectedAt` is a seconds field while `FireAt` is
  nanoseconds, so the deadline derived from it can precede a capped `FireAt` by
  up to a second. Four consumers derive one that way: the parked action, the
  resident `overdue` check in `mqttProcessConnect`, the expiry callback's
  recheck, and the record-driven sweep. Where that happens a session can be
  expired before the Will it was capping fires, and a reconnect in the gap
  suppresses that Will — the acceptance-fact predicate compares against `FireAt`
  and has no ended-session test. **F-041 leaves all four exactly as it found
  them.** An earlier revision of that item tried to repair the ordering by
  anchoring the close's deadlines earlier; three review rounds found it
  corrupting a different consumer each time, the last being the resume decision
  itself, so the attempt was deleted rather than widened. Repairing it means
  either widening a persisted field or capping `FireAt` against the coarse
  deadline, and both are wider than that item.
- **A park's disconnect stamp is still dated at the park, not at the close**, so
  a slow ownership-gate spin dates it late. Pre-existing, unchanged, and subsumed
  by the first residual above.

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
  competing write intervened. Both outcomes were always legal — the client was told nothing
  and must reconcile on reconnect — and the landed one was always possible; what changed
  is that it became deterministic. A granted Session Expiry Interval of 0 therefore ends
  the session at that close even though the CONNECT was refused, which is the
  grant-at-durability rule this broker already followed whenever the write happened to
  land.
  **A shared membership can survive a failed SUBSCRIBE on a second route, and it is not a
  resolution verdict.** Its eager per-filter write goes to the record BEFORE its group
  consumer exists, so that write may have succeeded while the packet's final update AND its
  compensation both failed definitely. The rollback then restores the membership rather
  than discarding it, so a resume restores a filter the client was never acknowledged for.
  This does **not** mean every definite failure establishes retained remote membership: the
  restored value is local state as it stood before the compensation, and whether the record
  agrees is the arbitration question. The alternative was worse — dropping the holder while
  its group durable stayed live, which no sweep reclaims.
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
  leaves an unreferenced durable with **no retry anchor**: since F-036 course E the orphan
  sweep does not collect an affine consumer, so it is retained until an authorized lifecycle
  cleanup reaches it or an operator removes it by hand. A generation the save merely
  re-established is deliberately excluded: it may owe assignments to a delivery already
  started to it. If the assignment scan FAILS — it is account-wide and fails closed on any
  claim it cannot read or attribute — the subscription and the charge are still released,
  because once the record stops naming the path nothing else can reach them. The discard
  is a STREAMING one, so its progress is PARTIAL rather than none: the generation's claims
  the scan had already reached are discarded, and a claim that was already proven on both
  sides may also have been reaped, while the malformed record aborts the remainder. Those
  remaining assignments and the consumer stay pending, and they are not deferred to
  the orphan sweep either: **since F-036 course E its candidate-reclamation route no longer
  reclaims an affine consumer by absence**, and even for the CLAIMS it does reconcile the
  same unreadable claim aborts its inventory, deliberately, since an incomplete inventory
  cannot authorize a transition. So
  an unreferenced consumer stays on `$MQTT_out` regardless of this record — that is the
  accepted course-E leak, not a wedge — while what this malformed claim additionally wedges
  is affine CLAIM reconciliation account-wide until the record is repaired or removed.
  That wedge is confined to that class: the sweep's other phases — including PUBREL
  durable reclamation, which needs no assignment inventory — keep running.
  The RESIDUAL is the crash window between the ambiguous
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
  terminated for a session that is still live, which is the [MQTT-4.8.2-5] outcome applied
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
  subject is still read as not-ours. (It was read as *another owner's* before F-084; the
  disposal is the same, and the claim it makes no longer names an owner.)
  (For a competing CONDITIONAL write the
  route is closed — see the same-base boundary below.) Narrowed from "absence is a
  verdict", not closed.

### A forked session record is RECOGNISED, and its recoverable grant REPAIRED

`$MQTT_sess` records carry `WID`/`PWID`, a parent pointer, so a survivor that provably
**CLAIMS the parent** of the record this broker was acknowledged for is recognised as a
SIBLING — which is what mints the repair grant. It also stopped being reported as a
takeover here, which F-084 has since generalised: **no** record-derived route claims one.
The client is told **0x89 "server busy, try again later"**
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
    boundary), so there is no older path to be asymmetric with. It did **not** by itself
    close F-036's bound deleted-record case, which unit 2 has since split — see the next
    bullet;
  - a record that was **deleted** rather than moved is handed to the ownership marker,
    which converges an UNBOUND session. **Its BOUND outcome is now split by F-036 unit 2,
    and only one half converges.** Where a valid retirement watermark's `mseq` exceeds the
    sequence this broker is acknowledged for, that watermark plus the record's absence
    plus a full in-fence revalidation is a LICENCE, and the periodic pass retires the
    bound session itself with no traffic from the client — reporting **0x89 "ownership
    unsettled"**, never a takeover, since no record shape demonstrates a successor
    (F-084). Where there is NO such licence — the watermark absent, unreadable,
    indeterminate, or naming a marker at or below our own sequence — nothing converges: the
    marker stays armed and a bound Keep Alive 0 client that never sends again may strand
    for the life of the connection, until traffic drives the pre-dispatch settlement. That
    remaining half is a **permanent stated residual**, not filed work: the licence is the
    only positive evidence available, and absence alone is exactly the inference F-036's
    site 3 exists to refuse. Absence is a real ownership fact — only a party that claimed
    the record at our
    sequence can remove it — but it is **not a successor fact**, and F-084 corrected the
    claim accordingly: this text used to say the accompanying `0x8E` was "right either
    way" and only its Reason String imprecise, which had the defect backwards. No new
    connection need exist, and none is now asserted. The disposal is right either way and
    the client's reconnect correctly gets a fresh session. **A gone-specific vocabulary
    value that would tell an expiry-driven vanishing from a takeover WILL NOT EXIST, and
    this is a permanent stated residual rather than filed work.** Its roadmap bullet was
    closed as refuted: the only positive evidence available, the retirement watermark's
    retired-record identity, names a record's BYTES rather than one OCCURRENCE of them, so
    it cannot say which occurrence a retirement removed; the field has been removed. So a
    vanished record, an unreadable one, an undecodable one, a below-base one and a
    valid-shaped stranger all report this one `0x89`, and an operator cannot tell an
    ordinary retirement from a store fault from that line. Closing it needs a claimed
    predecessor SEQUENCE on two permanent shapes, refused as disproportionate — see
    `docs/ARCHITECTURE.md`, "The generation-retirement watermark". **Removing the field
    also removed a FORMAT GATE, and that is disclosed rather than claimed away**: a
    watermark carrying a valid `id`, `gen` and non-zero `mseq` but no `rwid` was CORRUPT
    before and is VALID after, so four logical readers now ACT where they previously
    refused — the CONNECT restore's discard of a retired record, the orphan sweep's
    condemnation, the record-absent create's refusal (which keys on the corrupt verdict
    alone), and `recordGenRetirement` itself, which proceeds to a no-op-or-replace and
    then to deletion instead of failing closed. Two of those read the watermark AGAIN
    after their first read and a no-`rwid` value can appear in between: the writer's
    `retirementFenced` re-reads and the sweep's act-time revalidation. It creates **no new
    LOGICAL false positive**, because the validator only ever checked the field for
    NON-EMPTINESS and never compared it with anything, so an arbitrary non-empty string
    already passed every one of those readers; what was lost is a format check, never a
    semantic one. No client-visible behaviour changes for currently valid supported state;
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
    unadopted attempt CONCEDES, while a sibling of the acknowledged record itself still
    classifies forked; a stale pair can only narrow the verdict, never manufacture a
    false forked one. **What conceding says was corrected by F-084**: it used to be 0x8E
    plus the takeover-path Will cancel, and is now `0x89` ownership-unsettled with the
    Will taken through ordinary handling — failing to recognise sibling-hood leaves the
    broker knowing less, and less knowledge cannot license a stronger claim;
  - the **timer-cancel** consequences are unchanged in shape: both verdicts cancel the
    session-expiry timer (this broker may not write the record either way, so its timer
    must not fire against it), which still bounds what the rollback path's preserved timer
    is worth;
  - a bound connection's never-enqueued delayed Will **was** lost on eviction, on both
    verdicts, because eviction detached `sess.c` and the close's teardown gate then failed
    so the Will was never enqueued. **F-084 closed that**: the eviction tail now discharges
    a parked deferred action, or hands a live Will to ordinary handling, before the
    notification and the close. What THIS item narrowed was only the destruction of
    already-durable outbox state;
  - an **interrupted cancel scan** left a durable partial state — earlier records
    cancelled, later ones pending, no resume marker. Historical: the eviction worker runs
    no cancel scan on any verdict now, the displaced incarnation's Wills being the
    claimant's business at fire time;
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
  multi-level filter, the drains an acknowledged packet staged, the
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
  - a repaired **QoS 2** membership whose PUBREL provisioning then fails is no longer
    accepted-but-dead: `processSubs` runs before the success CONNACK now, and that
    failure refuses the CONNECT (F-050);
  - **shared-subscription membership is still excluded, but by choice rather than by
    ordering.** The ordering that forced it — the success CONNACK preceding shared
    restoration — is gone with F-050. Widening the repair set to shared membership is
    F-027's own work, tracked as its own roadmap bullet, and until it lands the
    exclusion stands.
- **The verdict does not prove the survivor erased us.** A teardown claim is a full-body
  CAS off the same base, so a sibling can merely have become current after `clear()`
  deleted our record. What is established is only that the survivor is not demonstrably
  a successor of ours.
- **RESOLVED BY F-084.** This bullet used to say the withheld verdict could publish a Will
  a genuine takeover would have suppressed, because on the definite-rejection bound path the
  foreign settlement discarded the Will while the 0x89 route ran ordinary close bookkeeping
  and published it. **Both arms now publish**: the foreign settlement demonstrates no
  successor either, so it consumes the Will exactly as the 0x89 route does. There is no
  longer a divergence for the forked verdict to reach more often.
- **The ambiguity path's treatment is unchanged in every other respect**, and one part of
  it was **RESOLVED BY F-084**: `disposeForeignStateLocked` still takes the parked deferred
  disconnect, but it no longer drops it — the prepared disposition DISCHARGES that action's
  Will against its original deadline, so the only copy of a Will and its expiry deadline are
  no longer lost there. Retained `uncertainLanded` continuations are still not cleared; that
  is PRE-EXISTING behavior on the foreign path, not a regression from this item, and is
  filed separately.
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
  generation-retirement watermark's `RWID` (a field since removed outright), an adopted
  marker's own `PWID`, `clear()`'s
  stamp of the record its claim held, and the `$MQTT_origin` session-interval register
  with the stream itself. The reason is stated below and is the whole finding of that
  audit — every route by which the inheritance produced a verdict was reachable equally
  by a FALSE fork, and there was no route on which it was reachable only by a true one.

  **Read the vocabulary in this subsection historically.** It records the write-lineage
  audit as it stood, when conceding meant `0x8E` plus a takeover-path Will cancellation.
  **F-084 changed what conceding SAYS and DOES** — `0x89` ownership-unsettled, with the Will
  taken through ordinary handling — so every "concedes to the takeover vocabulary" below
  describes the audit's reasoning, not today's wire behaviour. The audit's *conclusions*
  about which pairs are recognised are unaffected.

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
    `TestMQTTCreateForkE2EAnswersUnsettledToTheDisplacedClient`.

    **Its cost is no longer vocabulary at all.** The conceding side used to report 0x8E and
    cancel its delayed Will; since F-084 it reports 0x89 exactly as a recognised fork does,
    and destroys no durable Will state either. What still differs is the REPAIR: a
    recognised fork mints a grant anchored to a record proven to claim our parent, and a
    concession mints nothing, because nothing was proven.

    **The residual is conditioned on EVIDENCE, not on a rollout.** It stands unless a
    route is found that produces a true create-pair verdict without also producing the
    false ones below. Two refutations stay recorded so they are not re-proposed. A
    **reserved parent token** stamped on the record-absent arm was designed and REFUTED,
    because an absence is not a fact: administrative deletion and partial restore can
    leave a `$MQTT_teardown` with no historical fences. (Recreating it ALONE used to do
    the same and was the sharper example; that operation is now refused at startup —
    see "Stream prerequisites" — but the refutation stands on the operations that
    remain, which are unsupported rather than impossible.)
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
  holds — at the pinned nats-server no Raft-level field preserves "these entries came
  from one `ProposeMulti`"; entries are pushed individually, popped as whatever prefix
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
     them. **`$MQTT_inflight`'s ran (F-034, closed 2026-08-22)**: no lineage was needed
     there either, and what it found instead was a reachable identity-confusion route,
     remedied by identity conditioning plus bounded destruction.
     **`$MQTT_qos2assign`'s ran (F-033, closed 2026-08-18)**: every claim write is
     funnelled through one CAS helper that always emits its expectation, so it needed no
     lineage, and what it found instead was a source ack bound to a copy store that never
     proved retention, a reference skip, a reap on a stale reading and a claim key that
     outlives the `$MQTT_msgs` incarnation it indexes — F-063, F-064, F-065 and F-066,
     none of which this admission closure speaks to. **`$MQTT_qos2in`'s
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
  other outcome closes the connection — a `0x89` ownership-unsettled ATTEMPT when the
  probe proved the session foreign (it was `0x8E` before F-084, which claimed a successor
  the probe never found), a `0x89` "server busy" attempt when nothing was established, and
  SILENCE when the session was found REBOUND (the displacement owns the notification and
  makes its own attempt; a courtesy packet here would contradict it on the same socket).
  Each of those is
  an attempt rather than a delivered packet: a connection's one server DISCONNECT is claimed
  once (see (10)), the displacement notification is additionally gated on the CONNACK, and
  a write can lose to a concurrent close. A refused ownership claim is retried while this
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
     courtesy write, which is deadline-bounded rather than able to defer the teardown
     indefinitely (F-035). That 0x89 goes through the shared claim like every other
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
       the session and closing a live connection before its own write lands. Narrowed
       (10164 now defers instead of disposing) rather than closed; see the settlement
       section below. **F-084 narrowed the CONSEQUENCE too**: that close reports `0x89`
       ownership-unsettled rather than `0x8E`, and the connection's Will is CONSUMED rather
       than discarded, so a wrongly-classified live connection no longer loses its Will and
       is no longer told to stop reconnecting.
  8. **An ambiguous probe's fail-closed close parks a memory-only teardown**, so a crash
     before resolution loses the parked Will and the original expiry deadline. That is the
     pre-existing parked-teardown exposure, newly reachable from this path.
  9. **The park after an ambiguous probe is not guaranteed.** Between the sentinel return
     and the close bookkeeping, the periodic pass's bound-uncertain resolver can classify
     foreign and dispose first, reaching the owed-Will arm the Will bullet below names.
  10. **At most one server-DISCONNECT ATTEMPT per connection, which is narrower than one
      delivered packet with the right reason.** The second-DISCONNECT class is closed: the
      notification is a per-connection claim (`mqttClaimDisconnectLocked`) shared by all four
      displacement paths, the parse-error footer, the packet-too-large 0x95 and the shutdown
      drain, so no second server DISCONNECT can follow a first on one socket, which is what
      [MQTT-3.14.4-1] forbids. (Stated as codes before F-084; the displacement vocabularies
      now share `0x89`, so the guarantee is about the COUNT, which is what the claim always
      enforced.) Every enqueue-path writer consumes it in the
      same `c.mu` critical section as its write; `shutdownDisconnect` is the exception and
      RESERVES it, claiming under the lock and attempting its capped write
      (`queueOutboundBy`) after releasing it.
      What remains:
      - **Zero notifications is a legal outcome.** The displaced arm closes synchronously
        while the displacer notifies asynchronously, so the socket can be gone first; a
        3.1.1 or size-suppressed build never reaches the claim; a connection displaced
        before its own CONNACK is closed silently by design ([MQTT-3.2.0-1]); and the
        shutdown drain can spend its reservation and then write nothing — suppressing a
        competing displacement DISCONNECT — if the process dies or the capped write fails
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
      - **The notification write is deadline-bounded** (F-035) and the persist
        observation's eviction worker holds no lock across it, but it does still issue
        it inline and it takes `c.mu` to do so, so a stalled client still occupies that
        worker — for a time this design puts **no figure on**, only one a non-reading
        socket can no longer make indefinite. The old overlap with a gate-held probe is
        gone — the worker OBSERVES the gate and defers to a holder — at the price that
        the deferral can lose the eviction (bounded retries, then a refused exit).
      - **In-process, so no wire, stream or record state is involved** (see (6)). Every
        broker is built from this tree (Compatibility boundary), so the claim is not
        qualified by a rollout.
  11. **The gate hold is bounded against non-blocking writers only.** "At most one probe per
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
  12. **A takeover still mid-flight can have its Will published by this connection's
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
  priority gives more occasions of this same residual — not a cost-free arm (residual 11
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
  in the pinned floor since 2.14.4 and re-verified on each bump by `make depproof`),
  with two discriminating mutations and a control run on the pre-fence generation.
  That route is closed. Seven further shapes are refuted in
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
  but each of those findings owns its own consequence, and **all of them have now been
  audited on their own terms.** F-032 closed producing F-062, which closed on 2026-08-18;
  F-033 closed producing F-063, F-064, F-065 and F-066; F-034 closed on 2026-08-22
  producing F-067 and F-068. It also
  does not claim the two backstops below
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

- **Administrative loss reverses direction.** Pruning facts, or resetting the bundle,
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
nothing and persists with its consumers until an operator resets the nine-stream
bundle — the
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
identity as its own parent (F-029b). Its `RWID` field outlived that reader and has since
been **REMOVED**, by the vanished-record item that was the last candidate to give it one:
a write identity names a record's bytes rather than one occurrence of them, so it could not
tell which occurrence a retirement removed. F-036 was the other candidate and failed
earlier — its 2026-08-27 attempt disproved the predicate, since a one-slot-per-client
watermark makes `ackWID == rwid` recreate the superseded-watermark false negative. What the
field also enforced, that a record carrying no `WID` fails its teardown closed, is now an
explicit precondition in `recordGenRetirement`. Nothing ever reaps a watermark. See
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
  fresh-generation record; that resolves as the accepted convergence (whichever broker
  holds that record observes the move and concedes, its client reconnects with Session
  Present false, and generation-scoped deletions cannot touch that generation's own
  resources). The concession is driven by the record having moved, not by the concluding
  broker identifying a successor, which it cannot.
- **A finish can outrun the survivor's own observation.** Whoever finishes the
  condemnation marker deletes the record, and a bound, silent Keep Alive 0 client on the
  survivor's broker that dropped the marker observation then faces an ABSENT record — the
  F-036 shape, whose §0.2 item is now CLOSED
  (`docs/findings/ha-arbitration.md#f-036`): unit 2's licensed retirement converges that
  client where a valid watermark's `mseq` exceeds the survivor's own sequence, and leaves
  it stranded where no such licence exists, which is the residual stated with it. The
  sweep defers its finish by a pass to narrow the case it owns; a CONNECT adoption
  finishes immediately by design.
- **A whole-bundle reset loses every open fence**, along with everything else the nine
  streams hold. `$MQTT_teardown` recreated ALONE is no longer admitted — it used to be,
  stamping the current epoch and starting clean with the fences silently gone, and that
  admission is exactly the residual F-066's enforcement withdraws: an account holding
  eight of the nine is now refused before anything is provisioned. Watermark storage is
  permanent and bounded by the client-ID population.
- **An orphaned PUBREL durable is now reaped, and the PUBRELs it held go with it.**
  A `$MQTT_PUBREL_<idHash>_<gen>` consumer on the interest-retained `$MQTT_out` whose
  session generation is absent from the sweep's evidence — the persisted records plus the
  sweeping broker's own residents, which is what "authoritatively absent" means here and
  which does **not** observe a session live on another broker whose record was removed
  (F-036's false-absence inference, now GATED: since unit 2 this delete additionally
  requires a valid retirement watermark naming the candidate generation, and retains the
  consumer with its PUBRELs without one) — was previously referenced by nothing and
  accumulated forever; the orphan sweep now deletes it after a complete reference walk,
  a full stable-observation grace, a final authoritative record re-read and an
  instance re-lookup. The undelivered PUBRELs it was holding are dropped with it, which
  is correct in every state those checks can observe — no observable session could
  deliver them and their packet identifiers died with the session. Two windows are
  narrowed rather than closed, because a JetStream consumer delete is by name and
  unconditioned: a record save can land after the final re-read (no leader transition
  needed — a broker pausing between the consumer create and the record save suffices),
  and the durable can be deleted and recreated after the instance re-lookup. (These used
  to be stated as "the same bound the affine reaper has"; that reaper is gone, so they
  stand on their own.) The consequence of losing that race is
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
recreating `$MQTT_qos2in` by itself is no longer a SUPPORTED operation — the nine
broker-owned streams are one recreation unit, and an account holding a proper subset is
REFUSED at the next manager initialization — so the supported operation is an
all-brokers-stopped reset of the whole bundle (`docs/ARCHITECTURE.md`, "The nine streams
are one recreation unit"). Refused is not prevented: nothing stops an administrative
deletion, a manual recreation, a partial restore, or a config mutation behind an already
initialized manager. The refusal is what a broker does when it next starts. Two
neighbouring consequences of the same condition (a delivery
after discard, and the orphan sweep condemning a live remote generation whose record is
absent) belonged to **F-036**, now CLOSED: unit 2's release classifier refuses the
delivery for the first, and its retirement proof gate retains rather than condemns for the
second. The cutoff had widened the first of them, since a spared hold stays serviceable by
that peer — which is precisely the population the classifier answers.

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
  never competed for, and was therefore deliverable after the discard until F-036 unit 2's
  retirement classifier refused exactly that population (the watermark is recorded before
  any teardown deletion, so it is observable there); and the claim
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
resetting the whole nine-stream bundle — recreating this stream alone is REFUSED at
the next startup, since the broker-owned streams are one recreation unit; it is not a
rolling upgrade.

### Broker-owned stream prerequisites: startup refusal, a snapshot, and dropped fallbacks

Every broker-owned `$MQTT_*` stream is now validated at account-manager
initialization against the list its own protocol derives (see "Stream
prerequisites" in `docs/ARCHITECTURE.md`). This extends the startup-refusal
surface that previously covered five streams to **all nine** — the provisioning
table's closed set, which its own test asserts. Separately, and not as a tenth
member, the retired `$MQTT_origin` is refused by a **lookup-only** preflight run
before the provisioning loop, because a table entry with an always-failing
validator would provision the very stream it refuses. The consequences are the
ones the `$MQTT_qos2in` precedent established:

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
  **stop-all**: stop every broker for the account, delete **all nine** broker-owned
  streams, then restart, which provisions a fresh bundle. The errors offer no in-place
  fix and no partial procedure, because there is none: the nine are one recreation unit
  (F-066).

  **What the enforcement catches, and what it does not.** Two mechanisms, neither
  subsuming the other. A COUNT rule in a lookup-only preflight refuses an account holding
  one to eight of the nine — a member ABSENT when a broker starts, which is what a
  deletion nobody replaced or an interrupted first start leaves — and its value is that
  the broker no longer silently recreates it. A per-stream EPOCH stamp, minted by
  `$MQTT_sess` and compared by every validator, refuses a complete bundle assembled from
  different incarnations — a partial restore, or a replacement created without the
  account's stamp. **Neither catches an operator who deletes a member, recreates it by
  hand and copies the surviving epoch onto the replacement**: the count sees nine and the
  epoch sees agreement. That is forgery, outside what this enforcement claims.

  Residuals of that enforcement, stated rather than implied:

  - **A reset discards every client's permanent CONNECT acceptance history**, since the
    facts live on `$MQTT_sess`. It does NOT make a pending delayed Will fire: the reset
    takes `$MQTT_willq` too, so the Wills those facts would have suppressed are discarded
    along with the evidence. The fire outcome requires losing `$MQTT_sess` while KEEPING
    `$MQTT_willq` — a partial operation, which is what the enforcement refuses, and which
    an administrative deletion or partial restore can still produce. The reset also
    discards every open generation-retirement fence with `$MQTT_teardown`.
  - **Restore skew within ONE incarnation is undetectable.** A common epoch proves the
    nine belong to the same bundle; it does not prove nine independently restored
    snapshots came from one coherent instant. Two backups taken minutes apart carry the
    same epoch and are admitted while being mutually inconsistent.
  - **The preflight is nine independent lookups, not an atomic snapshot.** Under
    create-only concurrency an admission cannot wrongfully succeed. Concurrent DELETION
    can defeat it: an operator deleting streams behind the scan, leaving `$MQTT_sess`
    for last, can be observed as nine-present, after which the broker adopts the
    surviving epoch and recreates the other eight EMPTY under it — a mixed old/new
    bundle, admitted. This requires violating the all-brokers-stopped precondition the
    procedure states, and it is why that precondition is load-bearing rather than
    advisory.
  - **A concurrent first start can refuse one broker.** Two brokers starting together
    against an unprovisioned account can have one observe a partial bundle and refuse;
    `Server.Start` treats a failed initialization as fatal and offers no retry, so
    recovery is a **restart** and depends on the supervisor (the shipped Deployment
    restarts the pod). It recurs after every whole-bundle reset, not only at first
    install. If the broker that admitted zero dies after creating one to eight streams,
    every peer refuses and the documented reset is required.
  - **The reset recreates streams with broker defaults.** A supported administrative
    variation such as a `Sources` topology on `$MQTT_rmsgs` is not reconstructed and is
    the operator's to reapply.

  `$MQTT_origin`, once a member of the old `$MQTT_sess`/`$MQTT_teardown` unit, is no
  longer defined by this broker: an account still holding it is REFUSED at startup with
  delete guidance rather than validated. This is not a rolling
  upgrade. A *conforming* deployment restarts exactly as before —
  default broker-created streams are conforming, and so are supported administrative
  variations such as a source on `$MQTT_rmsgs`.
- **Startup validation is a snapshot.** A stream config mutated *after*
  initialization is not re-checked until the next process start or manager
  recreation. Runtime guards exist exactly where the code already has them: the
  drain refusal (both entry points) and the shared-subscription guard for a
  stream-forced `ConsumerLimits.InactiveThreshold` on `$MQTT_msgs`, and
  JetStream's own consumer-create rejection for a `MaxAckPending` limit below
  the broker's cap. That list is where the guards are, not where they would all
  be needed: the affine owner's normal create on `$MQTT_out` has none, so a
  post-initialization stream limit is substituted into a durable that requested
  zero and the owner never reads it back. The equivalent refusal exists only in
  `recoverQ2AffineStorageConflict`, which is entered after a create has already
  failed. That is F-096, and it is affine-only. A persisted "validated" flag was
  considered and rejected: it goes stale against exactly the mutations it claims
  to cover.

- **A server-side consumer cap is invisible to this validation.** Four of the
  validators require their stream to leave `MaxConsumers` unlimited, because each of
  those streams accumulates consumers the broker cannot bound; F-095's dossier holds
  the canonical stream-by-stream inventory. From the nats-server v2.15.0 line that is
  exactly the shape that arms a server-wide `default_max_consumers`, whose own default is
  **1000 per stream**: both create paths apply it whenever the stream's and the account
  tier's `MaxConsumers` are both `<= 0`, and upstream treats `-1` as "not set" so neither
  can opt out of it. The option is carried on neither `STREAM.INFO` nor `$JS.API.INFO`, so
  startup validation cannot see it and does not claim to. The cap is charged on **new
  consumer admission only** — an adopt of an existing name is exempt — so steady state and
  a resume whose durables are all intact are unaffected. What fails is a create of a
  consumer that is not there yet on a stream already at 1000: a SUBSCRIBE refused with
  `JSMaximumConsumersLimitErr` (10026) on `$MQTT_msgs`, and on `$MQTT_out` a new QoS 2
  SUBSCRIBE, a restoration that must create a missing durable for a saved filter, or an
  accepted-but-stalled recovery after CONNACK. F-095's dossier holds the full symptom
  list; every case has every stream setting conforming and nothing in the stream shape to
  explain it. Every JetStream peer must be configured
  `jetstream { limits { default_max_consumers: -1 } }`, or the account tier must carry an
  explicit positive `max_consumers` above the deployment's working set — a deployment
  prerequisite beside the cluster-wide version floor, and unenforced. It is stated
  unconditionally because the floor is 2.15.0: every supported peer has the option.
  Tracked as F-095.

- **This project's own external conformance environments do not yet meet either
  prerequisite, and cannot.** The two CI conformance jobs (`.github/workflows/ci.yml`)
  and `test/conformance/docker-compose.yml` launch the official `nats` image, whose
  newest published tag is **2.14.6** — there is no 2.15.x image, not even for the RC, so
  those harnesses run **below the documented floor** and without
  `default_max_consumers: -1`. Adding the key today would be worse than omitting it: a
  2.14.6 server raises `unknownConfigFieldErr` for it and refuses to start. This is a
  stated consequence of raising the floor ahead of the 2.15.0 GA, not an oversight.

  **The exception is pinned rather than drifting, which is what makes it an exception
  and not an accident.** All three launches name `nats:2.14.6` *by digest*. `latest`
  would have crossed the floor by itself the day 2.15.0 publishes — arming the
  1000-consumer cap in CI with no repository change and no paired configuration edit —
  so the deferral would not have been controlled and two environments could disagree
  about the server they ran. Pinning makes the upgrade an explicit, reviewable change.

  **The three launches do not share one remedy**, which matters for whoever performs it.
  The Paho job and the Compose environment mount `test/conformance/nats.conf` and would
  read the limit from it; the Rust conformance job runs `-js` with **no configuration
  file at all**, so editing that file does nothing for it and it needs its own compatible
  config plus a `-c` invocation, preserving its unauthenticated setup. Correspondingly,
  adding the key early would break the two config-reading launches, not all three. The
  roadmap item carries the ordering and the per-launch detail. Until it is done the
  conformance suites establish MQTT protocol conformance against a server below the floor;
  the JetStream contract itself is covered separately by the embedded-server suites and
  `make depproof`, which run at the audited pin.

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
  until the stream is fixed. Fixing it means the whole-bundle reset per the
  compatibility boundary — all nine broker-owned streams are one recreation unit, so
  recreating the offending stream alone is refused. The support floor is nats-server
  2.15.0, which always understands `AllowMsgTTL` — as did every release back to 2.14.5,
  so raising the floor neither gained nor lost anything for this row.
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

### A resumed shared subscription proves its group durable, repairs it, and arbitrates

A resume verifies the JetStream group durable behind every shared subscription whose
consumer configuration is still resident on this broker, and refuses the CONNECT before the
CONNACK when it cannot prove the durable exists and is usable (F-076). It then proves that
this broker still **owns** the membership it is restoring, through a classified record write
under the accept transaction's CAS (F-077). Previously such a resume was accepted with
Session Present = 1 and a subscription on a subject nothing publishes to.

**The self-repair residual is closed for one verdict and retained for the others.** An
**authoritatively absent** durable is now recreated rather than refused, so a session whose
group consumer was deleted out of band recovers on its next reconnect instead of refusing
forever. The other two verdicts keep their refusals, and the reasons are the ones that made
them distinct in the first place: a **present but non-canonical** durable may be serving
other members, and a configuration this broker would not have built is not one it may
normalize on their behalf; an **indeterminate** reading established nothing at all, and
recreating on it would turn one broker's transient error into a group-wide change. Both
still recover as soon as the condition clears by any means, and a clean start always
recovers at the cost of the session.

**The messages published while the durable was absent are gone, and the repair does not
recover them.** The messages stream is interest-retained: with no consumer there was no
interest, so nothing was kept. What the repair restores is future delivery, not the backlog.

**Ownership is proven at the write's linearization point, not continuously.** A takeover
that lands after a landed ownership write leaves this broker serving a membership it no
longer holds until it learns otherwise, which is the session-record watch's and its
committed-read backstop's convergence window rather than an admission check.

**The proof does not extend to the consumer.** `CONSUMER.CREATE` and `CONSUMER.DELETE` carry
no precondition field, so nothing binds the durable's existence to the record's ownership.
A durable deleted — or altered into a shape this broker would not provision — at any point
after the last verification or write receives no further check on that operation — the
interval between the ownership write and the CONNACK on a resume, and between the ownership
write and the SUBACK on a live SUBSCRIBE. What detects it later is an operation reaching
the create-and-verify arm: a resume, whose pre-pass classifies every resident shared sid,
or a SUBSCRIBE for a filter this session holds no resident entry for, which creates the
durable and thereby proves it. A **re**-SUBSCRIBE on a filter the session already holds
takes the refresh arm and verifies nothing.

**A durable this broker created and then lost the arbitration for is left in place** — on
**either** path. The live SUBSCRIBE's `!exists` create and its post-subscribe verify
`CreateOrUpdate` produce the same state as a resume's, and the verify create can move
group-wide settings before the arbitration is lost. Every
available deletion is refuted: the last-holder scan skips same-client-ID records, so after
a takeover a blind last-holder delete destroys the **new** owner's durable. Leaving it is
the least unsafe option and it is not inert — it holds stream interest, it can accumulate
pending messages for at least the orphan sweep's grace window, and a later joiner to the
same deterministic name may receive messages published while no authoritative membership
existed. It is reaped by the orphan-consumer sweep **only if no record references it**, only
after that grace, and only on a complete fail-closed walk.

**A refused resume is answered, but never with a DISCONNECT.** The displacement
DISCONNECT is gated on the CONNACK having been written, which is false for every
refusal inside the CONNECT, so no `0x8E` or `0x89` DISCONNECT is ever sent for one.

What the client gets instead is a v5 **failure CONNACK** carrying a legal reason code
and a Reason String — **within the post-accept-save, pre-success-CONNACK window**,
which is the scope of this claim and no more. In that window the sites are the
register recovery, the restore path, the shared-membership ownership write, the plain
QoS 1/2 reconciliation verdicts and the lifetime grant, and the only one still silent
is the **failed success-CONNACK write**, whose channel is the thing that failed.

**Exits EARLIER in the CONNECT stay silent, and that is a stated residual rather than
an oversight.** A CONNECT refused before the accept save — an unresolved record state,
or a post-clear failure that disposes the old client — returns without a CONNACK.
Those exits precede the point at which this connection is bound and its session
accepted, which is the boundary the signal was scoped to when F-050 introduced it;
generalising past it was never part of this work.

A **3.1.1** client still gets a bare close wherever it is refused: its CONNACK has no
reason code above 5.

Three residuals attach to that signal.

- **It deliberately UNDER-claims.** A disposing refusal says "session ownership could not
  be confirmed" even where a child-shaped body was observed. Only a body claiming our parent
  gets the more specific "ownership could not be established; try again". **The DISCONNECT
  and log paths no longer over-claim in the other direction** — F-084 withdrew the takeover
  assertion from every route that cannot hold the successor — so what remains here is one
  direction, not two.
- **The Reason String can be dropped for a client with a tight Maximum Packet Size.**
  `mqttMakeConnAckV5Frame` drops it **wholesale** rather than truncating when the frame would
  exceed the client's limit, and suppresses the frame entirely if it still does not fit. Such
  a client learns the reason code and nothing more, or nothing at all; the refusal proceeds
  either way.
- **The failure CONNACK's write no longer defers this frame indefinitely, but the frame
  is not bounded.** `enqueueProtoErr` reaches `writeSocket`, which installs
  `mqttWriteDeadline` (F-035), so a non-reading client can no longer stall that write for
  the life of the connection. The signal precedes the disposal by design, so such a client
  still delays this frame while it holds the session gate, the open record transaction
  and, for the reconciliation and lifetime sites, the session's residency and its binding
  — and a local same-client-ID CONNECT waits behind it. **No figure is claimed for that
  delay.** The deadline starts only once `wmu` has been acquired, so it bounds neither the
  preceding mutex waits nor anything else in the frame, and nothing bounds how many
  bounded writes one critical section may contain. What changed is that a non-reading
  socket is no longer an unbounded source of the delay; that is the whole of it. Remote
  durable ownership is unaffected either way: it is arbitrated by the record CAS, not by
  the gate. The ownership gate and the transaction were already held across this write at
  every pre-existing refusal site, so what is new is residency and binding at those two.

**A delivery admitted before the operation gate was armed can still escape it.** A live
SUBSCRIBE arms its gate before it installs anything, but a delivery callback that has
already completed its final gate check and is then descheduled before its irreversible send
cannot observe a gate armed in between — there is no further wake, and shared QoS 0 does not
wait at all. Worked: a QoS 1/2 callback reads both gates open and is descheduled; the
readLoop begins a shared SUBSCRIBE and arms generation N; the callback resumes and
transmits. **The direction is what bounds it**: because arming precedes every install, that
callback was admitted by interest which *predates* the packet — authorised by whatever
operation installed it, the CONNECT that F-077 arbitrates or an earlier SUBSCRIBE that this
one does — and never by the un-arbitrated interest the packet is about to install. It
therefore belongs to the point-in-time-ownership convergence window below, not to F-078's
new-interest guarantee. Closing it entirely needs a real callback-admission boundary, whose
cheap form (a counter registered after the check) is already refuted: a callback can be
descheduled between loading the verdict and registering.

**The operation gate's collateral cost.** While a shared SUBSCRIBE is unresolved, the
connection's *other* QoS 1/2, PUBREL and affine deliveries wait too, and its shared QoS 0
deliveries are dropped rather than waited on — the gate is packet-scoped, not
subscription-scoped, because a shared QoS 2 SUBSCRIBE installs delivery interest before any
subscription exists to hang a flag on. Plain QoS 0 is ungated throughout. A mixed packet
therefore gates plain QoS 1/2 traffic on a shared filter's arbitration.

**A foreign verdict leaves a brief window before the session is detached.** Between the
verdict and `sess.c` being cleared, the session's other shared subscriptions are still live.

**A quiet packet opens its gate even when it left an ancillary record write ambiguous.** A
live shared SUBSCRIBE whose filters all fail may leave such a write behind without installing
or reviving any shared membership — a shared QoS 2 filter provisions the session-affine
delivery path before the group consumer is created, so a provisioning save whose outcome is
never learnt fails the *filter* and continues. Its operation gate opens, because the packet
created no shared interest for the arbitration to be about. The read loop attempts uncertainty
resolution synchronously before enqueueing the failure SUBACK, but **that attempt can itself
return unresolved**, in which case the session stays uncertain and existing delivery interest
may go on serving until ordinary settlement or an ownership observation converges. That
interest predates the packet and belongs to the point-in-time-ownership convergence window
above; F-078 makes no stronger guarantee for it, and withholding it here would give one packet
type a guarantee this broker provides for no other ambiguous record write — while converting
an ambiguous affine provisioning failure into a connection-level refusal, which is a contract
the affine items own.

**What the verification establishes, and what it does not.** It is evidence about the
durable's *configuration*. Ownership is the ownership write's to establish.

**An unsupported record shape is refused rather than repaired.** A session record
carrying a shared group consumer configuration in `Cons` is a format this broker never
writes — shared membership is held in `SharedSubs` alone — and every path that reads one
refuses it and
leaves the record untouched: the CONNECT restore, both expiry paths, the orphan-consumer
sweep, the shared record validator, and the shared last-holder scan. The two paths that
decide by **absence of a reference** across the whole record set refuse in the way that
withholds their destructive act, because neither may act on a partial view: the
orphan-consumer sweep **aborts its whole pass**, and the last-holder scan **reports every
requested durable held**, which withholds the group-consumer deletion. The recovery is the
compatibility boundary's: stop the account's brokers and
reset the bridge-owned `$MQTT_*` streams — all nine, which are one recreation unit.
This is not a rolling upgrade.

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

### A client that cannot absorb a write within the deadline is disconnected

Every MQTT application write is bounded by `mqttWriteDeadline` (10s, no operator knob),
installed and cleared in `writeSocket`; see `docs/ARCHITECTURE.md`, "Every MQTT application
write is deadline-bounded". This restores upstream nats-server's per-flush deadline and its
`SlowConsumerWriteDeadline` close, which this port dropped when it replaced upstream's
`writeLoop` and outbound buffer. Before it, a client whose socket send buffer stayed full
blocked its writer — and any lock that writer held — for as long as the connection lasted.

What the new behaviour costs, stated:

1. **It is a WRITE timeout, triggered by the broker having something to send.** A silent
   idle client is unaffected; a client that negotiated Keep Alive 0 and reads nothing
   is still bounded by nothing until the broker next writes to it.
2. **Session expiry zero** initiates the ordinary session discard on that close. A surviving
   session follows its existing persistence and expiry paths.
3. **Surviving plain QoS 1/2 exchanges** keep their ordinary unacknowledged/recovery
   semantics, subject to expiry and to the existing recovery limitations — not "redelivered
   to the next incarnation".
4. **Shared-group pending work may redistribute** to another member: the ordinary close
   purges shared pending state without acknowledging the group message and without
   destroying the group durable. Shared QoS 2 keeps its existing affinity rules.
5. **The Will follows abnormal-close classification**, and the timeout is a NEW producer of
   the documented "early close" that the already-owed-now takeover exception names. A
   delivery can hold `c.mu` while a displacer waits to discard or consume the Will; the
   timeout closes the socket before that Will phase runs, and the readLoop can then race the
   displacer. A peer reset could already produce that schedule — what is new is that a peer
   which is still connected can produce it.
6. **A frame can be truncated before the close** when a write times out mid-frame.
   Pre-existing on a peer reset; newly reachable on a timeout. A PUBLISH header and payload
   are separately bounded, so one frame gets two deadlines rather than one.
7. **No caller learns a new return outcome**, only a new cause of an existing failure. In
   particular a delivery timeout does not make `mqttEnqueuePublishMsgTo` return `false`,
   whose caller reads `false` as a packet-size discard and acknowledges and removes state.
8. **No gate-duration bound is claimed.** The deadline removes indefinite blocking
   caused by a non-reading socket; it does not bound how long the gate is held. A `c.mu`
   section can contain many bounded writes (the retained flush plus the live PUBLISH under
   one hold), and a gate-held Will phase can wait on `subsBarrier.wait`, which has no
   elapsed-time bound at all. Both are stated in `docs/ARCHITECTURE.md`, and the second
   is a filed roadmap item.
9. **Two pre-existing durable-loss exposures become MORE REACHABLE, not new.** A Will living
   only in client or disposition memory when the socket closes, and the parked-teardown
   exposure behind an ambiguous probe, now both have timeout-triggered schedules. The
   related `FireAt` defect this item made more reachable — a delayed Will's deadline
   computed at CONSUMPTION rather than at the close — is **now FIXED**: the delay is
   measured from the observed close. What that fix does **not** bound is the interval
   between the close and the observation itself; see below.

### A connection that never sends CONNECT is closed; a connected one may still be silent forever

MQTT 3.1.1 / 5.0 [3.1.4]: if the Server does not receive a CONNECT Packet within a
reasonable amount of time after the Network Connection is established, the Server
SHOULD close the connection. The broker's timer implements that SHOULD **for a connection
that stays silent or incomplete** — the qualification matters, and the paragraphs below give
the case it excludes. Such a connection is closed after `-connect-timeout`
(default 5s, `MQTTNB_CONNECT_TIMEOUT`); the window is absolute rather than
restarted per byte received, and the bound is fail-closed — a value of `0` or
below is refused at startup rather than read as "unlimited". See
`docs/ARCHITECTURE.md`, "Connection liveness bounds".

**If the expiry callback transitions the state before admission**, the racing CONNECT is
refused before any authentication is attempted, so a connection the deadline closed never
dials a per-client NATS connection and never binds session state. That conditional is
load-bearing: the other order is reachable and is described below. CONNECT authentication
itself is unchanged either way: credentials are still adjudicated by the configured client
dialer.

**What "closed after `-connect-timeout`" does and does not promise, since the two are easy
to read as one.** The bound makes a silent connection's LIFETIME finite: the timer callback
closes the socket whenever it is scheduled, so no such connection is held indefinitely, and
that is the resource guarantee. It does **not** guarantee that a CONNECT arriving after the
configured window is refused. The arbitration is a race between two facts under `c.mu` —
the CONNECT arrived, or the deadline expired — and admission tests only whether the callback
has ALREADY transitioned the state, never whether the window has elapsed. So a CONNECT that
takes the mutex before a delayed callback does is admitted and served, and the effective
admissibility bound is the configured value plus however late that callback is scheduled.

**This is a known gap and it is NOT accepted**: an operator setting `-connect-timeout`
reasonably expects the second promise as well as the first, and the fix is for admission to
consult the elapsed window rather than the callback's state. Nor is the timeout a strict
pre-authentication admission bound, which is the reading to guard against.

On [3.1.4]: it is a SHOULD, and the timer generally implements it — a connection that
stays silent is closed. **What is not claimed is that the late-admission case honours
the configured window**: there the callback finds the CONNECT already admitted, does
nothing, and the connection proceeds under the keep-alive rules instead — which are
themselves no bound at all when the client negotiates Keep Alive 0, as the next residual
states. So nothing closed it for exceeding the window. The finding specifying the fix is
filed separately from the change that added this paragraph.

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

### The persisted-identity census is complete over a stated span, not over everything

`docs/findings/persisted-identity-census.md` enumerates what durable state this bridge
holds and which of its identities another stream issues, and eight tests enforce that
enumeration rather than leaving it to care. What it claims is bounded, and the bound is
stated here rather than left to be inferred:

- **Mechanically enforced, over a derived type universe:** every field of every
  persisted body type, plus `StreamConfig`, `ConsumerConfig`, `ConsumerInfo`,
  `SequenceInfo` and the configuration types reachable from them — including unexported
  fields, with each field's encoding recorded, and with the type universe DERIVED by a
  recursive walk from the persisted roots rather than hand-listed. A type the walk stops
  at must declare why.
- **A drift GUARD, not a closure proof:** the interpretation surface, 161 sites derived
  by parsing the package with `go/ast` and checked against the manifest in both
  directions. Its root set is hand-maintained and it keys a site by unqualified function
  name, so three kinds of drift still pass — a new branch or call inside an
  already-named function, a new wrapper around the transport primitives, and a same-named
  method on another receiver.
- **Named and NOT enumerated field by field:** message headers and subjects. Three of
  F-066's fourteen sample rows are deferred to named families, and the test requires a
  deferral to carry its family and reason rather than allowing a row to be silently
  absent.

**So F-066's completeness deferral is NOT discharged**, and the census says so at its
head rather than in a footnote. The plan's own rule is that an unproven closure class
means claiming neither completeness nor that discharge, and the interpretation surface is
such a class: a guard that catches real drift is not a proof that no drift is possible.
F-066's remedy USED TO depend on the population and would have inherited the same bound.
**It no longer does, and the enforcement is LANDED**: the all-nine recreation contract is
keyed on the STREAM set rather than on the identities those streams carry, so an unclosed
interpretation surface does not bound it. The census's own undischarged deferral above is
unaffected and stands exactly as written — it is simply no longer on F-066's path.

The reason for stopping there rather than claiming more is the history of this
enumeration. Four earlier attempts were each falsified as a filter presented as a sweep,
and two were refuted by an entry in the table they themselves introduced. A stated partial
span that a test defends is worth more than a total claim that a fifth review round
falsifies.

### A session teardown fails closed on an unreadable in-flight register

**Newly reachable, by construction, from a store written before the exchange lineage
existed.** The in-flight register's `Lin` is mandatory and an absent or malformed one is
REFUSED rather than defaulted, so such a record now loads unreadable and reaches this
residual — which is the compatibility boundary working as specified, not a new defect and
not a rollout condition. There are no releases and no mixed fleet: a record without a
lineage means the development store predates the design and must be recreated. The
mechanism is in `docs/ARCHITECTURE.md` under "An exchange's LINEAGE is carried, where its
nonce is deliberately re-minted"; the residual itself is unchanged and is stated below.

F-034 replaced the blind `$MQTT_inflight` filter purge at teardown with a pass that
enumerates, loads, checks each occupant's identity, and deletes only at or below the
watermark its own enumeration returned. That pass **cannot** proceed past a register it
could not read, and the consequence is a deliberate liveness cost rather than an
oversight.

**What happens.** An unreadable or indeterminate register makes `deleteTeardownTargets`
return an error, so the session record is **not released**. Every deletion that already
landed stays landed, so progress is real.

**Whether anything retries is BRANCH-SPECIFIC, and the table below is the statement of
record.** On a record-backed teardown the generation-retirement watermark stands and the
record stays marked, so a later attempt re-enumerates and finishes the remainder. On the
**no-record** branch (`mqtt.go:9706`, `seq == 0`) there is no record and no marker to
anchor anything: that branch returns before the claim, so nothing re-drives it and cleanup
falls to the orphan sweep alone. An earlier draft of this passage said the marker stands
and every caller retries, without that split; it was wrong for one of the three call sites
and is corrected here.

**Who is affected.** Every caller of that path: the ordinary discard, a teardown
**adopted** from a broker that vanished, session **expiry**, and the **orphan sweep**'s
reach into the same state. None of them completes a teardown on a state it could not read
— which is the guarantee. Which of them can RETRY it is the separate question the second
table answers.

**And the wedge reaches the shared group durables too.** At all three call sites —
`mqtt.go:9706` (no record), `mqtt.go:9794` (ordinary discard) and `sess_teardown.go:1052`
(adopted teardown) — `deleteTeardownTargets` runs BEFORE `releaseSharedTeardownDurables`
and its error arm returns immediately. The blind filter purge it replaced could not refuse,
so this is new with F-034.

Four earlier drafts of this passage each overstated something, so it is now a table of
outcomes rather than prose. **What a refused reap leaves**, in the order
`deleteTeardownTargets` performs it (`sess_teardown.go:730` onward):

| Step, in order | On a refused reap |
|---|---|
| `$MQTT_msgs` subscription consumers | delete ISSUED; confirmed only when `noWait` is false |
| the PUBREL consumer on `$MQTT_out` | delete ISSUED, same caveat |
| affine assignment claims | `discardQ2AffClaimsFor` has marked them DISCARDED; the reap of each is conditional, so a discarded-but-retained claim can remain |
| the affine consumer on `$MQTT_out` | delete ISSUED, same caveat |
| inbound QoS 2 release claims | `arbitrateQoS2ReleaseClaims` has run and may deliberately leave a hold and claim for the orphan sweep |
| **the register reap** | **REFUSES here — this is the abort point** |
| the shared group durables | never reached on this attempt |

**What can be retried, and by what.** This is where the drafts went wrong twice:

| Call site | Marker/record after a refused reap | Adoption retry possible? |
|---|---|---|
| `mqtt.go:9706`, no record (`seq == 0`) | none exists — `releaseTeardown` is not part of this branch at all | **No.** Nothing anchors a retry; the orphan sweep is the only path |
| `mqtt.go:9794`, ordinary discard | record and marker stand, since `releaseTeardown` was never reached | Yes |
| `sess_teardown.go:1052`, adopted teardown | as above | Yes |

**And a successful attempt is not atomic either.** `releaseTeardown` removes the record
BEFORE the shared release loop runs, so if that loop deletes some durables and then fails,
the marker is already gone and no adoption retry can re-run it. **Cleanup does NOT fall to
the orphan-consumer sweep** — an earlier statement here said it did, and that is false for
a `$MQTT_msgs` group durable: the only route that deleted a consumer from a candidate
listing on that stream was removed, and the sole surviving delete there targets
`$MQTT_out`. The real disposition is that the durable **survives until a member rejoins
that `$share` group and adopts it**, retaining every matching publish meanwhile because
`$MQTT_msgs` is interest-retained. A durable another member still holds is deliberately
retained, not deleted: the last-holder scan decides, and for a group with other members
the answer is to keep it. An earlier draft's "released once the register can be read" was
simply wrong.

**The cost, stated.** If the register is *permanently* unreadable — a shape no broker in
this tree writes, which is what makes it unreadable — that client ID's teardown never
completes on its own. The record stays marked, the client's next CONNECT is refused
rather than resumed hollow, and **automatic convergence is impossible until an operator
removes or repairs the record**. The diagnostic names the stream, the subject and the
sequence for exactly that purpose.

**Why this is the right trade.** The alternative is releasing the session record while a
register of unknown status remains, which asserts that nothing is in flight on the
strength of a read that failed, and which can free a packet identifier the client is
still holding. A refusal an operator can see and repair is preferable to a silent loss,
and it is the posture this tree already takes toward a persisted shape it cannot
interpret (F-056, F-057, F-058).

### A group durable owed a last-holder delete is retained while a record is unreadable

`sharedDurablesHeldByOthers` decides whether any session other than this one still holds a
group durable, and five release paths delete the group consumer on a "no": teardown, a
failed shared SUBSCRIBE's rollback, the 3.1.1 forced leave, the per-consumer release, and
UNSUBSCRIBE. Because `$MQTT_msgs` is interest-retained, that deletion destroys every
member's backlog with it, so the scan now **fails closed** — an indeterminate walk, a record
whose bytes do not decode, and a record carrying shared group consumers in `Cons` each
report every requested durable held. Previously only the first did; the other two were
skipped, and absence was then reported out of a record set that had not been read.

**What that costs, stated three ways.**

1. **Nothing reclaims the retained durable.** The orphaned-consumer sweep is the only other
   path to it and it **aborts** on the very same record, so a durable spared here stays
   until an operator repairs or removes that record. It is not inert: it holds stream
   interest and can accumulate pending messages, and a later joiner to the same
   deterministic name may receive messages published while no authoritative membership
   existed. The diagnostic names the stream, the subject and the sequence for exactly that
   purpose.
2. **The retention is unbounded in count**, not merely bounded in kind. One unreadable
   record spares the durables of every group released afterwards, for as long as it is
   present.
3. **One caller used to withhold more than the durable. That is CLOSED (F-082).**
   `releaseFailedSharedFilter` tested the scan's answer before touching any of its own state,
   so a refusal stranded the resident `cons` entry, the closed delivery state, the pending
   deliveries and the ack-budget accounting as well as the group consumer. It now separates
   the three dispositions the single `held` bit conflates — this session still holds the
   durable through another of its own keys, some other session holds it or the walk refused,
   or the walk reported it unheld — and only the last authorizes a `CONSUMER.DELETE`. The
   same item fixed the larger half the roadmap bullet had not stated: the release re-derived
   its work from `sess.cons`, which the failing attempt has not written to yet, so on the
   common shape it discharged nothing whatever the scan said. **A refusal now withholds
   exactly the group consumer, and nothing else.** What a refusal still costs is items 1 and
   2 above, unchanged.

**What this does NOT close.** The scan is a non-snapshot read that authorizes a
deletion, not
an arbitration: membership and record ownership are arbitrated by the session subject's CAS,
while `CONSUMER.DELETE` carries no precondition binding a consumer's existence to a record.
A joiner persists its membership before creating the group durable, which covers the
principal leave race but not all of it — a leaver can complete its scan before the join
record lands and delete after the joiner's final verification. That residual is unchanged.
Equally unchanged is the **same-client-ID skip** recorded above: the scan skips records whose
`ps.ID` matches its own session, so after a takeover the new owner's record is invisible to
it. The new unsupported-shape check is placed above that skip and narrows only records this
broker never writes, so the residual and F-077's refutations that rest on it stand
exactly as
before. This scan validates neither a client ID that hashes back to its subject token
nor a
generation, which the generation-scoped orphan pass does; that is deliberate, since
tightening attribution here would alter the takeover residual rather than the readability
defect.

**Scoped, because the scope is the point.** A refusal now withholds exactly the group consumer
**at every site that can have a surviving membership** — the failed-filter release's two
halves, UNSUBSCRIBE's two halves, and a SUBSCRIBE packet's rollback's two halves. F-083 closed
the last two by stating the rule as a property over the membership that survives the
operation, rather than as a rule per caller; `docs/ARCHITECTURE.md` carries the
mechanism and
the four authorities. The 3.1.1 forced leave and teardown are **vacuous** for that
property —
neither can have a surviving membership — so neither is exempted from it and neither is fixed
by it. The forced leave's own `cons`-versus-membership target mismatch is **now
closed**: it
derives its doomed configs and its release candidates from the membership keys plus
`cons`, and
its pending purge from the pending maps, at the staging half as well as the commit. Teardown
carried no such defect.

### What the 3.1.1 forced leave still costs after it stopped being a CONNECT exit

The leave is broker hygiene, so its failure no longer closes an acknowledged connection merely
because a write did not succeed. Seven residuals remain, and the first three are the
exits that
survive on purpose.

1. **A post-CONNACK refusal still closes, and therefore still publishes the Will**
   ([MQTT-3.1.2-8] requires it on a close not preceded by a DISCONNECT, and post-CONNACK there
   is no licence to discard it). Three arms reach it: an **unresolvable** ambiguous
   write, a
   **10164** outcome that is local-definite or unresolved, and a **10071** classified as a
   fork or as ownership-unsettled. The first two are the right trade — a connection
   serving a session that can persist nothing is worse — and the third is an ownership
   disposition,
   where stopping is the point. (The third read "classified takeover or fork" before F-084
   withdrew the takeover claim from this arm.)
   Each abandons the restore verdict so withheld deliveries are woken rather than parked.
2. **A leave that never completes leaves the memberships durable and their group durables
   alive** until a later resume re-derives it. This belongs to the refusing arms only:
   on the
   arms that continue, the leave has completed.
3. **The plain QoS 0 drop window is enlarged by the duration of the leave's
   settlement**, for a
   3.1.1 resume carrying shared memberships. The restore verdict now opens after that
   settlement, and QoS 0 has no redelivery to wait for, so it drops while the verdict is
   unopened; QoS 1/2, PUBREL and affine deliveries wait and are unaffected. Serving a message
   under a membership whose ownership is still unproven cannot be undone, and a dropped
   QoS 0
   message is what QoS 0 means.
4. **`resolutionAfterContinuations` considers only uncertainty, not an armed
   rejection.** The
   forced leave samples both itself, so it is covered; the shared helper remains
   incomplete for
   other continuation callers, which were not analysed here. A known gap, stated rather than
   widened under an item that never planned for it.
5. **A late rejection or ambiguity raised by the commit's register cleanup on the CONTINUATION
   path** — where the connection may already be gone — is logged and left to the marker or
   uncertainty the write itself armed. Nothing is serving there, so there is nothing to gate;
   the asymmetry with the in-connection path is stated rather than implied.
6. **A pre-CONNACK ownership refusal can tell a v5 client why, but not "do not retry".**
   `0x8E` "Session taken over" is not a legal CONNACK Reason Code ([3.2.2.2]) and no legal
   code carries that instruction, so the refusal uses `0x83` with a Reason String. A client
   that reconnects will contend for the session again. A protocol limit, not a gap. The
   refusal does not claim a takeover either, and since F-084 neither does the DISCONNECT
   these arms would otherwise have sent: on none of them was a successor established.
7. **PRE-EXISTING and unchanged:** a malformed shared membership key still fails to
   protect its
   group durable against the orphaned-consumer sweep, on any record. The pre-CONNACK preflight
   stops the forced leave from compounding it and makes the condition visible to an operator;
   it does not make malformed keys safe elsewhere.

### A takeover this broker cannot hold is reported as unsettled, and its Will is published

F-084 withdrew the `0x8E` "session taken over" claim from every route that reaches its
verdict through a **record**, because no persisted shape proves a successor:
`createOrRestoreSession` adopts `WID`/`PWID` without validating provenance, valid-shaped
bodies arrive through unconditional publication, and acknowledged bytes are replayed
verbatim. Only an in-process same-client-ID replacement this broker holds both ends of
licenses the claim.

Three residuals follow, and the alpha ships with them stated.

1. **A genuine remote takeover is under-reported.** Another broker really does take the
   session, this broker observes its record, and the displaced client is told `0x89`
   "ownership could not be established; try again" rather than `0x8E` "stop reconnecting".
   The client will reconnect, and against permanently unreadable or unclassifiable state it
   will keep reconnecting. **This is an availability cost, not a state-safety failure**, and
   it is preferred to the alternative: telling a live client its session was taken over when
   nothing established that, on evidence a forged, replayed, unrelated or malformed record
   produces identically.

2. **That takeover's Will is published rather than suppressed.** The same routes previously
   ran the takeover's Will discard. A displacement that cannot prove a successor may not
   suppress a Will — suppressing it would be silent loss on evidence that establishes
   nothing — so the displaced connection's Will now takes ordinary handling: published
   immediately, or enqueued on the outbox against its delay where a qualifying acceptance
   fact may still suppress it. For a real remote takeover whose client reconnects promptly
   the acceptance fact converges it; for one whose client does not, a Will is delivered that
   the pre-F-084 broker would have dropped. **The specified behavior is the Will, and the
   old suppression was the deviation**, but a deployment that relied on the old silence will
   see traffic it did not before.

3. **The in-memory consume window, newly reachable.** `mqttTakeWillLocked` marks the Will
   discharged and returns it, and the publish or enqueue follows outside `c.mu`. A crash in
   between loses that Will. This is the identical window `mqttPublishOwedWill` and every
   ordinary close already have — F-084 did not create it — but it is now reachable on the
   disposal routes as well. Closing it needs a durable intent record written before the
   in-memory consume, which is new durable state and out of scope for this item. The same
   applies to a parked action consumed into a prepared disposition and lost to a crash
   before its discharge: the discharge deliberately does **not** re-park, because phase 1
   has already deresidented the session and a re-parked action would be unreachable by any
   pass.

4. **An already-due parked Will whose immediate delivery fails is LOST, and only logged.**
   `dischargeDeferredWill` consumes the parked action and, on the already-due branch,
   publishes through the outbox's client-free publisher. If that publish cannot proceed —
   an undeliverable topic, a transport failure — the helper warns and returns. Nothing
   retries it, and nothing puts the action back.

   **The absence of a re-park is deliberate, not an oversight.** Phase 1 has already
   deresidented the session, so an action put back there is unreachable by the periodic
   pass and by the next CONNECT: re-parking would convert a logged loss into a silent one.
   Pinned by `TestF084DeferredWillImmediatePublishFailureIsLoggedNotReparked`. Closing it
   needs a durable intent record written before the in-memory consume, which is new durable
   state and out of scope for this item — the same boundary residual 3 sits behind.

Two adjacent outbox behaviors are **pre-existing** and unchanged, and are recorded here only
because these routes now reach them: an enqueue whose retries are exhausted publishes
immediately as a fail-safe, and because the create is then unconfirmed rather than proven
absent, a record that landed with its ack lost fires later as well and the Will is published
**twice**; actual loss requires that fail-safe publication to fail too.

### A drain interrupted by its own session's clear withholds that completion

The drain's finalizer captures its drain and sequence, then does JetStream work to release
the delivery's in-flight register. A `clear()` landing in that window — a discard, an expiry,
a takeover — retires the incarnation the captured state belongs to, and the finalizer refuses
rather than carrying a retired delivery's decisions forward.

Two refusals, and they are visibly different to an operator:

- **Before the release**, the finalize simply reports the delivery as not its own,
exactly as it does for a drain it never had. Nothing is logged, because nothing was
attempted. - **After the release but before the save**, it logs *"withholding the
completion of drained sequence N ... the session was cleared while its in-flight
register was being released"* and leaves the source unacknowledged. The register release
already happened and is not undone; what is withheld is the claim removal and the
acknowledgement.

**The cost** is that the source message stays pending and is redelivered. The retired
incarnation's own claim state is left untouched, so nothing about the fresh session is
disturbed. That is strictly better than the alternative the second refusal exists to prevent:
saving a retired delivery's decisions into a fresh incarnation's record.

**What neither refusal covers** is a genuinely live owner of the same register on another
broker — naming cannot separate that actor from this one, and it stays F-036's predicate.

### A drained delivery with duplicate in-flight registers does not complete

When recovery finds MORE THAN ONE `$MQTT_inflight` register naming one drained delivery on
different packet identifiers, which one the client holds is unknowable — a stream sequence
orders the writes and not the sends. The pre-existing posture is that all such registers are
reserved, none is released, and new outbound QoS 1/2 allocation is withheld for the session.

F-034 adds one thing: the drain no longer **finalizes over** that state. It previously
removed its claim and acknowledged the source while both registers survived behind a
consumer about to be deleted, which stranded them and reserved their identifiers for the
life of the session. Now the delivery is withheld instead.

**Convergence is at the next resume, not mid-connection.** The ambiguity is an observation
rather than a latch: it is not persisted, and every complete recovery scan recomputes it
over all claimed entries — clearing first, so a pair that has since vanished entirely does
not leave it standing. Nor does it clear on group SIZE: a scan finding exactly one
surviving register clears the ambiguity only when that register is on the identifier this
delivery addresses. A lone survivor on a DIFFERENT identifier leaves the delivery withheld,
because it does not say which identifier the client holds — which is the same fact the
duplicate pair recorded. A session that never resumes stays degraded, which is the same bound
`piSpaceIncomplete` already carries for the same underlying state: that flag too is cleared
only by "the next resume that completes its scans and finds nothing unaccountable".

So the cost is one drained exchange that does not complete until the session resumes, on a
session the broker has already declared unable to allocate. The alternative — completing it
— is a false completion, and it is what F-034 removed.

### An in-flight register that names another identifier is refused, never rewritten

`$MQTT_inflight` addresses a register by the packet identifier its subject names, and
recovery treats a record whose BODY names a different one as unreadable: the identifier is
reserved and the record is kept. Since F-034, every path that reads a register and then
destroys it — by delete OR by overwrite — is at least as strict. That closes a route by
which a record the reader deliberately preserves could be replaced with a valid tombstone
for the subject's own identifier, which a later resume would read as free.

No broker in this tree writes a register of that shape, which is exactly why meeting one
is treated as uninterpretable state rather than repaired. What an operator would see:

- **A QoS 2 retirement cannot be recorded**, so the exchange's PUBREL is kept rather than
  acknowledged away. The exchange stays recoverable and a restart re-reads it as past
  PUBREC — the outcome a lost retirement is already designed for.
- **A cancellation tombstone cannot be written**, so the fact that the delivery was never
  transmitted is recorded on the session record's nonce verdict instead. That rung is
  sufficient: a later resume still knows not to treat the identifier as an owed delivery.
  If neither rung lands, the identifier stays reserved for the life of the session, which
  is the pre-existing cost of an unproven cancellation. The same holds for a record that is
  present but UNREADABLE, and for the same reason — a read that failed licenses no
  conclusion that the register is gone.
- **A resume reserves the identifier rather than claiming or closing it.** The reservation
  is per identifier, not a session-wide withholding: allocation continues around it. What
  is also withheld is the SOURCE — an unclaimed member poisons its whole source group, so
  no member is installed and the source gets no pending mapping, because resolving a
  duplicate by installing its sibling is exactly the guess the grouping refuses to make.
  A later resume that reads the register cleanly settles it.

Each of these is a liveness cost on one identifier or one session, accepted for the same
reason as the teardown refusal above: releasing an identifier on the strength of a record
that could not be read is a silent loss, and a refusal an operator can see is not.

### Ephemeral-consumer cleanup is guaranteed per server RUN, and time-based across runs

The broker's two per-account ephemeral consumers — the retained observer on
`$MQTT_rmsgs` and the session-record watch on `$MQTT_sess` — are named from the server
run's own identity rather than freshly per initialization attempt, so a retried
initialization asks for the same consumer instead of stranding one under a name nothing
can compute again. See "Per-run ephemeral consumer identity" in `docs/ARCHITECTURE.md`
for the mechanism. What that does and does not guarantee, stated in full because the
first statement is not usable without the rest:

1. **The guarantee is conditional on a same-server-run retry** — the startup race (the
   listener opens before `Start` initializes the manager, so a CONNECT can drive
   initialization and a failed attempt is retried until one succeeds or `Start`'s own
   failure tears the server down), a direct caller, or any future retry path. There,
   cardinality stays at **at most one** consumer per (server run, account, purpose)
   whatever the attempt count, delivered by two different protocols: the observer proves
   the name ABSENT before creating it, and the watch ADOPTS its own config-identical
   consumer. It is not a claim that every previous stray is deleted — the watch's is
   deliberately adopted, not removed.
2. **A cleanly failed start gets prompt best-effort rollback**, and only for the
   consumers that attempt CONFIRMABLY created. That rollback is best-effort, not part of
   the guarantee: its own delete can be lost and nothing retries it. Nor is a failed
   `Start` always the last attempt of its run: the listener opens first, so a CONNECT can
   be queued on the manager mutex while the eager initialization fails.
   `getOrCreateMQTTAccountSessionManager` refuses to CREATE a manager once the server is
   marked not-running, which closes that path for every CONNECT that reaches it after
   `BeginShutdown`; one that passed the check in the instant before the flag flipped can
   still complete a SUCCESSFUL initialization, and the success path has no rollback, so
   its two consumers wait out `InactiveThreshold`. That window is stated rather than
   closed with a second mechanism, because its cost is exactly the five minutes
   statement 3 already governs.
3. **`InactiveThreshold` (five minutes) still governs everything else** — a consumer
   stranded by a crash, by a replacement, by a lost rollback delete, by an ambiguous
   create that no same-run retry reconciles, or by the **normal shutdown of a
   successfully started broker** (no rollback runs on the success path). No live broker
   can prove another identity's process is dead, and asking one to judge would
   reimplement `InactiveThreshold` inside the broker.
4. **Across rapid process restarts, cardinality is bounded by restart rate × inactivity
   window**, not by one consumer per purpose globally: each run draws a new identity,
   which is what keeps two live brokers from sharing a consumer.
5. **A name collision is a stated residual, not a mechanised one.** Identity rests on
   the run's own nuid draw. Under a collision the two runs also collide their delivery
   subjects; the watch's create is refused (its owner nonce differs), while the
   observer's unconditional pre-create delete would remove the other broker's live
   observer. The consequences are not one endpoint: the replacement observer may
   temporarily feed both brokers, a later rollback may remove it, and either broker's
   next initialization re-derives the same name. Guarding this would need a
   read-before-delete that still could not condition the delete against a collision
   arriving after the read.

### A duplicate plain filter that downgrades then upgrades keeps its consumer

One SUBSCRIBE packet may carry the same plain topic filter twice, first at QoS 0 and then
at QoS > 0. MQTT 5.0 3.8.4 makes that a sequence of subscriptions answered by one SUBACK
[MQTT-3.8.4-4], with the later entry replacing the earlier [MQTT-3.8.4-3], and this broker
already declares duplicate-filter SUBSCRIBE a supported packet (the retained-frame transfer
above). What it does now is end that packet coherently.

**Closed.** The deferred QoS 0 teardown is REVOKED by the later occurrence: the consumer
survives, the record names it, the delivery subscription reopens under the last
occurrence's terms, and the verdicts the staging recorded on transmitted registers are
withdrawn durably in the packet's own write. (Where the downgrade is eligible to become a
drain instead, its staging records no such verdict in the first place, so a revocation has
none to withdraw — see "A QoS 0 re-SUBSCRIBE drains rather than destroys".) Previously
the client was granted QoS 1 and
then received nothing for the rest of the connection, while the record named a durable the
packet went on to delete. Both halves of a multi-level wildcard are covered. Where delivery
readiness cannot be established the filter is REFUSED `0x80` and the downgrade stands,
because granting a QoS the broker cannot serve is the defect, not a lesser form of it.

**Not claimed: atomicity across duplicate occurrences of one filter in a packet.** The
earlier occurrence's raw subscription is still displaced by the later one's `processSub`,
and a per-filter failure of the later occurrence still leaves the earlier occurrence's
membership standing — the revocation re-stages the downgrade so the *consumer* stays
coherent with the record in that case, but the packet is not transactional across
occurrences.

**A revived consumer's deliveries skipped while the barrier was down used to redeliver
with `DUP=1` on their first transmission — CLOSED by F-072 (2026-09-09).** It was F-072's
defect reached by a new route rather than a new one: `deferredNaksLocked` deleted the
`sess.deferred` key before NAKing it and `trackPublish` cleared `dup` only for a key still
present, so any acknowledgement in the interval consumed the fact that would clear the flag.
The attribution rested on a PROPERTY rather than a case analysis, and F-072 delivered it:
the flag now follows the durable per-identifier send-intent fact at its single derivation
site, and the six `sess.deferred` DUP claims were deleted with it, so every case is decided
there. The route therefore needs no entry in F-072's close set, and no second,
weaker marker is added here — a marker recorded in this window cannot survive to be
consumed, for the reason above.

**A revived consumer's skipped deliveries are NOT NAKed, so each waits out `AckWait`
before it is re-offered — and can be overtaken while it waits.** Stated separately from the
DUP entry because **F-072 does not close it**: F-072 restores the flag, not the prompt
re-offer. The mechanism is this item's own — the closed arm records nothing, and the
revocation calls no `deferredNaksLocked`, so there is nothing to NAK.

- **Overtaking is real.** Skip A while the barrier is down, revive, publish B: with
  capacity available B reaches the client before A. That is the ordering gap already
  recorded above ("Ordering across a redelivery is not preserved — the same residual any
  AckWait or NAK redelivery carries"), reached by a new route.
- **The delay is not confined to the skipped set.** At `MaxAckPending` the unacknowledged
  skipped messages hold up later messages that never reached the callback at all.
- **Saturation is neither loss nor deadlock**, because the pinned JetStream processes
  eligible redeliveries *before* its pending-cap check. That is an upstream property, cited
  under "JetStream behaviors this design depends on" rather than re-proven here.
- **Delivery is preserved subject to ordinary expiry**, not unconditionally: stream and
  message TTL apply to a message waiting out `AckWait` exactly as to any other.

**A pre-existing staging residual, unwidened.** For a packet like
`[{foo,0}, {$share/g/bar,1}, {foo,1}]` the shared filter's early membership persist
serializes a record MID-LOOP, and that record carries occurrence 1's void verdict on a
TRANSMITTED register beside a still-pre-packet plain membership. A crash in that window
leaves a record saying a delivery the client is holding was never transmitted, which
licenses re-minting its identifier. This change does not close it — closing it would mean
either withholding the early save's `CancelledRegs`, which the revision-stamping design
deliberately does not do, or making the staging conditional on no later occurrence
existing, which cannot be done without failing the earlier occurrences too. What the
revocation does change is the SUCCESS path, where the packet's own save now withdraws the
verdict rather than leaving it to the next resume. The window is asserted in both
directions, so a future change cannot widen it unnoticed.
