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
Before leaving the experiment, operators must use the complete ten-stream bundle reset in
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
  (`processSubs`) now holds an entry in a per-account barrier and the single QoS
  1/2 store site (`mqttInitiateMsgDeliveryJSA`) waits out every SUBSCRIBE in flight
  when it samples the barrier, before storing (`mqttSubsBarrier`,
  `internal/mqtt/mqtt.go`), matching the effectively-synchronous subscription
  registration of in-memory brokers. A SUBSCRIBE that enters after the sample is
  not waited for: the publish preceded it, so it may miss this message, and it cannot
  extend the wait (until 2026-10-12 one shared count let continuously overlapping
  SUBSCRIBEs hold a store indefinitely). A publish is only delayed while a SUBSCRIBE
  it sampled is still being processed; otherwise the barrier is a single uncontended
  mutex check. No elapsed-time bound is claimed: a wait lasts as long as the slowest
  sampled SUBSCRIBE's own JetStream work.
  Regression tests: `TestV5SubsBarrierHoldsAPublishUntilInterestExists`,
  `TestV5PublishWaitsOnlyForSubscribesItSampled` and
  `TestMQTTSubsBarrierWaitIgnoresLaterEntrants` (`internal/mqtt/v5_test.go`,
  `internal/mqtt/subs_barrier_test.go`).
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

### Enumeration completeness (F-108) — UPSTREAM DEFECT, filed as nats-server#8632

**This is a defect in the data layer, not in this broker, and the broker does not
compensate for it.** By the boundary ruling of 2026-09-21
(`docs/known-upstream-issues.md`) JetStream is the data layer and a store that misreports
its own contents is never this
broker's issue. The limitation below is real and current; it is recorded so the behaviour
is not mistaken for a broker guarantee, and it closes when upstream corrects it and the
pin moves. It is filed as
[nats-io/nats-server#8632](https://github.com/nats-io/nats-server/issues/8632) with a
real-server reproduction. F-108 stays unchecked until then.

**What is true of this broker today.** No enumeration in this broker has authoritative
evidence that it reached the end of what it was enumerating, and several turn a failed store
read into a positive claim of absence:

- The pinned server maps every error from `LoadMsg`, `LoadNextMsg` and `LoadLastMsg` to
  `NewJSNoMessageFoundError()`, so a disk read failure, a corrupt block or a closing store
  is indistinguishable from an exhausted suffix. - A record whose deferred checksum check
  fails is **silently skipped** by `msgBlock.firstMatching` — and by `firstMatchingMulti`,
  none of whose three lookups propagates any error — which returns a LATER record
  successfully, with no error and no no-message response. Reproduced against the pinned
  server on 2026-09-21. A walk can therefore complete, by any client-visible measure,
  having never visited a record a healthy replica still retains. - A `STREAM.INFO` horizon
  samples `State` and `Cluster.Leader` independently, so an election between the two
  samples returns a lagging follower's state under the new leader's name. Refusing a
  leaderless answer does not close that, and comparing two unchanged tails does not
  either.

**What that exposes.** Destructive reconciliation can act on a reference set that is short;
a shared-cleanup classifier can reach a deletion verdict from a truncated walk; a
paged collector can skip coverage; a Will can be published despite a qualifying acceptance
the scan never visited; and a source message that is present but unreadable can be reported
authoritatively gone. `docs/known-upstream-issues.md` carries the F-108 evidence, the
affected sites found so far, and — because a prose list of sites has no natural end — a
**frozen census** of every load path and every response classifier in `internal/mqtt`,
bounded and counted at `1c143fc`. That census is a **preserved record of which broker
decisions rest on the server's answer**, not a repair audit: by the boundary ruling no
broker-side compensation is planned, and the record closes when the upstream correction
lands. The named sites are its output to date, **not a closed set**, and
`mqttClassifyMsgGetResponse` is named there as one function rather than as a list of its
callers. The supported suffix enumerations it reaches since the F-105 cutover are listed
by file and line in "The storage this design keeps on `$MQTT_inflight`" (F-105 section).

**Why nothing was built.** An implementation run planned an acknowledged-control-marker
protocol in full; its review refuted the protocol's central proof with the silent-skip
witness above, and the escalation ruled that the run record the blocker rather than
build a
mechanism whose evidence is not authoritative. The requirement is a read contract in which
an unreadable candidate cannot silently disappear from a successful enumeration while
genuine exhaustion stays distinguishable. **No client-side protocol satisfying that has been
established under the present constraints, and universal client-side impossibility is NOT
claimed**: two narrower error-reporting paths exist and are recorded in the roadmap bullet,
neither yet an approved primitive.

**F-109** covers the same root cause for per-subject point reads, and **F-107** — in
chapter P since 2026-09-20, because it is affine-only — covers the
separate admission-closure premise. Neither absorbs this one.

### Per-subject absence (F-109) — NOT A BROKER DEFECT, tracked in `docs/known-upstream-issues.md`

**The behaviour gap stands, and it is the data layer's.** A failed per-subject store read
still arrives as a 404 and is still classified ABSENT, so release, ownership, teardown and
retained-index decisions rest on the server's answer. By the boundary ruling of 2026-09-21
the broker is entitled to take "no message" at its word: the conflation of a failed read
with absence is the server's defect (the same root as F-108 and F-110), and its correction
is what closes this. No broker-side repair is planned; the census below stays as the record
of which decisions rest on that answer. The F-105 operations that read it (the CONNECT
ownership reads and the record loads) are named in "The storage this design keeps on
`$MQTT_inflight`" (F-105 section), beside the F-110 create-only writes.

What has changed is how much of the population is accounted, and that is now MEASURED
rather than asserted. `docs/findings/point-read-absence-census.md` computes the
conservative closure from the package's own source and reports what has NOT been
reviewed: default-only (edge, origin, arrival) triples, the functions carrying them, and
unresolved callee, dispatch, reverse-call and state obligations. **The census and the
repair-dependency map remain incomplete**, reviewed repair premises exist only for the
declared nodes and the interpreting worklist members, and the remaining reviewed
accounting of the default-only remainder is the census's outstanding bulk.

Two coverage facts belong here rather than only in the dossier. The conversion of the
checkpoint's hand-authored records carries **two recorded WITHDRAWALS** — the package's
only multi-read nodes, whose records do not say which read they were reviewed against —
so those sites are accounted as unreviewed rather than credited. And repair candidates
remain constrained by the neighbouring findings: an enumeration inherits F-108 and a
create-only conditional creation inherits F-110, both upstream defects in
`docs/known-upstream-issues.md`; and the recurring shape is that the absent arm is
precisely the one with no sequence to name, so the site that most needs an exact-sequence
re-attempt is the site that cannot make one.

The shared-group ACQUIRE settlement inherits this. A join's settlement loop
(`abortSharedAcquisition`) reaches its final unresolved exit ONLY when the ledger reads
stay indeterminate, or the fence writes stay ambiguous, through its whole attempt budget —
against a healthy store it always resolves to a definite outcome within the budget (the
acquisition is seen listed and a release verdict is written, or a later write is seen to
have excluded it). Under that persistent store flakiness, an acquire whose J1 landed but
whose landing it could never confirm fails the SUBACK closed and leaves a listed
acquisition with no release verdict. A still-connected session does not reconcile it; the
next teardown seals the ledger and releases everything listed, so it does not survive the
connection. Until then that entry counts as a holder and defers the incarnation's
last-holder retirement. This is a fail-closed consequence of the upstream store defect
class, not a broker-logic gap, and it closes with the same upstream correction.

### Outbound obligations across retirement (F-105) - integrated and accepted

**Status: integrated, and accepted with F-106 on 2026-10-08 by the integrated gate.** What
follows is current behaviour of the converted broker. The record classes, transitions and
rules
described under "The outbound obligation authority" in
`docs/ARCHITECTURE.md` are implemented and tested. The ORDINARY in-flight lifecycle now
admits through them — an ordinary QoS 1/2 delivery is admitted as an authority-carrying
obligation (`admitInflightPublish`), its SUBSCRIBE claims the source consumer's name
through the authority (`opProvisionResource`), and all three client-acknowledgement arrows
that end a PUBLISH advance the exchange in the same batch that re-phases its slot
(`opAdvanceExchangeAck`): a PUBACK to Terminal, a refused PUBREC to Rejected, and a
successful PUBREC to PUBREC-observed — and PUBCOMP ends the exchange at Terminal (or at
terminal-pending-drain while a PUBREL action is live, which that action's drain then
completes). Output is acquired and executed through the authority too: an ordinary PUBLISH
or PUBREL acquisition installs its token and its action record in one batch under the
connection's own accepted entitlement, and its frame is written through the authorized
boundary, only for its live holder and consumed once. A resume restores ordinary
obligations from the generation's duty inventory under the resuming connection's accepted
entitlement, and every owner-dependent mutation — a withdrawal, a cancellation tombstone,
the disconnect void — is fenced by the deciding connection's entitlement inside its
authority batch (7b). A source consumer's END is arbitrated too since 7c-lc: an
obligation's ending releases its holds on its source and canonical-PUBREL consumers in the
ending's own batch, and every route that ends a subscription records a disposal decision
under its own entitlement and hands the consumer to the session's retirement owner, which
retires the name and deletes it only once nothing holds it. Since 7d a DISCARDED
generation retires through its recovery owner: the teardown transfers its source, drain and
PUBREL consumers instead of deleting them, and the recovery executor ends every obligation
of the generation under the committed discarding closure (`Discarded`, which releases holds
but no identifier), records the closure-licensed disposal, retires the names and deletes the
consumers. Since 7e a shared acquisition is released only when no exchange still owes
delivery through it (the release fence), and a discarded generation's shared acquisitions
are released by its recovery owner, which the clean-start handoff previously never did.
Since 7f a SERVING generation's next accepted tenure settles what its displaced tenures left
undecided or unretired, and fork repair never reinstates a consumer another broker decided.
Since 8a a CONNECT settles any ownership proposal a crashed or concurrent acceptance left
standing before it destroys anything, every discard settles one at its own boundary, an
expired or refused acceptance abandons its own aborted proposal, the ownership deadline caps
every request of the protocol, and a refused acceptance is certified closed
(`docs/ARCHITECTURE.md`, "A standing proposal is settled before anything is destroyed").
Since 8c a connection that attempted an acquisition persists its closure certificate, and
a persisted certificate ends the live output token its connection's holder left standing
(`docs/ARCHITECTURE.md`, "A closure certificate outlives its process"). A shared-group
QoS 1 delivery is admitted through the authority too
since 7a, beside an exchange naming the exact acquisition it depends on; the experimental
affine route alone still uses the pre-F-105 writer (chapter P).

So an obligation admitted in a live generation keeps its durable representation and
completion route across a teardown. Every teardown route either cleans up under a licence
— a committed discarding closure (receipted, adopted, or the clean-start handoff's), an
acquisition ledger already sealed under such a licence, or admission proven fenced (a
closure of the generation, or an authority proving it never served) together with an
absent or empty ledger — or withholds, leaving the session record marked for an adopter
(F-105 7d, 7e, 8a, 8b; `docs/ARCHITECTURE.md`, "One tenure and one closing driver"). A
resume that preserves the generation restores its obligations from the inventory (7b, 7f,
8c). The client-visible outcome through the assembled production route is witnessed with
real clients and a peer winning retirement (WP-F W2,
`internal/mqtt/outbound_peer_witness_e2e_test.go`): a PRESERVED obligation completes on the
peer (`TestF105PreservedObligationCompletesOnThePeer`, and
`TestF105AnUnconfirmedSourceAckStillNeverResendsThePublishOnThePeer`, whose PUBLISH is
never re-sent there); an obligation of a DISCARDED generation is ended only under its
discarding closure (`TestF105DiscardedObligationEndsUnderTheDiscardingClosure`,
`TestF105ExpiredObligationEndsUnderThePeersDiscardingClosure`); and an overdue session is
not resumed on the peer (`TestF105AnOverdueSessionIsNotResumedOnThePeer`). The integration
was accepted by F-105's integrated gate on 2026-10-08.

**A durable closure certificate is one attempt, and its absence is never a licence (F-105
8c).** A connection that attempted an acquisition persists its certificate once, off the
certifier's path; a failed or lost write is logged and not retried, and a certificate that
never stood, vanished, or cannot be read leaves every token of that connection standing —
exactly the abandoned-action residual below, never a clearance on no evidence.
Certificates are kept for the recovery epoch like closures and outcomes, one per
connection that attempted an acquisition: storage grows with the generation's history of
acquiring connections, not with its in-flight population. EVERY resume lists its
generation's certificates (one request); a non-empty complete listing adds a register
enumeration and, for each register carrying a token, an action read, a certificate read
and a bounded clearance attempt. A listing the server truncates is not complete, and the
resume then clears nothing (the tokens stand until a later resume or acquisition meets
them). A PUBREL slot whose token stands is met again only by the next resume: a stored
PUBREL's admission refuses a standing token before its acquisition runs, so a mid-session
PUBREL redelivery does not clear one.

**The discard boundary's settlement can fail; a clean start then refuses, and any other
discard's release waits for an adopter.** Every discard settles a standing ownership proposal
before it claims (F-105 8a), bounded by one request timeout, and a store that cannot answer
within that bound leaves the proposal standing (F-105 8b):

- *A clean-start CONNECT* refuses before anything is detached (the client retries):
  taking the discard arm there would mark the record from the object's memory, and the marker's
  continuation could retire targets the displaced record does not name. Witnessed by
  `TestF105_8bUnresolvedBoundaryRefusesTheCleanStart`.
- *Every other discard* (disconnect, expiry, gone-retirement, the pending-discard finisher)
  claims the record with no retirement receipt, and the closing driver finds no licence for
  the shared closing (the generation can still admit), so it withholds: the record stays
  MARKED, no discarding closure is written and nothing is enrolled. The next adopter of the
  marker — a CONNECT for the client, the session's expiry, the orphan sweep — settles the
  proposal first and, finding the authority serving the marked generation, commits the
  missing receipt with a CAS of the marker and finishes the discarding close. So the residue
  is a DELAYED release, converging once the store answers and an adopter runs; the
  session-record CAS still arbitrates the claim against the proposal's own metadata commit.
  Witnessed by `TestF105_8bReceiptlessTeardownWithholds` and
  `TestF105_8bExpiryAdoptionContinuesReceiptless`.

**A preserving closure with an acquisition that joined before the seal withholds.** The
closing driver seals behind a non-discarding closure only when the generation's ledger lists
nothing (consult 122). A closing that observed an empty ledger, then failed after a late join
landed but before its seal, leaves an open ledger listing that join; the next adopter
withholds on it rather than release an acquisition it cannot prove nothing depends on. Safe,
but not guaranteed to converge for that schedule; production writes a preserving closure only
over a reservation that never served, so the schedule needs a stale join under a generation
that never installed. Witnessed by `TestF105_8bLateJoinBeforeTheSealWithholds`.

**Every CONNECT pays for its tenure.** One acceptance-fact read on `$MQTT_sess` and one
installation on `$MQTT_shared` sit on the critical path of every CONNECT, shared
subscriptions or not, and every generation of every client leaves one permanent acquisition
ledger subject within the recovery epoch (F-105 8b). Every CONNECT also reads the client's
outbound authority on `$MQTT_inflight` to settle any standing ownership proposal, and an
accepted one runs at least two authority transitions there (a reservation then a creation
finalize, or a handoff preparation then a handoff finalize), each a compare-and-swap of the
per-client authority plus a create-only, permanent transition outcome in one atomic batch.
What that leaves in storage is "The storage this design keeps on `$MQTT_inflight`" below.

**The ownership deadline caps the protocol's requests, not the whole CONNECT.** The budget
is anchored at F-051's stop and its deadline caps every request of the ownership protocol.
The restore, the uncertain-record resolution and the incarnation retirement before it spend
the budget's wall clock but keep their own bounds, so a CONNECT can overrun the budget by
what those phases take.

**The PUBCOMP release blocker is closed; three limits of what closed it are stated.** A
PUBCOMP now records the completion on the exchange and the slot in one batch, so a
completed ordinary QoS 2 exchange reads a durable ending and never "release owed"; the
canonical PUBREL is not acknowledged until that completion is recorded; and a drained
PUBREC whose intent landed before its exchange advance is recorded on the exchange by
every route that interprets the intent (`docs/ARCHITECTURE.md`, "The recovered PUBREC").
The release conjunction (row 5) is delivered: the custody ledger releases a charge or an
identifier only on an exact read of an ended phase or on a generation discard ("Capacity
and identifier custody belong to the exchange" below).
What remains is accepted as three stated residuals (consult 125, 2026-10-05): each is a
narrow case whose fix is costly.

- **A PUBREL holder that dies without draining, with no usable persisted certificate for
  its connection,** leaves its exchange at terminal-pending-drain ("drain owed"), holding
  its identifier and — unless the exchange carries a historical abandonment fact, which
  keeps the charge released — its Receive Maximum charge; at Receive Maximum 1 a
  non-abandoned one stops further QoS 1/2 delivery meanwhile. Since F-105 8c each resume
  attempts certificate-backed clearance of such a token ("A durable closure certificate is
  one attempt" below); a certificate never written, lost or unreadable, an incomplete
  listing, or a clearance that withholds or exhausts its attempts leaves the hold standing
  until a later clearance succeeds or the generation is discarded.
- **An ordinary `pubrel_ready` slot whose canonical PUBREL vanished for a reason other
  than PUBCOMP** is re-materialised since 7b: the resume enrolls it, and a continuation
  publishes a new canonical record and re-binds the slot to it under the recovering
  connection's entitlement (`docs/ARCHITECTURE.md`, "Resume restores from the inventory").
  Since 7f the continuation RECREATES the PUBREL consumer when it is verified absent under
  its standing LIVE ownership with a LIVE, undecided resource and a stream whose
  inactivity limit cannot reap it. Accepted as a residual, for these not-ready causes only
  (configuration boundaries and indeterminate reads, never the absent-consumer case
  repaired above) — an armed inactivity threshold, a stream limit, a spent or foreign name,
  a decided resource, an indeterminate read — or while the resource does not carry this
  exchange's canonical-pubrel edge, the continuation is WITHHELD and retries, so the
  identifier stays held and, unless the exchange carries a historical abandonment fact
  (which keeps the charge released), so does its charge, so a Receive Maximum 1 client
  receives nothing further meanwhile. The repair is
  scoped to an ENROLLED continuation: a successful rebind ends it, including one whose
  consumer is deleted after the new record was read back and before the rebind, so a later
  deletion of this broker-owned consumer is outside the supported configuration (below)
  and is repaired only at the next resume. The continuation is process-local; a crash
  leaves the durable slot, which the next resume re-enrolls. Recovery still never
  completes such a slot: an absent record is no proof of a PUBCOMP.
- **The completion worker is process-local.** It carries withheld acknowledgements and
  unsettled cleanups; a crash loses it, and the durable state carries on without it — an
  unconfirmed completion's PUBREL redelivers after the resume (or, if its batch landed,
  recovery finalizes the terminal slot), and a recorded cleanup's terminal slot is
  finalized by the next recovery. A worker whose cleanup is blocked by a later
  transmission's binding keeps re-checking at the capped delay until that transmission's
  own PUBCOMP (or a resume) finishes it. Progress after a crash therefore depends on the
  client reconnecting; losing durable evidence, or dropping the retries of a live worker
  that is still authorized, is outside this residual.

**Capacity and identifier custody belong to the exchange (F-105 row 5).** An ordinary
obligation's Receive Maximum charge and its packet identifier are held by the custody
ledger from its admission's reservation until the EXCHANGE durably ends, and a
scheduling-map removal — a metadata disposition, an abandonment, a tombstone, a
destruction without evidence — releases neither (`docs/ARCHITECTURE.md`, "Capacity and
identifier custody belong to the exchange"). An unsent obligation is ended durably
(`WithdrawnUnsent`) wherever its whole-exchange evidence stands. What remains, stated as
limits:

- **An ordinary exchange with no ending route holds its charge and identifier until the
  generation is discarded.** Since F-088 the gated door no longer destroys a register
  without the marker evidence: a cancellation or nonce verdict is not that evidence, and the
  door's no-marker and missing-exchange rows refuse, so the register stays the durable
  carrier of what a late acknowledgement completes. An unacknowledged exchange is held to
  discard now only where nothing can take its acknowledgement: no acting entry, no
  AckRoute or HandBack binding (F-097, "A late acknowledgement is bound before it arrives"
  in `docs/ARCHITECTURE.md`), and no resume that restores an acting entry for it (its
  register already gone, or one recovery keeps reserved). There the ledger keeps its charge
  and identifier at `Publish` (nothing is manufactured), and at Receive Maximum 1 that
  session delivers nothing further at QoS 1/2 until a Clean Start or expiry. This is
  F-106's accepted cost: a lost message reported honestly rather than a second PUBLISH
  sent while the client may hold the first. **R-B stays a narrow residual:**
  a crash between receiving an acknowledgement and its durable advance leaves no evidence
  to complete from, and the next resume's recovery treats the exchange like any other
  possibly-attempted one; no completion is inferred.
- **Experimental session-affine deliveries are still counted from the maps** (chapter P);
  their metadata removals still release their charge. A shared-group QoS 1 delivery is
  charged by the ledger since 7a, and a retained QoS 1/2 one from its admission (7c), like
  an
  ordinary one. An affine delivery whose cancellation meets an absent unsent marker is
  kept reserved like any possibly-attempted register (the release authority's marker rule
  is uniform across classes), but it gets no AckRoute binding, since slot-only entries get
  none: its late acknowledgement is served only where the affine route's own
  acknowledgement path reaches it, and otherwise the reservation is held until the
  generation is discarded (chapter P).
- **A PUBACK frees the slot one authority round-trip later than before**: after the
  exchange's advance lands and is observed, not when the scheduling entry is removed. The
  observation is the custody dispatcher's (below): the advance hints it, and an
  indeterminate read is retried on the dispatcher's own cadence rather than waiting for
  another offer or a resume.
- **A resume reads the generation's inventory before CONNACK.** A census that fails, or
  that meets a duty entry below the ledger's high-water mark or an exchange it cannot
  read, withholds new QoS 1/2 delivery (an incomplete census) and identifier allocation
  (either) until a re-observation settles it. It does not lose the deliveries already
  admitted: a validated register whose lineage the inventory could not decide keeps its
  identifier and source mapping under PROVISIONAL custody, and the uncertainty's
  resolution decides its projection (resolved live, it stays and its redelivery reuses the
  identifier; resolved ended, it is removed and the identifier freed). Its PUBLISH is
  still sent only through the durable acquisition, which compares the register it names.
  An unreadable entry or exchange is an unsupported shape and is refused, not interpreted:
  if it never becomes readable, the session withholds allocation until its generation is
  discarded. Two more shapes are refused the same way (F-088): an ordinary register with no
  exchange that is not a positively identified pending cancellation fence (no
  `cancel_origin: pending-create`, or the sparse form not met) is refused rather than
  interpreted: with a complete census, recovery reserves its identifier like any register
  whose exchange the inventory did not reach, and no cancellation bit exempts it; and a
  register whose
  `cancel_origin` is missing while
  it is `Cancelled`, unknown, or present while it is not `Cancelled` (and a
  `pending-create` body outside the sparse fence form) fails the shape check at decode.
  Development stores holding cancellations written before `cancel_origin` existed read
  those registers as unreadable and reserve them; recreating the streams is the remedy, and
  nothing migrates them.
- **A live generation's history is retired behind a floor; what it does not reclaim
  (F-112, RESIDUAL).** Since F-112 a live generation resolves its exchange duties and
  retires them behind a proven contiguous floor (`docs/ARCHITECTURE.md`, "The inventory
  floor"): a resume, a custody census, an admission's identifier-exclusivity check and a
  shared release fence read `[Lo, Hi]`, not the generation's history, and the exchange
  records and duty entries of reclaimed positions are physically deleted. What remains:
  - **Total storage is still unbounded.** Every transition's outcome, every acquisition's
    action record, every resource record, closure and certificate stays on
    `$MQTT_inflight` for the recovery epoch; F-112 reclaims exchanges and duties only.
  - **An obligation that never ends in a live generation is carried, not reclaimed**: a
    delivery abandoned on an ACL deny that the client never acknowledges, a shared
    delivery stuck at Publish after another member acknowledged its source, and a
    transmitted delivery cancelled by a downgrade or a refused drain (F-106's accepted
    cost) — each stated elsewhere in this file. So is a completed QoS 1 obligation whose
    deleted register a store recovery REVIVED (upstream MAXMSGSPER-REVIVAL, chapter U,
    measured by F-112's own fixture after a server restart): the revived register names
    the lineage, so the floor carries its position rather than reclaim it, and the broker
    does not compensate. Their exchanges and duties stay until the generation is
    discarded, bounded by the identifier space, and each time the floor passes them they
    are rewritten to a representative above it — at most half as many writes as positions
    reclaimed, amortized, because a horizon executes only once it reclaims at least
    `max(32, 2 x relocations)`.
  - **The floor trails the history.** It advances only past positions that are provably
    finished, from a horizon each broker measures on its own, a few dozen positions per
    dispatcher unit, when the span exceeds 256 positions, on an ending, a resident ledger
    read or a 30-second periodic re-arm, and every write it makes yields to announced
    foreground operations. A position that cannot be read stops it there until it reads.
    So `[Lo, Hi]` is the outstanding work plus up to a few hundred recently finished
    positions plus what is carried, and resume latency grows with that, not with the
    history.
  - **A discarded generation's exchanges and duties stay** (the 7d residual below): the
    floor moves only in an OPEN ledger; a seal freezes it, though deletions already
    authorized below it finish before the recovery root retires.
  - **The ended-lineage veto keeps one string per ended lineage** whose register still
    stood, or that the session object still projected, at the last floor run; the prune
    runs on the next ending or resident ledger read, so a session with no further traffic
    keeps them.
  - **The floor's background writes can still exhaust a delivery admission (holistic round
    H4, consult 2).** The ordinary and shared delivery admission ladder announces itself
    as a foreground authority operation since F-112, so no background attempt admitted
    after it starts can take its attempts. Attempts admitted BEFORE the announcement still
    may: the retained driver, the continuation, the reclaimer and the floor unit can each
    hold one. Worked interleaving: four background attempts are admitted; the admission's
    attempt 1 reads the authority, writer 1 reads and commits, attempt 1 loses its CAS;
    writers 2 and 3 do the same to attempts 2 and 3; the ladder (three attempts) is
    exhausted — three interfering commits suffice. Nothing is stranded or doubled: an
    attempt that may have landed installs admission continuity, and a never-landed one is
    deferred and its source re-offered at AckWait. The cost is latency, with no finite
    bound under repeated contention; F-114 (ROADMAP §2) owns the remedy.
  - **The reclamation proof reads absences as facts**: that the register subject no longer
    names the lineage, and that no duty inside the floor names an absent exchange. A store
    read failing as "no message" (upstream F-109, chapter U) would be believed here as it
    is at every other point read; the broker does not compensate.
- **The custody dispatcher reconciles without traffic, within stated limits (F-097).** The
  custody observer is an admitted worker holding the incarnation lease, and it runs as a
  dispatcher of BOUNDED units: at most eight per iteration, scheduled round-robin across
  its queues (hints, a requested pass's targets, due read retries, due owed withdrawals),
  each re-checking its captured generation, epoch and ledger. Nothing retries inline: a
  unit without progress backs off from 100 ms, doubling to `max(100 ms, min(5 s,
  AckWait/4))` with +/-20% jitter, and progress resets it; a tracked timer is armed for the
  earliest eligible item, a latched demand or a throttled census. The census over the
  generation's history is O(H) and is not a unit: it runs at most once per cap interval
  across every trigger (`lastHeavyAt`). A unit makes at most one handoff to an existing
  owner (terminal retry, intent resolution, owed materialization), and that owner's
  execution is not the dispatcher's. Its derived bound is **152 dispatcher requests per
  pass** (eight units of at most 19 requests, an ordinary withdrawal being the largest;
  an admission-settlement unit settles every unsettled attempt of its admission at up to
  five requests each, and the ladder makes at most `mqttInflightStoreAttempts` = 3
  attempts, so it stays within 15), with the census and inherited-owner execution
  EXCLUDED. An admission-continuity unit (below) takes ONE step, each within its own row:
  settle the attempt, OR classify (two reads), OR one withdrawal; a classification that
  finds a withdrawal due re-queues the record so the withdrawal is the next unit's step.
  Nothing here bounds a pass's total cost, the time to
  completion or the retries until convergence, and every
  inherited-owner request count observed in tests is a measurement, not a bound. The
  deltas this item adds to existing owners: one marker read per write-boundary evaluation
  that must refute a cancellation; at most one whole `intendPubRelSlot` per resolver
  attempt for an AckRoute's owed successful PUBREC; the admission classifier's marker probe
  for a cancelled `publish` occupant, moved before classification (an added read where it
  ends in a reap or an abandon); and the exclusion-reclaim pass's pending-fence step,
  charged to that pass's own 64-request budget at no more than six requests per step.
  Progress depends on storage answering: while JetStream cannot, the dispatcher keeps
  backing off and releases nothing. It never reads or clears `piSpaceIncomplete`.
- **An admission that may have landed withholds its source rather than doubling it
  (F-097 WP5).** When the ladder gives up on an admission the custody ledger reports
  landed or unsettled, an admission-continuity record keyed by the source delivery makes
  `trackPublish` refuse that delivery's offers, and the custody dispatcher decides the
  first admission one bounded step at a time: it settles an unsettled attempt; retires the
  record on an exchange that owes no further delivery (ended, its delivery abandoned, or
  its generation discarded); NAKs the source when nothing of the allocation landed; and,
  for a landed never-sent obligation (register at `publish`, no token, exchange at
  `Publish`), withdraws it under the admitting connection's recorded licence (a successor's
  boundary refuses it, and the successor's recovery decides) and then NAKs the source, which
  is re-offered and admitted once (witnessed by W5b, also across a cold peer resume).
  **Residual:** for every other state of that first admission (possibly attempted, past
  PUBREC, an action token, unreadable) no disposition was built, because no
  witness reached one, so the refusal STANDS: the source delivery's offers are withheld
  (never delivered twice) while the record is retried at its backoff. A resume that
  restores this allocation's own entry (same identifier, nonce and source delivery)
  supersedes the record: `trackPublish` retires it and serves the restored entry, and the
  dispatcher's next unit retires it too (stated from the code; not route-witnessed, as
  the same-object-resume subtest skips). Otherwise the record is dropped only with its
  generation (a discard or a clean start). Only the ledger-admitted classes (ordinary and
  shared) install one; a retained delivery and an experimental affine one install no
  record (chapter P keeps the affine route's own behaviour).
- **A restored delivery denied on resume is abandoned, not retransmitted, and its
  identifier remains held until valid protocol completion and cleanup or generation
  discard.** Abandoning the retransmission
  §4.4 would require is a deviation this broker already made (HEAD's deny acknowledged the
  source away); what changed is that the ordinary delivery's abandonment is now durable
  and its IDENTIFIER is held (HEAD freed it), because §4.3 ties identifier reuse to the
  acknowledgement. Its Receive Maximum charge is released on the resumed connection (Spec5
  §4.9). While the abandonment cannot be made durable — a send section open, the
  connection having itself acquired the delivery, an unresolved write — the source is left
  unacknowledged; installed authorized owners retry without requiring a re-offer.
- **A restored delivery already claimed by an UNSUBSCRIBE or QoS 0 downgrade drain is NOT
  abandoned** (consult 35). Terminal denial keeps the previous scheduling-entry and
  register disposal and the source acknowledgement, and writes neither
  `delivery_abandoned` nor `DeliveryAbandoned`. Since F-088 a POSSIBLY ATTEMPTED entry's
  removal there installs an AckRoute binding in the same critical section and the door
  preserves its register (no destruction is licensed), so the client's
  late PUBACK or PUBREC completes the exchange and its custody releases
  (`releaseTransmittedIdentifier`). Where no acknowledgement ever arrives, or the entry
  carried no possible-attempt evidence, the exchange stays at `Publish` with no ending route
  and its Receive Maximum charge and identifier remain held,
  including across same-generation recovery, until generation discard; at Receive Maximum
  1 this can prevent all subsequent QoS 1/2 delivery until Clean Start or expiry. This is
  an explicitly deferred F-106 limitation, and this population receives no durable
  abandonment suppression: the drain's own claim and finalizer are unchanged, and nothing
  here completes the drain claim.
- **A non-abandoned `*PendingDrain` exchange whose PUBREL holder died holds its
  charge** until a certificate-backed settlement or a generation discard. Since F-105 8c
  a usable persisted certificate of the holder's connection clears its token and, for a
  `pubrel_terminal` slot, completes the drain to Terminal (`op-certified-drain`,
  `outbound_close_cert.go`), which releases the charge and the identifier
  (`mqttExchangeCharge`, `outbound_custody.go`). With no usable certificate it is the
  PUBREL-holder residual above. A historical abandonment fact keeps the charge released,
  but does not release the identifier or the live output hold.

With stable serving authority and eventually successful operations, installed disposal and
cleanup owners converge without incidental traffic while their required actions remain
authorized, including cleanup of durably established abandonment.

A failed or incomplete recovery, cold or same-object, including one that displaced the
tenure of an installed but unlanded disposal, may remain fail-closed until a later
successful resume. Returning storage alone does not guarantee progress for this
population. The durable exchange obligation remains preserved; a retained old-tenure
disposal preserves its evidence, unresolved handles and restrictions but cannot submit its
denial under successor authority. The next successful synchronous recovery reconstructs
the durable obligation and routes it under its own tenure, making any required fresh
authorization decision and completing the existing transactional drain-floor handoff
before activation. Thereafter, owned disposal and cleanup converge under the stated
progress assumptions.

While recovery-dependent disposition remains unresolved, the affected source is not
acknowledged, transmitted or made actionable as a drain claim; its identifier is not
reused, and required custody and suppression evidence is not destroyed. This residual does
not excuse a lost installed obligation, dropped retry after admission failure, unsafe
acknowledgement handling, suppression loss, or failure of an otherwise authorized owner to
retry.

P-12 f6 remains an ACCEPTED RESIDUAL under consults 46 and 48, not fixed or refuted. The
original no-reconnect requirement remains unmet.

"Source acknowledged" refers to JetStream source settlement; valid identity-bound
MQTT ACKs retain their meaning. Local abandonment alone does not release the
identifier; later valid protocol completion can, subject to source and output
holds. The drain-owned limitation and retransmission deviation above remain.

An unpersisted successful PUBREC is not reconstructible from an abandoned pair
after process loss. Live admission-refused work remains owed and must be retained.
ACK-first terminal/rejected cleanup retains durable source identity and
suppression until confirmed source settlement even when abandonment never landed.
Successful recovery reconstructs this cleanup without inventing abandonment or
requiring source redelivery.

**What the output boundary does not cover.**

- **Experimental affine QoS 2 output** (PUBLISH and PUBREL) is still acquired by the slot
  machine's standalone register CAS, writes no action record and is written raw; its
  conversion is chapter P's. Shared-group QoS 1 output is acquired and executed through
  the authority since 7a, like an ordinary delivery's. This is a chapter P boundary, not an
  alpha residual: it is neither repaired nor accepted for supported traffic.
- **Abandonment evidence is process-local (accepted residual, consult 125).** An ordinary
  acquisition given up before it wrote is recorded ABANDONED on the connection that
  abandoned it, which only stops that connection from executing it; it neither ends the
  durable token nor serves as cross-process completion evidence. A persisted certificate
  (F-105 8c) or the generation's discard governs the remaining hold. This is distinct from
  the durable `DeliveryAbandoned` fact and the source-suppression obligations above. The
  whole-exchange discharge proof that once read it was deleted in F-105 8c, unconsumed.

**A shared delivery another member consumed while its member was away stays charged
(pre-existing, stated at 7a consult 85).** A persistent member that disconnects with a
transmitted, unacknowledged shared QoS 1 delivery keeps that exchange, its identifier and
its source mapping, so on its resume the group's redelivery to it is retransmitted under
the same identifier with DUP set. But the group may instead redeliver the source to
another member while this one is away (Spec5 4.8.2 lets the server do either), and once
that member acknowledges it the source is gone: this member's exchange is never resent,
stays at Publish, and holds one Receive Maximum charge and its identifier until the
generation is discarded. Independent replay of a consumed group message is not provided;
complete reconnect conformance for shared deliveries is not claimed.

**The release fence makes that stuck exchange hold its group too (7e, stated cost).** An
acquisition is released only when no exchange naming it is still at Publish, because that
exchange's only retransmission is the group consumer's re-offer of the source. The fence
cannot tell the stuck exchange above from a live one — proving the source gone would rest
on an absence read, which this design never treats as a fact — so an UNSUBSCRIBE (or any
other release) of that acquisition is WITHHELD for the rest of the generation: the
acquisition stays listed and closed, the group incarnation stays held, and the
interest-retained backlog for that group keeps accumulating even if no member serves it,
until the generation is discarded and its recovery owner's closing releases it. Each
re-attempt of a withheld release (the owner's re-trigger, every stable pass of the verdict
sweep) also advances the session's outbound authority once, so a concurrent authority
operation of that session pays one compare-and-swap re-read.

**Shared QoS 0 output can overlap a later hand-back (accepted, 7a consult 84).** Before
output, a shared QoS 0 offer checks the session's source disposition. An existing HandBack
binding or unresolved disposal prevents output and positive source acknowledgement. The
check does not reserve the source against a disposal or recovery installation that occurs
afterward. Consequently, an offer that passed its authorization and disposition checks may
emit a QoS 0 PUBLISH even though HandBack is installed before its source-ACK submission.
If HandBack remains held at submission, the ACK gate submits a paced NAK instead of a
positive ACK; an unresolved disposal also prevents positive acknowledgement. The source
remains eligible for group redelivery, so recipients may observe duplicate application
messages, including delivery to another group member. This is the duplicate-delivery class
already possible when the JetStream source is redelivered before acknowledgement. It is
not an at-least-once delivery guarantee for MQTT QoS 0, and no duplicate-count or timing
bound is claimed. The overlap does not authorize a denied offer or allow this member to
positively acknowledge a source while HandBack is held.

**An accepted residual: ordinary QoS 1/2 acknowledgement-cleanup latency.** An
ordinary acknowledgement's terminal cleanup now runs `opAdvanceExchangeAck` and THEN
deletes the register (`recoverPublishTerminalSlot`), one authority round-trip more than
the pre-F-105 slot-only path. A packet identifier re-allocated inside that widened
interval finds the prior terminal register still on its subject, classifies
`mqttSlotDispTerminalCleanupWithdraw`, and is withheld to its source consumer's AckWait
redelivery (default 30 s) rather than sent promptly. The interval is a pre-existing one —
the slot-only path has the same terminal-register-on-reused-PI window — that this change
widens by one round-trip; measured at 0 in 120 isolated runs and absent from the full
suite under load. It was recorded as closing "at 7a"; that promise is WITHDRAWN (7a,
consult 82): converting the shared class does not delete the register inside the advance
batch — the advance writes a terminal register and terminal source completion still needs
that register as its durable carrier, and nothing has shown that deleting it inside the
batch is safe. Closing it needs a carrier/settlement design of its own, and consult 125
(2026-10-05) accepted the delay as a stated residual rather than owed work: the window
needs an identifier re-allocated within one authority round-trip, and the 0-in-120 figure
is history, not a frequency guarantee.

**The recovery pass (the checkpoint that added it).** The pass is real. A closing
transition allocates a
REACHABLE
RECOVERY ROOT in the same atomic batch that writes the closure, seals the inventory and
enrols the
durables, so a closed generation is discoverable by construction rather than by a later
write that
a crash could lose. An account-manager pass walks the directory on the periodic sweep's
cadence,
within an explicit budget of WORK UNITS rather than store calls — a unit is charged where
work is dispatched, and the helpers it calls carry nested reads it does not itemise — with
capacity reserved for its own progress
writes, and
executes the affine cleanup family end to end: the readiness assertion, the provisioning
seal, the
deletion authorization, the physical delete, the classified outcome and the duty's
resolution —
the last two in ONE batch. One part of that family is deliberately OUTSIDE the
store-operation
budget: the readiness WALK is account-wide and cannot be filtered, so charging it per
record would starve it rather than bound it. It is capped at ONE attempt per pass
instead, and what protects the sweep goroutine is cancellation rather than accounting —
between the enumeration's reads and before each record's processing unit, with a unit
already entered running to its end (nested reads, up to `mqttQ2AffClaimAttempts`
load/CAS rounds, its mutations, and on the discard side one reap that may itself advance
and delete). Shutdown therefore waits for one bounded unit, not for the account. The
pass ENDS after the root that spends the attempt, with that shard's traversal position
advanced past it and persisted, so a root whose resolution fails permanently cannot hold
the attempt against its siblings. That is the same walk the owner's teardown already
performs inline, so it performs comparable work rather than new work — though scheduling
another scan does add work. The walk itself takes an AUTHORITATIVE horizon and re-reads
the stream's end afterwards, asserting completion only when the tail has not moved, in
the same stream incarnation, with at most two fixed-horizon extensions; a fixed horizon
alone would be unsound, because MaxMsgsPer:1 means advancing a claim rewrites it at a
HIGHER sequence and `confirmSourceAckAndRecord` does so with no owner scoping, so a
claim of the discarding generation can be moved above it. **A latency residual is stated
and accepted here**: sustained movement of the assignment stream — a single write inside
every verification interval is enough, the writers need not outpace the reader —
postpones readiness indefinitely, delaying the physical cleanup and retaining the
recovery state. No MQTT client observes an error for it, which is why it is admissible.
The bound applies to the recovery pass's charged walk only; the uncharged callers keep
their behaviour, and the pre-existing unbounded orphan scan on the same goroutine means
this does NOT establish that the maintenance goroutine as a whole cannot hang.

A SECOND and more serious dependency is stated here rather than accepted as a residual:
the walk's completeness proof assumes the closed generation admits no fresh undiscarded
claim, and at HEAD it can. `beginQ2AffOp` admits a shared QoS 2 move against LOCAL session
state and the claim CAS never contends with the durable closure, so a mover paused before
that CAS can install one after the final verification read. This is NOT admissible as a
residual, because it has not been shown to be harmless — a late undiscarded claim
disproves readiness, and while no redistribution witness has been established, neither has
harmless accumulation. It is open work, filed as F-107 **in chapter P**, and it makes the
recovery pass a NON-SHIPPING CHECKPOINT whose readiness claim is conditional on that fence
existing. A SECOND and independent dependency stands beside it: the walk has no
authoritative completeness evidence, because a `STREAM.INFO` samples its state and its
leader name SEPARATELY, and because a failed store read is indistinguishable from an empty
suffix — the pinned server maps every error from the store-load calls to
`JSNoMessageFoundErr`. The walk now WITHHOLDS on that response rather than reading it as
an end. The two dependencies are of different kinds. F-107 is open work in chapter P. F-108
is an upstream data-layer defect tracked as nats-server#8632
(`docs/known-upstream-issues.md`): not a residual or blocker this repository carries, and
not an alpha prerequisite under the boundary ruling, but it is NOT confined to this affine
walk; the supported path runs suffix enumerations it reaches too ("The storage this design
keeps on `$MQTT_inflight`" below names them). For this walk, together they mean what is
demonstrated is execution over the exercised fixture populations, not affine recovery.

A generation whose duties are all terminal is retired: its completion
certificate is written and confirmed first, and only then is its root deleted.

The release gate's third fact changed with it. It asks whether the generation is REACHABLE
— a
root standing at the address its own closure records, inside its shard's high-water — and
no
longer whether that root's sweep is unfinished. A finished sweep is not a retirement, and
making
the gate depend on a cursor position would have made one goroutine's liveness into its
safety
condition.

**What is transferred, and what is not.** Cleanup ownership is per TARGET, and the
selector is the
closure's own immutable discard disposition. A DISCARDING closure enrols its affine QoS 2
consumer
and that consumer's deletion becomes the recovery pass's; a closure that preserves the
session's
obligations enrols none, and its affine consumer stays the teardown's to delete exactly as
today.
The SHARED group durables are not resources of the authority at all: their release is the
shared-group lifecycle's — a group record arbitrating each logical group's physical
incarnation and holder count, a per-generation acquisition ledger the teardown SEALS before
it releases what the ledger lists, and a closing gate that withholds the record's release
until the sealed ledger is empty (`docs/ARCHITECTURE.md`, "A group durable is held by
arbitration, not counted by a scan"). Landed 2026-09-22.

**Shared release is delivered by that lifecycle rather than by this recovery pass, and
full P3 is not met.** A group durable is held by several sessions, so a name-based delete
racing a join needed a holder-set mapping plus physical-incarnation naming — one mechanism,
and a change to the production shared-subscription lifecycle in its own right, which is
what landed: the crash-left half of a shared release is recovered by the adopted teardown
(seal, release, gate) and by the orphan sweep's verdict and retirement passes, from records
they read, never from an enumeration's exhaustion. The affine recovery EXECUTOR is
demonstrated only CONDITIONALLY — on an admission closure accounting for every previously
admitted producer (F-107, which does not exist yet) and on authoritative enumeration
completeness (F-108, which awaits the upstream correction) — and full P3 remains unproved.

**Nothing in this section blocks cutover step 2 any longer.** Until the boundary ruling of
2026-09-21 this paragraph named sound evidence for the supported-path decisions F-108 and
F-109 affect as a step-2 prerequisite; that sentence is retired. The converted broker
handles a reported failure, an ambiguity, a concurrent change and a GENUINE absence by its
own contract (a genuine absence never establishes completion); a false absence or a false
end supplied by the store is the upstream defect's (`docs/known-upstream-issues.md`), and
the broker does not compensate for it. The affine executor's two conditions block only the
EXPERIMENTAL claim: session-affine shared-subscription QoS 2 is not an alpha release
feature, F-107 was relocated to `docs/ROADMAP.md`'s chapter P on 2026-09-20, and F-105's
affine completion work went with it. Full P3 stays unproved either way, and relocation
completes nothing.

#### Three populations this change left unresolved (one since closed), none client-visible

Stated together because each is an accumulation rather than a lost message.

**1. Roots that never retire — CLOSED on 2026-09-22.** This population was a cost of the
split above: every generation carrying the untransferred shared cleanup duty kept that duty
unresolved and its root was never retired. The shared-group lifecycle transfer deleted the
duty — a shared group durable is no longer enrolled into a closure at all, so no root
carries one — and with it the population. Whole-generation retirement is no longer
prohibited on this ground; the affine populations below still stand.

**2. A delayed affine create that lands after certification.** The readiness assertion
does not
prevent a JetStream create already in flight: a creator that read it as absent and then
paused can
still land afterwards. What that leaves is consumer state, possibly with retained copies,
for an
ENDED affine generation — and nothing reaps it, because F-036 course E deliberately
removed the
only route that could. It is the same accumulation that residual already accepts, reached
through
a narrower window.

Its acceptance does NOT follow from being already documented, nor from "it loses no
message".
Incorrect delivery, lost completion evidence and premature identifier or capacity release
would
each violate the rule that a residual is admissible only where no MQTT client experiences
incorrect behaviour, and those remain the obligations of the existing delivery and discard
protections and of F-105's proof custody. **A recreated consumer supplies no serving
authority and
no completion evidence.** The certificate and the seal certify THIS authorized cleanup
duty;
neither asserts that no consumer can ever exist at that name again, and no decision may
reinterpret
them that way.

**3. A creation over a CLOSED predecessor, which is PRE-EXISTING.** A generation closes
without
discard, a creation is reserved over it, the metadata write replaces the standing session
record,
and the broker dies before any teardown — leaving that generation's affine consumer with
nothing
naming it. The outcome is identical at HEAD: the trace writes a closure there too, what it
does
not do is transfer the target, and nothing reaps an affine consumer either way. This
change
neither creates the window nor widens it, and no certificate is written about that target.

#### The behaviour the mechanism introduces

Stated now because it is a deliberate, permanent consequence of the design rather than a
defect to be removed later:

**A displaced connection may receive a late packet, and it may be that exchange's FIRST
transmission.** Bytes handed to a transport are beyond any durable arbitration. An action
acquired before a retirement is explicitly permitted to execute afterwards, on its original
connection with its original packet-identifier binding, because the alternative — treating
a
retirement as though it stopped a remote process's socket — is the false claim behind
F-105's original defect. The broker does not describe this as a duplicate: it may be a first
transmission, and a conforming client applies the ordinary QoS 1/2 rules to it.

A connection is treated as unable to emit only on a positive closure certificate, which only
the process owning that connection can produce. A timeout is not closure, and neither is
failing to observe another broker. Where no certificate is obtainable the obligation stays
outstanding — a leak, accepted deliberately against the alternative of destroying a live
exchange. The leak is tied to the GENERATION: an outstanding obligation holds its identifier
(through its exchange's identifier exclusivity) and its Receive Maximum charge (counted over
the generation's exchanges) until its generation is discarded, which is F-106's accepted
cost. A same-generation resume preserves the generation, so a resumed connection DOES carry
that identifier and that charge; a clean start or an expiry discards the generation, and the
fresh one carries neither. Since F-105 8c a persisted certificate of the holder's connection
ends the leak earlier ("A closure certificate outlives its process").

**Permanent storage.** Transition outcomes and generation closures are never deleted during
the account's recovery epoch, because no proof exists that every delayed caller has stopped
referring to one; nor are the other permanent classes listed in "The storage this design
keeps on `$MQTT_inflight`" below. Storage grows with the epoch's accumulated activity.
The per-CONNECT share of it, together with the acceptance facts and receipts on
`$MQTT_sess`, is stated once as an accepted exposure under "Per-CONNECT permanent records
accumulate for the recovery epoch, and a reconnect loop can exhaust storage".

**A competing acceptance may preempt a live, uncommitted ownership claim, and sustained
competition has no starvation guarantee.** An acceptance that finds another claimant's
unresolved proposal settles it by taking that proposal's permanent metadata slot create-only.
The original claimant may be alive and about to commit; it then loses the race and is told
so. This is deliberate: the alternative is a bare refusal, which leaves a client whose
claimant died between reserving and resolving permanently unservable, because no arbitration
record is ever deleted.

The winner is decided by the slot and never by elapsed time, and the design carries no lease,
timer or backoff — introducing one would rebuild the liveness inference the whole mechanism
removes. Two bounds stop it degenerating: an acceptance settles at most ONE foreign proposal
and never chases a second, and an acceptance whose own proposal was aborted refuses the
CONNECT rather than minting a replacement. Neither bounds a client that reconnects
repeatedly against a peer doing the same, so under sustained competition a given acceptance
has no guarantee of eventually serving. What is not promised is that a particular one wins.

Termination is likewise conditional on the STORE ANSWERING. Every step here is a positive
record — a create-only slot taken, a verdict read back — and a read or a write that neither
succeeds nor definitely fails leaves the acceptance UNRESOLVED, which is the honest answer
and not a decision. Such an acceptance refuses the CONNECT rather than guessing, and the client
retries; so an individual CONNECT terminates with a definite answer only while the session
store is answering, and no timeout is substituted for one that is not.

#### The storage this design keeps on `$MQTT_inflight`

**What is permanent.** Within the account's recovery epoch nothing deletes these F-105
classes on `$MQTT_inflight`: the per-client authority (rewritten by compare-and-swap),
generation closures, transition outcomes, name ownership, closure certificates, generation
completion certificates, resources, exchanges, actions, generation ledgers, duty entries and
directory shards. The only `$MQTT_inflight` deletes are exact-sequence deletes of
registers, unsent markers, retained `rel` records, retained material, recovery roots (after
their completion certificate) and a discarded obligation's register and `rel`. This storage
is accumulated, not a ceiling: it grows with the epoch's activity, nothing reclaims it, and
no fixed bound is claimed.

- **Already by the first accepted CONNECT,** not only once an obligation is admitted. An
  accepted CONNECT runs at least two authority transitions, so it makes at least two
  authority writes and leaves at least two distinct permanent outcomes. Retained subjects
  and write volume differ: the authority is ONE retained subject per client however often
  it is rewritten, while each outcome is a new subject retained for the epoch. A CONNECT
  that committed a reservation or a handoff preparation and then failed can leave records
  too.
- **Per admitted obligation,** its exchange, an outcome per transition (the admission and
  each later advance) and its dependency records (its duty entry and resource edges),
  written in the register's own admission batch and so adjacent to it in sequence.

**MAXMSGSPER-REVIVAL** (evicted-version revival; "A limit-evicted in-flight register can
revive on a crash recovery" below). Occupancy: the stream is non-empty for an account from
its first accepted CONNECT, so whole-stream emptiness is no longer an escape for any
account, single-session accounts included. A register is revival-ELIGIBLE only once an
obligation was admitted AND its subject meets the defect's conditions: rewritten under
`MaxMsgsPer: 1` (the acknowledgement advance), its survivor explicitly deleted (terminal
cleanup), and a recovery that revives (a crash, an unclean shutdown or a stale or missing
`index.db`; the clean-shutdown witness of 2026-09-24 reproduced it too, and no index or
rebuild cause is inferred from that witness). The permanent neighbours remove whole-stream
emptiness as an escape; they do not establish that any of them shares the register's
storage block, and no universal revival is claimed.

**F-108** (false enumeration exhaustion; a failed read reads as the suffix's end). The
SUFFIX enumerations (`loadNextMsgFromSeq` over a subject filter) the supported path runs:

- `scanPubRelRecordsCharged` (`unsub_drain.go:3227`, on `$MQTT_out`), reached by a resume's
  register recovery (`recoverInflightRegistersOnce`, `inflight_register.go:9356`), the
  owed-PUBREL lookup (`findOwedPubRelRecord`, `pubrel_action.go:1596`), terminal PUBREL
  disposal (`disposeTerminalPubRelRecords`, `pubrel_action.go:2310`) and the drain PUBREL
  recovery (`recoverDrainPubRels`, `unsub_drain.go:3311`);
- the account sweeps over `$MQTT_sess`: `sweepSessionExpiries` (`mqtt.go:8773`),
  `sweepOrphanedConsumersBefore` (`mqtt.go:8942`), `buildSessionRefs`
  (`orphan_qos2_sweep.go:860`), `adoptAbandonedTeardowns` (`orphan_qos2_sweep.go:557`) and
  `sweepAcqReleaseVerdicts` (`shared_lifecycle_sweep.go:168`); the acceptance-fact scan
  for a delayed Will (`scanAcceptFacts` via `loadNextAcceptFact`,
  `sess_accept_fact.go:344`);
- `condemnRetiredSurvivors` (`orphan_qos2_sweep.go:667`, `$MQTT_teardown`),
  `collectInboundQoS2Candidates` (`orphan_qos2_sweep.go:1229`, `$MQTT_qos2in`),
  `collectInflightCandidates` (`orphan_qos2_sweep.go:1659`, `$MQTT_inflight`),
  `scanWillOutbox` (`will_outbox.go:955`, `$MQTT_willq`) and
  `sweepSharedGroupRetirements` (`shared_lifecycle_sweep.go:128`, `$MQTT_shared`).

The affine walks (`qos2_shared_affine.go:3820`, `:4008`, and the affine PUBREL proofs at
`sess_teardown.go:833`) are chapter P's. The duty traversal is NOT a suffix enumeration: it
reads the named ordinals 1..high-water at exact addresses in bounded batches
(`traverseGenerationInventory`), so its exposure is F-109's. The subject listings read through
the stream-info API (`streamSubjectsComplete`, `enumerateInflightSubjects`) are a different
enumeration API: whether F-108's false exhaustion reaches them is not established, and they
are not claimed covered by this list.

**F-109** (false point-read absence). The CONNECT ownership reads: the authority read
(`loadOutboundAuthority`, where "no message" classifies the authority ABSENT and the
acceptance then reserves create-only at revision 1), the standing-proposal settlement and
the proposal's metadata slot; and the record loads that classify "no message" as absent,
among them the transition outcome, the generation closure, the generation ledger (an absent
ledger is an empty inventory), the duty traversal's entries (a missing entry is held
UNCERTAIN, fail-closed), exchanges, resources, name ownership and closure certificates.

**F-110** (a create-only compare-and-swap admitted over an unreadable occupant). The
create-only writes: every transition outcome (and the ABORTED verdict that competes for
the same subject), the first name-ownership claim at revision 1, and each closure
certificate.

This corrects the exposure statement. It does not reopen the upstream prerequisite, and the
broker adds no compensation: each defect closes with its upstream correction and a reviewed
pin bump (`docs/known-upstream-issues.md`).

### The source consumer's lifecycle (F-105 7c-lc): what it costs and what it does not cover

- **The supported-configuration boundary for consumer inactivity.** A depended-on source
  or PUBREL consumer must be deleted by NOBODY while an obligation needs it, JetStream
  included. The broker creates these consumers with no inactivity threshold and its
  admission refuses a consumer whose effective configuration carries one, or whose stream
  imposes one; but that check is a snapshot, not a fence. An administrator who imposes an
  inactivity limit on `$MQTT_msgs` or `$MQTT_out`, or updates one of these consumers to
  carry one, AFTER an obligation was admitted against it can still have JetStream reap it
  under that obligation. Running both streams and their consumers without inactivity
  limits for their whole lifetime — provisioning, readiness, admission and dependence — is
  a deployment requirement; this rests on the existing live configuration-drift residual
  and does not eliminate it. The same boundary covers deleting one of these broker-owned
  consumers by hand: the PUBREL consumer is recreated only by an enrolled re-materialisation
  continuation or the next resume (F-105 7f), never by detecting the deletion.
- **Offline retention cost.** Plain QoS 1/2 consumers no longer expire through JetStream
  inactivity while their session is offline: a consumer lives until a route disposes of it
  and nothing holds it, or until its session ends. A session that subscribes and goes away
  keeps its consumers (and the interest they retain on `$MQTT_msgs`) until its Session
  Expiry Interval ends it. This is the deliberate cost of never deleting a consumer an
  obligation may still need.
- **A cancelled transmitted obligation retains its source consumer.** A downgrade or a
  refused drain purges a started delivery's in-flight entry and cancels its register
  WITHOUT unsent evidence (it was transmitted), so its exchange stays at Publish until the
  generation is discarded — F-106's accepted cost — and its hold now retains the source
  consumer for as long. The client-visible outcome is unchanged from before (the started
  delivery is not redelivered; a client that acknowledges the copy it holds ends it, and
  the consumer is then retired); the added cost is the retained consumer. Not
  client-visible.
- **In a generation that still serves, an unfinished retirement waits for the next accepted
  resume (F-105 7f).** The retirement owner is the session's; it stops when the session
  object is no longer resident (a takeover, a restart, a refused CONNECT). A consumer of a
  still-serving generation whose decision stands but whose retirement did not finish, and
  one whose route crashed or lost its entitlement before its decision was recorded, are
  settled by the generation's next accepted tenure; if the client never resumes, the
  session's expiry discards the generation and the recovery executor finishes it (7d).
  Until then they stand — consumed storage, never a deletion under an obligation. A decided
  consumer already CLEANUP_COMPLETE whose duties are still open is left to the discard's
  census: the consumer is gone, and resolving them at every resume would cost a ledger walk
  per resource.
- **The settlement's cost is per resume and grows with the generation's subscription
  history.** Each accepted resume lists the generation's resource subjects (one stream-info
  request per page) and reads every resource record once; resource records are never
  physically deleted (F-112 reclaims exchanges and duties, not resources), so the walk
  covers every consumer the generation ever
  provisioned. It runs in the background after the CONNACK and retries an incomplete visit
  with capped backoff while the session stays resident.
- **A held unnamed source waits on its holds.** A settled consumer still held by an
  obligation stuck at `Publish` (the 7a consult-85 residual) is decided but not retired until
  that obligation ends; with the stuck exchange, it stays for the generation's life.
- **A late CREATE after a retirement** — a queued create landing once the name is RETIRED
  — leaves an orphan the plain orphan sweep reclaims on a later pass; nothing can depend
  on it. It consumes storage until then; it is not promised to exist only briefly.

### A discarded generation's retirement (F-105 7d) — residuals

- **Cleanup after a teardown is eventual, not prompt by promise.** The teardown kicks its
  generation's recovery root and a bounded worker visits it at once (32 visits per kick, a
  bounded set whose overflow is dropped); whatever it leaves is the periodic pass's, which
  promises eventual visitation and no latency bound. Until then a torn-down session's source
  and PUBREL consumers stand, retaining their streams' interest-retained traffic. Not
  client-visible: nothing delivers from them.
- **The retirement deletes consumers and the obligation's `rel` and register, not the F-105
  records themselves.** Exchanges, duties, resources and closures of a discarded generation
  stay on `$MQTT_inflight` (F-112's live-generation reclamation does not reach them either).
- **A discarding generation's named consumer that no resource names is left standing and
  reported**, never deleted on the strength of a missing record. Production claims every
  name before its CREATE, so this is a shape it does not write.

### Retained delivery through the manifest (F-105 7c) — residuals and dispositions

A retained QoS 1/2 delivery is an admitted exchange with its own copy since 7c
(`docs/ARCHITECTURE.md`, "Retained delivery through the manifest"). What that still
leaves:

- **The dispositions, enumerated.** A landed admission is EMITTED, in order, while its
  entitlement and its subscription remain valid and it is still eligible for transmission.
  Otherwise: the subscription departs (UNSUBSCRIBE, its connection's close, a takeover) ->
  withdrawn unsent; no longer fits the resumed connection's Maximum Packet Size, or its
  topic is no longer authorized -> withdrawn unsent when proven unsent, ABANDONED (the
  retained arm of the abandonment fact, reason `packet-too-large` or `acl-deny`) when
  possibly sent. Spec5 [3.1.2.11.4] directs a packet that does not fit to be discarded "as
  if it had completed sending"; the abandonment releases the charge and holds the
  identifier until a valid completion or the generation's discard, exactly as an ordinary
  abandoned delivery (the deviation stated for the ordinary arm applies).
- **A snapshot whose materialization or admission definitively fails is SKIPPED** (the
  loss disposition): every attempt proven never landed, the identifier collisions
  exhausted, or the copy closed before it was admitted. That subscription does not receive
  that retained message; the allocation is released on proof, and the copy is closed and
  reclaimed.
- **The SUBACK precedes the reservation.** The replay is executed after the SUBACK, so a
  crash between the two loses the replay of a subscription that stands: retained messages
  are sent at SUBSCRIBE, and the resume restores admitted exchanges only.
- **QoS 1/2 live deliveries may overtake the retained replay.** The gate covers the
  subscription's raw QoS 0 path only; a live QoS 1/2 delivery rides the durable consumer
  and can arrive before that subscription's retained frames. A live QoS 0 delivery waits
  on the gate; its memory is bounded by the NATS pending limits, beyond which the client
  library drops QoS 0 messages, and a departure drops the waiting ones.
- **An obligation this connection cannot settle closes the connection** (consult 106). A
  retained PUBLISH or PUBREL that cannot be acquired, bound or written, a withdrawal whose
  register delete is refused, a PUBREL entry whose installation is refused, and a replay
  whose authorization verdict is INDETERMINATE are each re-attempted with backoff while
  the connection serves; after eight attempts the connection is closed so the reconnect's
  resume replays the exchange from its records. Authorization recovering while the
  connection stays open delivers the replay there. A PUBREC whose advance is not recorded
  closes the connection as an ordinary one does. An admission that LANDED but whose
  register cannot be read back to promote the allocation is treated the same way — never
  as an unadmitted copy — and so is a materialization step whose attempt could not be
  settled, whose copy the next CONNECT's discovery reclaims. An abandonment decision that
  cannot stand within the budget closes its connection too. A connection is therefore
  closed for a sustained store fault, not only for a protocol violation.
- **A copy's reclamation is retried, not dropped, on a store failure** while its session
  object is resident; a store that keeps refusing keeps a capped retry running per copy
  for the session's life.
- **A retained exchange whose records the store positively answers as unusable is
  QUARANTINED, unowned** (consult 107) — the one exception to the ownership guarantee. When
  answered reads establish one of the enumerated unusable or contradictory conditions (in
  `docs/ARCHITECTURE.md`, "One exit door"), or the register's own shape is refused, the
  resume may succeed with that exchange owned by nothing and with no delivery progress on
  that connection. Its identifier stays unavailable, and the quarantine gives no authority
  to dispose of its records or material. A later resume classifies it again: evidence that
  was corrected restores it, unchanged corruption withholds it indefinitely. This is a
  fail-closed residual for a data-layer fault, not a settlement or a repair. Unanswered
  reads and refused restorations still refuse the resume.
- **Contradiction detection is not integrity protection** (consult 109). The broker refuses
  the contradictions its shared consistency checks and the copy's admission address
  establish — at recovery, at the copy's close and at the acknowledgement advance — and
  quarantine grants no authority to act; an explicit generation discard remains
  independent authority. It does not protect against arbitrary rewriting or misreporting by
  the store: mutually consistent forged records, concealed records, and corruption
  introduced outside the supported concurrency model may evade detection.
- **A resume that cannot read a retained exchange is refused**, before CONNACK, rather than
  accepted with that exchange withheld: a failed read of its exchange, copy, chunks, marker
  or canonical PUBREL — first read or consistency re-read — refuses the CONNECT, and so does
  any walk that succeeds without installing anything — its passes exhausted, or a failed
  enumeration, register batch, marker or PUBREL scan withholding the identifier space —
  while its inventory cannot rule a retained exchange out; the client's
  reconnect classifies again. A sustained store fault, or a retained exchange that keeps
  moving under every pass, therefore keeps such a client from resuming at all.
- **Unfinished material without a CONNECT or a closure** (consult 99, verbatim): a crash
  can leave the session's single active retained materialization unfinished; without
  another CONNECT or generation closure its material can remain indefinitely; CONNECT
  discovers prior material and schedules reclamation; closure exposes it to recovery.
  Cleanup backlog and repeated reconnect/crash cycles have no fixed total-storage bound.
- **On a Discard, 7c reclaims the MATERIAL only.** The exchange's own records — its
  register, its canonical PUBREL and its exchange duty — are the recovery executor's discard
  census's since 7d: the `rel` is deleted first, then the register, and only then may the
  generation certify past the exchange.
- **The close's producer arm is a trusted-caller obligation.** Within the producing tenure
  the write boundary cannot tell the producer's own disposition from another caller of
  this broker composing from the same standing authority; only the producer's workers
  close an unadmitted copy under that arm.
- **Foreground priority is bounded, not total.** A foreground authority operation on this
  broker (the SUBSCRIBE name claim, the PUBREC, PUBACK and PUBCOMP advances) loses at most
  one attempt per retained background worker (three) to their writes. Contention from
  writers outside the handshake — another broker, the recovery pass, the ordinary delivery
  paths, store failures — can still exhaust the SUBSCRIBE claim's eight spaced attempts
  and refuse the subscription; the spacing adds 70-175 ms in that case and bounds no
  latency. Background retained work waits while any foreground operation is announced, so
  continuous foreground traffic can delay it without bound, and a foreground operation
  that never returns keeps it waiting.
- **The other bounded re-attempt loops are unchanged.** Sixteen further loops
  re-attempt an authority or register write with `mqttInflightStoreAttempts` (3) and no
  spacing; they are recorded for classification (`docs/ROADMAP.md`) rather than converted
  here.

### A limit-evicted in-flight register can revive on a crash recovery — UPSTREAM DEFECT

**This is a defect in the data layer, not in this broker** (tracked in
`docs/known-upstream-issues.md`, MAXMSGSPER-REVIVAL). By the boundary ruling of
2026-09-21 (`docs/known-upstream-issues.md`) a store that misreports its own contents is
never this broker's issue. The limitation below is real and current; it is recorded so
the behaviour is not mistaken for a broker guarantee, and it closes when upstream corrects
it and the pin moves. It is not yet filed as its own issue — the tracking artifact is the
closed, deferred PR
[nats-io/nats-server#8506](https://github.com/nats-io/nats-server/pull/8506); a minimal
witness is kept with the lane and a report is drafted for filing after alpha.

**What is true of this broker today.** `$MQTT_inflight` uses a `MaxMsgsPer: 1` subject
as a single-slot register: each rewrite limit-evicts the prior copy, and a terminal ack
deletes the survivor at its exact sequence. On the pinned server a recovery that
rebuilds a stream from raw blocks (a crash, an unclean shutdown, or a stale or missing
`index.db`) can REVIVE an evicted copy when a neighbour keeps the register's storage
block non-empty and the survivor carried an explicit-delete tombstone. A revived
register is read back as a live pending delivery and reserves its packet identifier at
the next resume. **Reproduced on a CLEAN shutdown too (2026-09-24, F-105 PUBCOMP
unit):** a minimal real-server witness on the pinned server — a `MaxMsgsPer: 1` register
rewritten once (by a plain CAS, and separately by an atomic batch), a neighbour record,
the survivor exact-deleted, the subject reading empty — revives the evicted first
version after an ordinary `Shutdown()` and restart, in both modes. The earlier "a clean
shutdown recovers correctly" is withdrawn; no index or rebuild cause is inferred from
the witness. The witness is kept with the lane, outside the tree.

**Scope, and what the broker does about it.** F-105 WIDENS the potential exposure. Since
the cutover every account that accepted a CONNECT keeps `$MQTT_inflight` non-empty for its
recovery epoch (the authority and its permanent outcomes), single-session accounts
included, and each admitted register has permanent neighbours written in its own admission
batch; the population is any account with an admitted register, no longer only
multi-session accounts. The conditions and what is not claimed (no shared-block placement,
no universal revival) are in "The storage this design keeps on `$MQTT_inflight`" above.
Only the shared-group lifecycle records stay off this stream (on `$MQTT_shared`,
`DenyDelete`); the F-105 authority classes cannot move, because an atomic batch is per
stream and the authority commits with the register it authorizes. The broker does not
compensate for the defect and carries no store-integrity layer.

What the broker concludes from a revived register is unchanged in kind: it reads its own
durable records. One case is decided by them positively — an ORDINARY `pubrel_ready`
slot beside an exchange that durably reads Terminal is a completion this broker already
recorded, and recovery finishes it (`docs/ARCHITECTURE.md`, "Recovery finishes an
ordinary slot only on its exchange's word"). That is not a store repair: every other
revived shape is still restored as what it reads — a PUBLISH-phase register as a live
pending delivery, a slot-only class's register by the pre-F-105 rules — and holds its
identifier.

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

**A takeover no longer destroys an owed-delayed Will; what it leaves.** The takeover tail
consumes an owed-delayed Will into the outbox (`mqttSettleWillOnTakeover`), so a delay
that had already run out is published and a reconnect within it suppresses through the
acceptance facts. Three limits remain, all pre-existing in kind:

- *Between the take and a confirmed create the Will is in memory only.* A crash in that
  interval loses it, as it does for every delayed Will between its consumption and its
  enqueue; and the enqueue's two fail-safe exits still publish without the delay, as
  residual 8 states.
- *An earlier connection's parked action outranks this connection's lifetime.* When a
  connection closes under an unresolved write onto a session that already carries a
  parked action of an EARLIER connection (a drain-timeout repark followed by a resume),
  the first action keeps its deadlines. This connection's Will is now discharged from
  its own baseline (`parkClose`'s occupied outcome) where it used to be taken and dropped,
  but its lifetime obligations — the disconnect stamp and the expiry it would have
  parked — are still not recorded.
- *F-125: a disposal that meets both a parked action and a live client completes one of
  the two Wills.* `disposeCaptured` and `disposeForeignStateLocked` share one selection,
  keyed by the licence: a licensed disposal with a live client settles the live Will and
  leaves the parked action's Will undischarged; otherwise the parked action is discharged
  and the live client's Will is left unconsumed. Both Wills are owed. Reachable through the
  same drain-timeout repark and resume; T1b reaches `disposeCaptured`. Accepted as a
  residual, not a roadmap item: a fix needs `mqttForeignDisposition` to carry and complete
  both obligations, with `applyWill` and the disposal fixtures reworked.

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

**Residual 11 - a group message offered to a denied member has no delivery deadline
while that member stays in the group.** A group message offered to a member the
account denies is handed back paced, on the delivery-count backoff, because a sole
denied member otherwise loops against `MaxDeliver` of -1. The cost is on the other
side of that trade. Each re-offer selects a queue member without consulting its MQTT
ACL - nats-server starts from a random member - so selecting a denied member again
costs another paced refusal, and the next gap is longer. With one allowed and one
denied member, the number of offers until the allowed member is selected is
geometric: two on average, with no finite guaranteed delivery deadline. The 30s cap
limits each refusal delay, not the total wait. A member who joins after the backoff
has grown is not woken by the join, and waking the consumer would not remove the
wait either, since a woken offer still selects at random. An isolated run observed
an allowed member that joined at about 3s receive the message at 81.1s, on delivery
11; that is an instance, not a bound. Nothing is lost - the message stays
unacknowledged and keeps being offered. Once the denied members have left and an
allowed member remains, the next offer goes to an allowed member, but a refusal delay
already scheduled still runs first, so that offer can itself be up to the cap away.
Upstream NATS avoids this by filtering denied queue members before selection, which
this port cannot do because the selection happens on the service connection.

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
marker's presence and, since F-088, from an output token the register still carries
beside a standing marker (`inflight_marker.go`, `docs/ARCHITECTURE.md` "Outbound in-flight
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
session's destruction releases it. The durable record
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

**The same cost now covers a NEVER-WRITTEN message whose holder's output authority was
outstanding at recovery (F-088).** Recovery classifies an ordinary register whose unsent
marker stands but which still carries an output token as possibly attempted, keeping the
marker as deletion debt (`docs/ARCHITECTURE.md`, "A late acknowledgement is bound before
it arrives"): the token's holder, a displaced tenure among them, may record intent and
write after the read, and at the read the two outcomes are indistinguishable. Where the
holder never writes (its token drains without intent, or its write is prevented, as in
F-121's B-first ordering, where the takeover sealed the displaced connection's gate before
its certificate cleared the token), a message the client never received is retained:
its identifier and Receive Maximum slot are held, Message Expiry, a drain's withdrawal, a
teardown's purge and the void staging all refuse to release it as unsent, and a later
send delivers it with DUP=1. Over a source that can never be re-offered (Complete or
Lost) nothing sends it again, so it stays reserved until the client's acknowledgement or
the session's end. This is the price of two repaired failures, not a residual of either:
a PUBLISH a displaced holder legitimately wrote, whose delivery the successor then
cancelled, no longer loses the client's PUBACK (the entry is enrolled or routed rather
than withdrawn as unsent), and a register covered by a cancellation verdict that recovery
restores acting is no longer fenced from every retransmission (the restoration retires
the verdict). Both were witnessed failing before the fix, and the same tests are now
their regressions (`TestF088CrossTenureLegitimateTransmissionThenCancellation`;
`TestF088CrossTenureVerdictOverAPossiblyAttemptedRegisterCold`, re-expressed over a
persisted verdict, since the fix removes the producer that made one).

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
  needs to complete — and gets no never-transmitted verdict. A retained QoS 1/2 delivery
  has a register of its own since 7c and its dispositions are the exchange's ("Retained
  delivery through the manifest" below); only a STAGED retained allocation, never
  admitted, is still released at its ABANDONMENT, the one site that proves no bytes were
  written. A staged verdict also fences its own register in `beginSendLocked`, or a
  callback
  already past the subscription barrier could transmit after the verdict was staged and
  make it false. What each route costs is stated below.
- **F-101's own accepted cost, stated rather than concealed behind F-102.** When a consumer
  teardown overlaps an OPEN outbound PUBLISH send section, the teardown preserves the entry
  and its durable owner register and schedules no later release — it cannot decide an
  outcome that has not happened yet, and a release it cannot perform under its own lock is
  not performed elsewhere. If first-send intent then fails DEFINITELY before any write,
  this **can** retain one packet identifier and one capacity charge despite proven
  non-transmission, and repeated occurrences **can** exhaust the session's outbound QoS 1/2
  capacity. **F-102 changes what ends it, and not by much.** A fresh recovery over a
  `Lost` source now CONSUMES a surviving marker-present register instead of restoring it,
  so an ELIGIBLE hold of this shape is released at the next resume rather than persisting
  for the session's remaining lifetime. Eligible excludes a drain-owned, unproven-affine,
  cancelled or retired record. The MEMORY release is applied under the session lock and
  depends on neither the freshness fence nor the pass reaching its drop loop; those two
  govern the DURABLE cleanup only, so a refused or unreached delete leaves the pair for the
  next resume with the identifier already free in memory.
  Two further paths can still end it: a later same-object resume can release the surviving
  LIVE entry through F-100's proven-unsent reap, which a fresh recovery still cannot,
  because it installs no ack subject; and the Maximum Packet Size callbacks keep their own
  independent proven-unsent disposal. What F-101 accepts is unchanged in kind: there is
  still no **prompt** reclamation for this window, and a possibly-attempted hold over a
  `Lost` source is still held to the session's end.
- **Charging a reservation against a NEW connection's Receive Maximum is conservative
  broker policy, not a spec requirement.** MQTT 5.0 §4.9 states that the send quota and
  Receive Maximum "are not preserved across Network Connections, and are re-initialized
  with each new Network Connection... They are not part of the session state". This broker
  counts every outstanding exchange the custody ledger holds, plus the scheduling entries
  the ledger does not own, which is stricter, and deliberately so
  (`receiveMaxOccupancyLocked`, F-105 row 5).
- **CLOSED by F-105 7c: a transmitted, unacknowledged RETAINED exchange no longer holds
  its identifier and its capacity for the rest of the session's life.** It is a durable
  obligation with its own copy, replayed at every resume — with DUP when possibly sent —
  until the client completes it, so its reservation has the ordinary release edge.
- **A STAGED retained allocation whose disposition never runs, or runs after its hold has
  been swept, keeps its identifier.** The release is at the abandonment, and
  `releaseRetainedSendAbandonedLocked` returns early when the hold is gone — rebind sweeps
  a predecessor's owner-scoped inert holds before the displaced connection is closed
  asynchronously, so a late abandonment can find no hold and leave the entry reserved even
  though no bytes were written. Fail-closed and accepted: the alternative is releasing an
  identifier on something other than the proof, which is the defect F-101 removes.
- **F-102 — an eligible release with conditional durable cleanup, and what each word
  excludes.** A proven-unsent register whose source can never be re-offered — *Complete* or
  *Lost* — is consumed by recovery and its identifier freed, on the strength of its unsent
  MARKER rather than of the source classification. That closes the reservation which
  previously had no release edge at all.

  **Eligible** is a real restriction, not a hedge. Only a register that reaches the routing
  site is a candidate, so a drain-owned, unproven-affine, cancelled or retired record is
  excluded from THIS arm — not preserved outright, since other arms may still consume them
  on their own evidence. Those exclusions are what keep ownership where it belongs, and a
  drain-owned register is excluded by nothing else.

  **Conditional** applies to the durable half only. The memory release is applied under the
  session lock; the register delete comes later and can be refused by the freshness fence
  on a same-exchange rewrite, can fail, or can be skipped entirely when an earlier
  unresolved terminal advance ends the pass — no crash required. Each leaves the pair for
  the next resume, and convergence needs storage progress rather than a bounded number of
  retries.

  **A possibly-attempted exchange over a Lost source is still reserved** until the client's
  acknowledgement or the session's end, exactly as over a Complete one. Consumer absence is
  not client completion.

  **A never-sent message can enter that possibly-attempted population (F-121).** After a
  takeover certifies the displaced connection, a send section of it still open across the
  certification (parked there by the witness) can still record intent, a confirmed
  delete of the unsent marker, although its sealed gate then prevents
  the write: the durable state reads possibly attempted for a PUBLISH that never left.
  Where the certificate clears the token before the successor's recovery read (the
  A-first ordering), the successor has already classified the delivery proven unsent, its
  UNSUBSCRIBE withdraws the entry and lands the occupant tombstone, and the exchange is
  left at `Publish` with no entry and no AckRoute, its identifier and Receive Maximum
  capacity held by the custody ledger. No legitimate acknowledgement is forthcoming, since
  the client never received the message, so the hold lasts until the generation is
  discarded. In the
  B-first ordering, where the token is still outstanding at the successor's read, F-088's
  classification keeps the delivery and nothing is stranded. The source's state is as
  observed by the witness (its consumer at `NumAckPending` 2, `AckFloor.Stream` 0,
  `NumRedelivered` 0), never inferred from the recovered register carrying no
  `jsAckSubject`. Pinned by
  `TestF121IntentDeleteAfterCertificationStrandsAWithdrawnExchange`, and filed as F-121 in
  `docs/ROADMAP.md` §0.3; its fix needs an ordering design. **Accepted as a residual on
  2026-10-12:** no owed delivery is lost, because the message never left and withdrawing
  a never-started delivery after UNSUBSCRIBE is permitted (MQTT 5 §3.10.4). The cost is
  one identifier and one Receive Maximum charge per affected exchange, held with no time
  bound until the generation is discarded; repeated occurrences accumulate, and at a
  Receive Maximum of 1 one occurrence stops further QoS 1/2 delivery to that session. This
  says nothing about genuinely transmitted messages, and the logged consumer counters
  establish neither source deletion nor eventual delivery.

  **A displaced subscription churns while its eviction is delayed (F-122, accepted as a
  residual 2026-10-12).** While a takeover's eviction of the displaced broker's resident
  session is delayed, that session's still-live subscription keeps re-offering, each offer's
  admission is refused against the moved entitlement, and its authority writes can starve
  the successor's authority-CAS advances. Completion stays owed and is retried, so it is
  delayed, not lost. It was shown only under a fixture's artificial eviction hold: the
  observed ~1,750 refused identifiers in ~15 s is neither a production rate nor an
  exhaustion proof, and no production eviction-latency bound is established, so the 15 s
  hold is not one either.

  **Identifier exclusion outlives the entry, and its reclamation is bounded but not
  guaranteed** (F-098's C-IV consolidation, then its prompt-reclamation successor). Every
  route that removes local tracking while the durable slot's release is unconfirmed hands
  the IDENTIFIER's exclusion to the send hold. The exclusion charges **no** Receive Maximum
  capacity, so it never consumes the client's in-flight window — but it does **not** follow
  that it cannot throttle delivery: allocation needs a free packet identifier, and
  accumulated exclusions can exhaust the identifier space and stop allocation outright.

  **What is now promised**: while the session is connected and resident, every held
  exclusion is re-tested on a bounded cycle, and it is lifted as soon as every subject it
  binds is CONFIRMED ABSENT. **What is not promised**: any bound on how long a standing
  durable record holds an identifier, and any convergence guarantee at all. The pass
  polls —
  it writes nothing durable and cannot make a record go away — so an exclusion whose record
  never disappears is retested forever and never lifts. A pass is bounded in subject reads
  rather than identifiers, and its cycle length scales with the accumulated distinct
  restrictions, so the capped retry delay is **not** a capped reclamation latency.

  **What does lift one**, so the boundary is not mistaken for "nothing ever lifts": terminal
  cleanup lifts an exclusion whose subject it has proven EMPTY — both the PUBLISH side
  (`recoverPublishTerminalSlot`) and the PUBREL side (`finalizeTerminalSlot`); recovery's
  drop loops and the proven-unsent disposal lift through `settleRecoveryDropExclusion`
  on the same proof; and a RECOVERED exchange adopts its own exclusion back, on a
  unanimous exact subject-and-nonce match. Each capturing route takes the exclusion
  instance BEFORE its confirming read, and a reassertion of an identical bind advances
  that instance, so an absence observed before one cannot discharge it. A successor
  occupying the subject deliberately does not lift: that is the case the
  exclusion exists for. Everything below is what neither route reaches.

  Of the five cases that had no prompt release, **the reclamation pass reaches three**,
  and none of the five needs a crash or a peer:

  - **A landed delete whose confirmation failed.** REACHED. The register is gone but the
    absence was never proven, and recovery reconciles through records it OBSERVES — a
    register that vanished contributes no row — so nothing re-derived the proof. The pass
    re-reads the subject and lifts on the absence.
  - **A handoff installed after its successor already finished.** REACHED. The exclusion is
    correct when taken and no *event* lifts it, but the subject is empty, so the next pass
    confirms it.
  - **A drain that retired from a non-completing phase.** REACHED WHEN THE RECORD GOES. The
    rule installs unless the drain itself completed the exchange, and an ordinary retirement
    leaves an exclusion only a later confirmed absence clears — the pass now supplies that
    confirmation instead of waiting for an unrelated edge to cross it.
  - **A withheld PUBREL record.** NOT REACHED, and correctly so: the record stands, so no
    absence is provable. Continued withholding prevents terminalization entirely,
    so repeated PUBCOMPs consume their debt without ever proving the durable slot released,
    and the identifier stays excluded while the withholding persists. Distinct from the
    pending PUBREL entry beside it, which charges capacity conservatively where the
    exclusion does not. The reachable population here is a persisted affine reference
    this broker does not generate, so this is failure containment rather than a shape
    normal operation produces.

  - **A retired register whose source completed, preserved by recovery.** NOT REACHED, and
    correctly so for the same reason — the record stands. The source's
    completion consumes the source and nothing else: `Retired` is written on the strength
    of the client's own PUBREC, so the PUBLISH provably reached the client and no proof of
    non-transmission can be true of it. Recovery therefore PRESERVES the register and
    excludes its identifier on every pass that finds the record, including a cold resume
    with no in-memory entry to carry the obligation. Nothing lifts it while that record
    stands, which is the correct direction: the client may still be holding the identifier.

  Storage is proportional to the accumulated **distinct** restrictions rather than to the
  65,535 map entries, since differing exchanges and generation-bearing subjects are separate
  constituents and only exact duplicates are suppressed. The asymmetry that ordered this
  work stands: an identifier held too long is an availability cost, where an identifier
  exposed while an exchange still owns it is a lost or duplicated message.

  **The reclamation pass reclaims IDENTIFIERS, never VERDICTS.** A cancellation verdict
  retained because a foreign shared claim did not provably supersede ours is kept
  indefinitely, and a verdict whose register later disappears is kept too — an absence
  observed at scan time cannot exclude a late CAS, so unseen verdicts are deliberately
  preserved. Storage for those is bounded by the number of such cancellations, not by
  traffic.

  **A cancelled experimental affine delivery can still be served after its pending fence
  is disposed of (chapter P).** For the supported classes (ordinary, supported shared,
  retained, and the recovery paths) F-119 closed this: a pending fence of the serving
  generation is deleted only behind a confirmed, entitlement-fenced authority advance and
  only once no admission ladder of its nonce is still running, so no admission attempt of
  a cancelled allocation can land after its fence is gone. What that costs: one authority
  revision per such disposal (a concurrent operation of the session pays one
  compare-and-swap re-read); and a fence whose advance cannot land, whose ladder has not
  returned, or whose session object has no current connection (it cannot tell whether
  another broker now serves the session) stays standing, with its identifier excluded,
  until a later disposer can. The experimental affine (slot-only) class has no authority
  to advance and keeps the plain disposal: an affine admission paused past its ownership
  check, or an ambiguous affine pair that commits after its placeholder was abandoned, can
  still land after its fence was disposed of, and with no surviving verdict a resume may
  restore it acting. That remainder is tracked in chapter P of `docs/ROADMAP.md`;
  [F-119](findings/ha-arbitration.md#f-119-cancellation-must-survive-pending-fence-disposal)
  records the supported closure.

  **A foreign successor keeps the identifier excluded.** When an acknowledgement's cleanup
  finds a successor at a higher acquisition counter, the successor is preserved and this
  broker's obligation is discharged — but the identifier is not freed, because a drained
  action proves only that the ACTION drained, never that the client exchange completed.

  **A condemned open send keeps its capacity charge.** An entry whose transmission has not
  returned is condemned rather than deleted and still counts against Receive Maximum until
  `endSendWith` settles it, so the memory release and the capacity coming back are not the
  same moment.

  **The release rests on a classified snapshot, not on confirmed durable relinquishment**,
  and there is no within-generation fence against a concurrent sender. Worked: broker A
  holds a send token with the marker still present; B reads the marker, classifies the
  source non-reofferable and releases the identifier in memory; B's register delete then
  fails or is declined on a rewrite; A confirms its intent, deleting the marker, and
  writes. B's local release was unlicensed, and the surviving record is now marker-absent,
  so B's next recovery reserves it as possibly-attempted rather than repeating the drop.
  The end state is fail-closed — the identifier ends reserved, never reused — but the
  intermediate release was not proven. `condemnSendLocked` protects a LOCAL active
  section's reservation until it settles; it is not cross-broker authority. This window is
  equally reachable through `Complete` after a TTL expiry and is F-098's, not F-102's.

  **The store path still refuses that identifier while the pair survives.** Until a
  recovery pass deletes it, `storeInflightRegister` meets the stranded record, classifies
  its source `Lost` rather than Complete and fails closed, so an allocation that wraps onto
  that identifier is abandoned and re-offered under a different one. The cost is one
  abandoned allocation and one AckWait-paced retry per wrap, ended by the next successful
  recovery cleanup rather than by the incarnation.

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

Two qualifications after F-102. Over a source that can never be **re-offered** the
duplicate-delivery consequence usually cannot arise at all, because nothing remains to
offer the source — but "usually" is the honest word: a payload already dispatched to a
callback is not retracted by the consumer's deletion, so an outstanding sender can still
put those bytes on the wire. And a marker-proven drop reaches this residual by a second
route: its delete is declined outright on a same-exchange rewrite, deliberately, and it
then leaves the marker alone as well, so the pair stays intact for the next resume rather
than becoming a half-torn pair.

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

The teardown-owned release of shared group durables is **outside** this boundary: it is
retirement work created after the displaced incarnation has drained, not an effect derived
by an admitted displaced route. Its target is safe by a property of its own — a
`CONSUMER.DELETE` is issued only for an incarnation the group record has moved into
`retiring`, whose incarnation-qualified name no successor reuses — so the scan-then-delete
race on a deterministic group name that used to be recorded here is CLOSED with the
shared-group lifecycle transfer, not carried.

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
  `$MQTT_*` streams — all ten, which are one recreation unit.

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
  limit is set — or cannot be read — the broker **refuses to drain**, and logs a warning
  naming the limit. Since F-105 7c-lc the UNSUBSCRIBE no longer tears the consumer down: a
  started QoS 1/2 delivery's obligation holds its source consumer, so the consumer is kept
  until that obligation ends and only then retired. The started delivery is still not
  RETRANSMITTED (its in-flight entry is purged, as before), so [MQTT-3.10.4-3] is honored
  only where the client completes the delivery it already holds; a client that never
  answers leaves the obligation, and the consumer it holds, standing until its generation
  is discarded (see "A cancelled transmitted obligation retains its source consumer"
  below). Running `$MQTT_msgs` without a stream-level consumer inactivity limit is
  therefore a deployment requirement, exactly as it already is
  for shared subscriptions. The `$MQTT_msgs` startup validator deliberately adds
  no check here: the limit can be imposed *after* startup, so these runtime
  refusals remain the authority (the same field **is** a startup failure on
  `$MQTT_out`, whose affine durables must survive offline sessions — see
  "Stream prerequisites" in `docs/ARCHITECTURE.md`).
- **Per-consumer inactivity timers.** Since F-105 the broker creates every plain
  QoS 1/2 source consumer (and the PUBREL consumer) with NO inactivity threshold,
  and has no option that would request one, so there is no per-consumer timer to
  clear when a drain is committed; the clearing mechanism is deleted. Admission
  verifies the consumer's EFFECTIVE configuration as the server reports it and
  refuses a consumer an administrator armed — see "The supported-configuration
  boundary for consumer inactivity" below for what that snapshot cannot cover.
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
    removes withheld markers for shared durables — deliberate behaviour that stops a
    resume NAK-stealing group messages. It gives up only a shared delivery PROVEN never
    transmitted (withdrawn through the ordinary door since 7a); a possibly-transmitted one
    keeps its identifier and custody, since a disconnect is not client completion. Kept as
    a separate statement from the race above because it does not depend on any
    interleaving. Fixing it means changing what a disconnect does to shared pending state,
    which is not F-050's.
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
- **A stale source re-offer after an exchange's in-memory PUBREL-phase entry is released
  (F-115).** For ordinary source deliveries the broker declines a fresh allocation while the
  source is represented by an in-memory PUBREL-phase entry, and a send section open when the
  client acknowledges its PUBLISH does not write. PUBCOMP carries that representation in a
  terminal reservation until its release: normally after confirmed terminal cleanup, or at
  PUBCOMP in the locally held action and retained-reoffer arms. A stale offer dispatched
  before JetStream processed the source acknowledgement (a dispatched redelivery is not
  recalled), but reaching admission after that representation has been released, can be
  admitted as a fresh delivery if no other existing guard suppresses it, and the client then
  receives the payload a second time. Terminal cleanup may still be owned elsewhere at that
  point. The schedule needs the source durable's delivery callback held across the PUBREL
  delivery and the client's PUBCOMP round trip, through the reservation's release; before
  F-115 the same gap opened at PUBREC. Closing it would need a separate queue-boundary
  mechanism (declining a source for the offers already queued on its subscription when the
  acknowledgement was confirmed), which is not built. The experimental session-affine class
  keeps its existing order. See "An acknowledged PUBLISH is not sent again" in
  `docs/ARCHITECTURE.md`.
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
- **A drain whose completion is retried after a failed save keeps its consumer until
  resume settlement or generation disposal (F-124).** When the save that removes a drain's
  last claim fails, the retry belongs to the terminal-source continuation, which is armed
  with no connection (`holdTerminalSourceLocked` calls `armTerminalRetryLocked(nil, ...)`).
  Its drain finalize therefore reads a zero removal entitlement, the removal mark accepts
  it, and the disposal correctly refuses to record a decision no accepted entitlement made
  and leaves the consumer for recovery, against the drain's own invariant that the
  removal is recorded under the deciding connection's entitlement or not at all. The
  observed exchange completes and releases resources in-session; it does not demonstrate
  false non-transmission, lost completion authority, or conflicting delivery under C1. The
  retained consumer is a supported-feature liveness/storage defect. Under
  `InterestPolicy` the retired drain consumer keeps retaining later publications that
  match its filter until the generation's next accepted resume settles it (F-105 7f) or
  the generation is disposed of; no fixed time or storage bound is claimed while the
  session continues. Characterized by the injected arm of
  `TestF088RecoveredDrainClaimRetiresDespiteAFailedSave`, which gets no credit for
  in-session disposal after the failure. The mechanism is in code the F-097/F-088
  close-out leaves unchanged against `48a9003`; it was observed with that change applied
  and not separately reproduced on `48a9003`. Filed as F-124 in `docs/ROADMAP.md` §0.3
  and accepted there as this residual on 2026-10-12: it needs a failed last-claim removal
  save.
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
- **A destroyed downgrade's durable is unnamed but not unowned from the confirmed save.**
  The consumer reference leaves `sess.cons` at the packet's final-update boundary, so that
  the record carrying the QoS 0 membership does not name a durable the packet then deletes.
  Between that save and the teardown the durable is named by neither the record nor the
  session, but its name stands LIVE, and no orphan sweep — a **peer** broker's included,
  whose sparing pass cannot see another broker's resident sessions — deletes a durable whose
  name is not RETIRED (F-105). The packet's own teardown records the disposal decision and
  the retirement owner retires the name and deletes the durable.
- **On an AMBIGUOUS landed save the durable loses its prompt cleanup path.** The packet
  returns before its deferred-delete loop and a destructive downgrade registers no
  continuation, so the record lands carrying the QoS 0 membership while the durable is still
  alive, named nowhere and LIVE. No orphan sweep reaps it. Convergence is the session's
  **next accepted resume**, on any broker: its tenure settlement decides a source consumer
  provisioned under a tenure that no longer serves and named by nothing the serving tenure
  holds, and the retirement owner retires it. A session that never resumes keeps the durable
  until its end discards the generation; until then the durable keeps its filter's messages
  on the interest-retained stream.
  - **Convergence is demonstrated for a broker that is gone, up to a completed graceful
    shutdown.** The cross-broker fixtures stop the owning broker, await its shutdown
    lifecycle to completion, and resume on the peer — in both orderings, resume before the
    stop and stop before the resume — with the peer's sweep withholding the LIVE name
    throughout. An **abrupt** loss of the process is not exercised, because it is not
    constructible in an in-process harness, so nothing here is claimed about an ordering
    only an abrupt death produces.
  - **The session's `MaxAckPending` charge IS recovered, by the resolution rather than by
    the sweep.** Proving those bytes durable runs a refund-only landed continuation for
    every released durable, so the resident session does not keep a charge no consumer of
    its own can spend — a charge nothing gives back would cost a subscription slot for the
    life of the session, refused at the aggregate ceiling. The continuation gives back the
    charge and nothing else: no `Cons` entry returns, and the **durable** is still the
    retirement owner's. A **forked** or **foreign** verdict discards the continuation,
    which is correct, because the session is then being disposed of rather than kept.

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
entry. Whether the inherited exchange then survives a failed-store settlement is still
F-088's, per the residual above.

**A cancellation of the replacing allocation no longer frees the identifier.** A
cancellation is not evidence that a PUBLISH was never sent (F-088: the release authority
admits a never-transmitted release only on the allocation's standing unsent marker), so a
cancelled register is classified by its marker like any `publish` register (and, by
recovery, as possibly attempted when an output token stands beside the marker), and a
register-only replacement, which mints no marker, reads possibly-attempted. A cancellation
that meets a landed register-only replacement leaves the register in place and keeps the
identifier excluded, and the custody ledger holds the identifier until the exchange ends.
The guarantee this section states for the deviation is still confined to the **QoS byte**.

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

- **CLOSED by F-105 7c: retained-message deliveries are covered across a restart, and so
  is the identifier.** An admitted retained QoS 1/2 delivery has a register, an exchange
  and a copy, so a restart restores it like an ordinary one — replayed from its verified
  bytes under its original identifier (DUP when possibly sent), its PUBREL from the
  canonical record — and the identifier is the ledger's, never re-minted while a QoS 2
  client holds it. What F-091 built for the in-memory phase now covers only the STAGED
  allocation between the snapshot and the admission, which a crash loses together with the
  replay ("Retained delivery through the manifest" below).
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
  is no exception since 7c: it has a register, and its PUBREL is acquired on it and
  written from its canonical record on `$MQTT_inflight`. A record naming a different
  exchange is refused and, deliberately, **not acknowledged**: AckWait re-offers it, so a
  refusal can never lose a PUBREL the client is owed. F-093 goes with it — a completed
  exchange leaves no `pendingPublish` entry and no `cpending` mapping, so it no longer
  reserves an identifier and a Receive Maximum slot for the life of the session.

  **THE RESIDUAL: an abandoned action holds its identifier for an unbounded TIME.** An
  abandoned action is any acquired action whose holder can no longer confirm its durable
  drain.

  **F-098 WIDENS THIS RESIDUAL from the PUBREL path to every outbound QoS 1/2
  transmission.** A PUBLISH transmission now acquires a durable action of its own, because
  the process-memory hold it had before fenced no peer: two brokers may legitimately serve
  one incarnation, so `sendsInFlight` excluded nobody, and `recordSendIntent` records
  nothing at all on a retransmission (it returns `intentNone` whenever the marker is
  already gone). The exclusion is real now, and so is its cost: **an ordinary QoS 1
  delivery, or any QoS 1/2 retransmission, whose broker dies mid-send strands its
  identifier, its Receive Maximum capacity and the drain serving that source** until the
  holder confirms the drain, the generation is discarded, or an operator intervenes. The
  ends listed below are unchanged; only the population reaching them is larger.

  **A graceful shutdown joins every admitted PUBLISH send section**, and re-attempts every
  unresolved drain and terminal cleanup by its holder,
  while JetStream requests still work (see `docs/ARCHITECTURE.md`, "A graceful shutdown
  lets every holder finish its own drain"). What still strands one is a shutdown that
  cannot confirm the drain before the process ends — abrupt termination, a forced kill, or
  storage that stays unavailable through the final pass.

  **An acknowledged exchange whose terminal cleanup cannot be confirmed holds its
  IDENTIFIER, not its capacity.** The client has completed it, so Receive Maximum is freed
  at once; the identifier stays reserved until `publish -> publish_terminal -> delete` is
  confirmed, and while storage is unavailable that can be indefinitely. It is an
  availability cost on one identifier, never a release: nothing re-mints it while the slot
  still names the exchange. A terminal record recovery cannot delete costs the same one
  identifier, and nothing else, until its delete is confirmed. An acknowledgement parked
  on a section whose durable action never resolved holds the same identifier for the same
  reason, and its cleanup stays BOUND TO THE TOKEN rather than to a counter that may
  already be stale: whoever resolves that token — the reconciler, or the shutdown's final
  pass — binds it to the record in hand. Storage that never returns leaves both the token
  and the identifier held, which is the same availability cost stated above.

  Finite failures recover — a validation or opener failure aborts before the write,
  an ambiguous acquisition adopts its own draining successor, a recovered panic runs the
  armed abort, a finite CAS or drain failure retries, and a proven generation discard
  retires the holder. What does NOT recover is process termination, a permanently leaked
  goroutine or blocked socket write, and indefinitely unavailable storage.

  There is deliberately no lease, TTL or steal, and that is not conservatism: a broker can
  pause AFTER its own expiry check and resume into the same stale write, and an MQTT client
  cannot validate a fencing token, so the external effect is not fenceable downstream. An
  abandoned action therefore ends only through its own holder confirming the drain, a
  persisted closure certificate of the connection that acquired it (F-105 8c), an explicit
  whole-generation discard (clean start or session expiry), or destructive operator recovery
  under the existing all-ten-stream reset boundary. Since 8c the residual is NARROWED to an
  ordinary action with no confirmed holder drain and no USABLE matching certificate: a crash
  before the certificate was persisted, a write that failed or was lost, a certificate that
  is unreadable or names another connection, or a clearance write the store cannot take. A
  slot-only (experimental affine) acquisition writes no action record, so no certificate can
  end it (chapter P).

  **The bound.** Per session it is the actions admitted under whatever limits were in force
  when each was acquired — ultimately the 65,535 per-session identifier space, and NOT the
  session's *current* effective capacity, because Receive Maximum is re-evaluated on every
  CONNECT and a session may acquire many actions under a high limit and reconnect
  advertising 1. **Across sessions there is no global bound**; the total is the sum over
  affected sessions. CONNECT and QoS 0 continue and other identifiers stay usable, but at
  Receive Maximum 1 a single reserved identifier stalls further QoS 1/2 delivery for that
  session — and a reconnect at Receive Maximum 1 may find several already reserved.

  **A REFUSAL NOW LEAVES A CONTINUATION, with one stated exception.** A stored PUBREL
  refused because a transmission is live on its slot — either half: the durable action
  token, or the local `pubIdent` naming a PUBLISH send section — registers a waiter that
  the transmission's own end NAKs, so the re-offer is immediate instead of an AckWait
  interval. A retained exchange's PUBREL is no stored record since 7c: an emission that
  could not acquire is re-attempted by the session's continuation worker.

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
  until generation teardown or operator removal. **One schedule produces such residue on a
  live connection** (pre-existing; stated by F-116, which did not create it). The PUBCOMP
  handler consumes the PUBCOMP's debt first and then releases the session lock for its
  affine and retired-source discharges before it settles through the entry. An accepted
  ending of the exchange's lineage landing in that window finds nothing owed, so it removes
  the entry rather than converting it to a released delivery; the handler then finds no
  entry and returns without acknowledging the stored PUBREL or terminalizing the slot. A
  surviving terminal slot's records are still disposed by terminal cleanup; with the slot
  already gone nothing reaps the record, which stays until generation teardown deletes the
  PUBREL consumer or an operator removes it. F-116 adds a second route into the same
  outcome, no wider: a released delivery retired when a later attempt on the same
  identifier, refused after entering its write, gives back the last owed unit while that
  PUBCOMP is still mid-discharge. The identifier is correctly free in both, since the
  client answered every PUBREL it received. An absent slot alone cannot prove an
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
  can win the direct arrow to `pubrel_ready` before that CAS runs — but the
  pre-existing canonical tuple and phase history: the slot already named an earlier proven
  record and had already left `publish`.

  No scan ordering can close this. The window spans two streams and JetStream atomic
  batches are per stream. It belongs to F-098, which owns exclusion across terminal cleanup
  and PUBREC advancement including the store-before-phase window.

  **Scope since the F-105 PUBCOMP arm: affine slots only** (shared slots were in scope until
  7a made them ordinary; a shared delivery is QoS 1 and has no PUBCOMP at all). For an ORDINARY
  exchange step 3 no longer happens — PUBCOMP withholds the canonical PUBREL's acknowledgement
  until the completion is recorded on the exchange and the slot — and step 6 no longer happens
  either: recovery finishes an ordinary `pubrel_ready` slot only beside a durably Terminal
  exchange, never on an absence. The schedule above remains for the slot-only classes.

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
  the ten broker-owned streams are one recreation unit (F-066).
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
- **CLOSED by F-105 7c: a retained QoS 2 exchange's PUBREL survives a restart and is
  TRANSMITTED** from its canonical record, acquired on the exchange's own register. The
  slot-less admission (`retainedPubRelAdmittedLocked_`) is deleted, and a `$MQTT_out`
  PUBREL that no slot-backed exchange owns is refused.
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
  **The same withholding can begin mid-connection**, from a live session's own
  admission, on either of two marker reads. Right after an allocation's register and
  unsent marker land, the marker is read back and anything but this exchange's marker —
  absent, another exchange's, or unresolved — withholds it. Beside a same-delivery
  occupant the allocation would replace, the occupant's marker read as neither the
  occupant's nor authoritatively absent withholds it (an authoritatively absent marker
  there instead permits a register-only replacement). Either way the identifier is
  reserved marker-opaque — nothing is sent or settled under it — and the identifier space
  is marked incomplete, so no new identifier is allocated for the session at all, retained
  deliveries included. An indeterminate read can be an ordinary transient failure, not
  only a misreporting store, so this is a stated availability cost rather than a
  data-layer exclusion. It clears only when a later resume completes its reconstruction
  with no unaccountable record and no incomplete apply — which resolves the marker's
  disposition and need not find both records present — or at a session discard; there is
  no live-session release.
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
  makes recovery drop a register outright, and since F-088 only while the allocation's
  unsent marker stands and no output token is outstanding on the register, so what is
  lost is that close's purge and what returns is the pre-purge state — the identifiers
  and Receive Maximum slots it had freed. Bounded to one crash per close and
  self-limiting: the next surviving close purges again. The same window applies to every
  close, not only the record-restored sessions the purge newly reaches — the DEFERRED
  close included, where the linearization points are the cancellation tombstones
  (written first), the exact deletes, and the one classified record save: a crash
  part-way through the rungs leaves some allocations durably tombstoned and others live
  with no verdict, and a crash after the rungs loses a fallback verdict whose nonce
  existed only in memory.
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
    The reconnect race (F-040) reaches the same write from the other side: a takeover
    that holds the gate while the DISCONNECT's connection is still bound now performs it
    on the close's behalf before deciding (see "A reconnect racing its own DISCONNECT"
    below), so the definite-write-failure intersection above is reachable from that
    caller too, with the same warning.
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

### A reconnect racing its own DISCONNECT

F-040: a v5 client that ends its session with DISCONNECT (Session Expiry Interval
0) and reconnects could be told Session Present 1, with its old subscriptions
resumed, when the reconnect took the ownership gate before the old connection's
close did (Spec5 [3.1.2.11], [MQTT-3.2.2-3]). A takeover that finds the
DISCONNECT's connection still bound now persists the ending on the close's behalf
and discards; see `docs/ARCHITECTURE.md`, "The DISCONNECT lifetime override is a
connection's OBLIGATION". Three gaps remain.

- **A DISCONNECT recorded after the takeover's sample is a displaced connection's
  DISCONNECT, and does nothing.** The broker observes the override when the packet
  handler records it under the connection's lock, not when its bytes arrive or are
  parsed. A client that writes DISCONNECT and immediately reconnects on another
  socket can therefore still see Session Present 1 when the broker processes the
  new CONNECT first: the two connections carry no order the broker could observe,
  and it serves them in the order it recorded them. Rare in practice, because the
  takeover samples only after its own record round trips; under heavy load it is
  the remaining way `TestMQTTv5SessionExpiry/disconnect_overrides_expiry_to_zero`
  can fail.
- **Across brokers, the record arbitrates.** The override is memory-only and is
  never marshalled, so a CONNECT on another broker sees only the record. If the
  close's F-041 write lands first, the remote acceptance fails its CAS or
  re-drives onto a Clean record and discards. If the remote acceptance's metadata
  lands first, the close's write takes a disposing verdict and the remote CONNECT
  proceeds to resume, subject to its own later acceptance failures, even though
  the first broker had already recorded the DISCONNECT. Closing that would need a
  remote acceptance to be ordered against a local obligation; nothing here does
  that, and it is not claimed to be impossible.
- **A definite failure of the takeover's ending write** leaves the pre-override
  terms durable, exactly as on the close's own path (the F-041 residual above). The
  takeover still discards in memory and warns; a retirement that then fails after
  its detach leaves a resumable record.

### No Local is decided when the message is owned

F-117: a re-SUBSCRIBE that changed the No Local option re-judged messages the
broker already owned whenever their offer reached the subscription after the
re-SUBSCRIBE. Turning it OFF delivered the client's own earlier message
([MQTT-3.8.3-3]); turning it ON acknowledged away an admitted message, a loss
([MQTT-3.8.4-4]). The decision is now taken at ownership and carried by the
message; see `docs/ARCHITECTURE.md`, "No Local is decided when the message is
owned". Three gaps remain.

- **Another server run does not enforce it.** The tags are keyed by a per-run
  secret that is neither persisted nor shared, so a self-published message
  forwarded by a run other than the one that owned it (after a restart, a
  takeover to another node, or a QoS 2 hold moved to another broker) does not
  verify and is delivered to its publisher. No Local fails open, never dropping
  a legitimate message.
- **The order is per connection.** A connection's PUBLISH and SUBSCRIBE packets
  are ordered on its read loop. Where a displaced connection keeps serving after
  ownership moved, a message owned through one connection is judged by that
  connection's options, and a re-SUBSCRIBE on the other is not ordered against
  it.
- **The QoS 2 PUBREL window.** QoS 2 onward fan-out and storage occur at PUBREL,
  so matching subscriptions created after PUBREC can receive the message despite
  not existing at ownership ([MQTT-4.5.0-1]). With the ownership-time No Local
  stamp, this also permits self-echo to a publisher's own No Local filter
  created in that interval, which the former live-option check suppressed. That
  self-echo is new behavior from F-117; the window it rides is pre-existing and
  is the defect to repair. `TestF117PubRelWindowSelfEchoResidual`
  characterizes it and changes when the window is repaired.

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
  no longer lost there. **What the discharge does NOT carry is the session's expiry**
  (F-028, accepted as a residual 2026-10-12): the foreign discharge performs no session
  save and no expiry scheduling, so an ambiguously parked close later disposed as foreign
  leaves session expiry to the surviving record and its recovery, with no guarantee
  measured from the original deadline. Retained `uncertainLanded` continuations are also
  not cleared, and stay reachable while the old object is referenced, with no fixed
  reclamation bound. Both are PRE-EXISTING on the foreign path and rare: they need an
  ambiguous write and then a foreign verdict. This does not claim every parked or live
  Will is preserved; the F-125 overlap is stated separately.
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
- **A drain whose consumer another broker decided is NOT reinstated, and what only it
  carried is lost (F-105 7f, consult 116).** Fork repair reinstates a consumer of ours
  only while it still serves; a DRAIN of ours whose consumer the survivor's tenure has
  since disposed of (its record did not name it) is omitted, identity and contents. Where
  that drain carried evidence or pending work no independently continuing exchange
  represents — a PUBREL intent persisted before its exchange's PUBREC advance (the
  inventory still reads `Publish` and cannot infer the PUBREC), or an identity-bound
  release request — an owed PUBREL can stay unproduced, or a nonterminal exchange can keep
  its packet identifier, Receive Maximum capacity and resource holds for the generation's
  life (a Receive Maximum 1 client then receives nothing further); a reconnect alone does
  not repair it. Scope: a recognised erased branch carrying a drain the survivor lacks,
  AND a cross-broker settlement deciding its source, AND such drain-only work. Ordinary
  crash and reconnect recovery is not affected; no completion is inferred and no
  identifier released from the omitted drain. A MEMBERSHIP whose consumer was decided
  keeps its grant and gets a fresh consumer from the resume reconcile; what that consumer
  could no longer deliver is the vanished-durable repair's existing loss.
- **Fork repair's fence can refuse a CONNECT under sustained authority writes.** When fork
  repair reinstates a consumer, the accept's handoff preparation must find the outbound
  authority exactly where fork repair read it, and any write since — a disposal, or an
  unrelated write by a still-live predecessor connection — forces a full re-drive. Under a
  sustained stream of such writes the existing bounds (three full re-drives, the ownership
  deadline) end the CONNECT in a retryable refusal rather than a guaranteed acceptance. An
  unreadable fork-repair read also refuses the CONNECT before its accept, the repair kept
  for the retry.

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
      (`writeActionlessFrameBy`) after releasing it.
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
  memory-only and non-transferable. **Accepted as a residual (F-038 arm (a), 2026-10-12):**
  a remote takeover racing an ordinary finite close can cause one unwanted immediate Will
  from that close, which subscribers may read as a false offline notification; it is never
  a lost Will, and its scope is that close's publication, not an exactly-once guarantee.
  Retracting it needs a durable Will intent. The periodic pass's bound-foreign disposal
  that this bullet used to name as a second route no longer reaches the takeover arm
  (F-084).
- **A rejected finite close that crashes before settlement loses its deadline (F-037,
  accepted as a residual 2026-10-12).** The ordinary close's finite disconnect save, when
  rejected, leaves `disconnectedAt` set in memory only; a landed probe re-saves it, but a
  crash before any settlement write lands leaves the record reading as connected. Such a
  persistent session can survive its original expiry deadline and resume later than the
  client asked; recovery may reconstruct a later deadline (the 15-minute sweep stamps it),
  but no bound measured from the original disconnect is guaranteed, and the sweep's origin
  gate can defer it until the originating broker returns. Reached only by a rejected save
  followed by a crash in that window.
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
  not by the lease. The wait covers the `processSubs` runs in flight when it sampled
  (later SUBSCRIBEs cannot extend it), and each creates a JetStream consumer per
  filter, so one CONNECT session restore with a few dozen subscriptions on a cluster
  timing requests out holds it past the 30s lease. A peer
  reclaims the still-`publishing` record, reuses the persisted decision and publishes
  again. **One stall yields two publications, not unboundedly many.** `mqttSubsBarrier`
  is per account manager, so the stalled broker's `processSubs` does not stall the peer:
  the peer's worker runs through, commits `done` and deletes the record. A third
  publication needs each successive claimant to ALSO outlive its own freshly minted
  lease or fail terminal reconciliation — possible, but a separate condition rather than
  something the first stall causes.
  Bounding the barrier is not the fix — the fan-out precedes it, so abandoning there
  would skip the store that protects the racing subscriber and trade a duplicate for a
  LOSS. **Accepted as a residual (2026-10-12):** subscribers can receive a duplicate Will
  when a claimant stalls past its lease during subscription restoration; do not advertise
  an unconditional maximum of two. End-to-end phase-aware ownership, described in the
  ticked `docs/ROADMAP.md` §0.4 entry, remains the fix if it is taken up.

  The permanent acceptance facts did not introduce this: a slow barrier always let a
  peer reclaim and republish. What they changed is that the peer now reuses a persisted
  decision instead of rescanning, which makes an already-permitted trace DETERMINISTIC
  rather than adding a new one. **This is the only duplicate class left, now that the
  terminal regression above is retired**, and the line was always evidence rather than
  liveness: here `done` was never committed and the system genuinely lacks proof of
  completion, whereas the retired class had `done` COMMITTED and then taken back —
  evidence it already held, discarded.
- **Per-CONNECT permanent records accumulate for the recovery epoch, and a reconnect loop
  can exhaust storage.** Accepted residual, not fixed and not bounded (decided 2026-10-12;
  the `docs/ROADMAP.md` §0.3 item "Reclaim CONNECT acceptance facts, or bound them" is
  closed on this decision).

  *What accumulates.* Every successful CONNECT leaves at least four permanent records: its
  acceptance fact and its create-only handoff receipt (`$MQTT.handoff.>`), both on
  `$MQTT_sess` under subjects unique to the acceptance, and at least two F-105 transition
  outcomes on `$MQTT_inflight` (reserve or prepare, then finalize). A CONNECT that failed
  after committing some of them can leave records too. Records created by LATER activity
  are not per-CONNECT and are not in that minimum: those on `$MQTT_inflight` (admitted
  obligations, generation closures) are listed in "The storage this design keeps on
  `$MQTT_inflight`"; on `$MQTT_sess`, a STANDALONE discarding teardown writes a create-only
  teardown receipt (`$MQTT.trcpt.>`) keyed per generation, which accumulates per discarded
  generation on the same stream (not every discarding close writes one: a clean-start
  handoff discards its predecessor without it). Neither stream can carry global limits,
  which would evict live records, and `MaxMsgsPer: 1` does not apply because each of these
  records has its own subject.

  *The exposure.* Storage grows with cumulative activity throughout the recovery epoch,
  which has no end short of a reset, and one repeatedly reconnecting client suffices. The
  broker enforces no storage ceiling and no acceptance throttle, so any credential
  permitted to CONNECT can exhaust the account's JetStream storage and with it deny
  service to every client of the account. This is cheap, authenticated resource
  exhaustion, not a rare edge case; it is accepted consistently with the F-105 permanent
  records above, whose own retention argument it shares.

  *Why the evidence is kept.* The reclamation rule once proposed for facts — reap every
  fact of client `C` at or below `L = min(C's session-record sequence, min OriginSessionSeq
  over C's non-terminal $MQTT_willq records)` — is refuted twice, and so is its amendment
  that keeps the current acceptance's own fact:

  1. A fact has readers besides the Will scan. `mintAcceptanceTenure` reads the CURRENT
     acceptance's own fact back on every CONNECT and refuses the CONNECT unless it reads
     landed; the uncertain-acceptance resolver reads an attempt's own fact as the monotone
     half of its readback, and a fact that reads absent beside a record that is still the
     attempt's leaves the session unresolved for as long as that pairing persists. A fact
     commits below its own record, so `L` takes exactly that fact.
  2. A delayed Will can be outside `$MQTT_willq` for an unbounded time while its origin
     connection already exists. Worked interleaving, with auth passthrough enabled: client
     `A` connects on broker 1 (record `r0`) and closes with a delayed Will due at `T`;
     broker 1's service NATS connection is unavailable, so the enqueue is refused before
     any effect and the Will is held in broker 1's retention queue, carrying its original
     origin `r0` and deadline `T`. Broker 2, connected to the same account and domain,
     accepts `A` before `T` (fact `f1`, receipt, record `r1`) and again after `T` (fact
     `f2`, receipt, record `r2`), so `r0 < f1 < r1 < f2 < r2`. A reaper sees no outbox
     record for `A`, so `L = r2`; with the amendment it keeps `f2` and reaps `f1`. Broker
     1's connection returns and the Will is enqueued with origin `r0`. Only `f1` qualified
     (dated before `T`); `f2` cannot substitute, so the Will is published although `A`
     reconnected within its delay.

  *Failure direction of a wrong reaper.* Deleting suppression evidence publishes a Will a
  timely reconnect should have suppressed — a false affirmative built on an absence.
  Deleting an acceptance's own fact refuses its tenure, and so the CONNECT, or leaves an
  uncertain acceptance unresolvable. A reaper that decided from a "complete" outbox walk
  would also inherit F-108 in the destructive direction: a truncated walk that missed a
  protecting Will would authorize the deletion.

  *Operational limits.* Monitor `$MQTT_sess` and `$MQTT_inflight` message count, storage
  and growth rate; monitoring is not a bound. Pruning facts or receipts administratively,
  or adding evicting limits to either stream, is not a safe remedy: it removes evidence
  silently and can let a superseded Will fire or refuse a CONNECT. The only way to shed the
  accumulation is the coupled reset of all broker-owned streams (`docs/ARCHITECTURE.md`,
  "The ten streams are one recreation unit"), with everything that reset discards.

  *Scan depth.* The claimant's own walk is narrower than the history, but only within one
  incarnation: for a Will whose origin epoch is current it starts at `OriginSessionSeq + 1`
  and stops at the cutoff, so it reads only facts written after the connection that
  enqueued that Will. For a Will from ANOTHER incarnation the walk starts at the beginning
  of the stream, because that origin sequence names a store that no longer exists, so its
  scan depth is the whole current history. Since F-066 that case needs `$MQTT_willq` to
  outlive a `$MQTT_sess` recreation, which only an unsupported partial operation produces.
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
nothing and persists with its consumers until an operator resets the ten-stream
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
- **A whole-bundle reset loses every open fence**, along with everything else the ten
  streams hold. `$MQTT_teardown` recreated ALONE is no longer admitted — it used to be,
  stamping the current epoch and starting clean with the fences silently gone, and that
  admission is exactly the residual F-066's enforcement withdraws: an account holding
  nine of the ten is now refused before anything is provisioned. Watermark storage is
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
recreating `$MQTT_qos2in` by itself is no longer a SUPPORTED operation — the ten
broker-owned streams are one recreation unit, and an account holding a proper subset is
REFUSED at the next manager initialization — so the supported operation is an
all-brokers-stopped reset of the whole bundle (`docs/ARCHITECTURE.md`, "The ten streams
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
resetting the whole ten-stream bundle — recreating this stream alone is REFUSED at
the next startup, since the broker-owned streams are one recreation unit; it is not a
rolling upgrade.

### Broker-owned stream prerequisites: startup refusal, a snapshot, and dropped fallbacks

Every broker-owned `$MQTT_*` stream is now validated at account-manager
initialization against the list its own protocol derives (see "Stream
prerequisites" in `docs/ARCHITECTURE.md`). This extends the startup-refusal
surface that previously covered five streams to **all ten** — the provisioning
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
  **stop-all**: stop every broker for the account, delete **all ten** broker-owned
  streams, then restart, which provisions a fresh bundle. The errors offer no in-place
  fix and no partial procedure, because there is none: the ten are one recreation unit
  (F-066).

  **What the enforcement catches, and what it does not.** Two mechanisms, neither
  subsuming the other. A COUNT rule in a lookup-only preflight refuses an account holding
  one to nine of the ten — a member ABSENT when a broker starts, which is what a
  deletion nobody replaced or an interrupted first start leaves — and its value is that
  the broker no longer silently recreates it. A per-stream EPOCH stamp, minted by
  `$MQTT_sess` and compared by every validator, refuses a complete bundle assembled from
  different incarnations — a partial restore, or a replacement created without the
  account's stamp. **Neither catches an operator who deletes a member, recreates it by
  hand and copies the surviving epoch onto the replacement**: the count sees ten and the
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
    ten belong to the same bundle; it does not prove ten independently restored
    snapshots came from one coherent instant. Two backups taken minutes apart carry the
    same epoch and are admitted while being mutually inconsistent.
  - **The preflight is ten independent lookups, not an atomic snapshot.** Under
    create-only concurrency an admission cannot wrongfully succeed. **"Create-only
    concurrency" here means concurrent stream PROVISIONING with nothing deleting** —
    `mqttPreflightBrokerStreamBundle` reads `STREAM.INFO` and `ensureBrokerStream`
    create-or-loses on `STREAM.CREATE` — and it is NOT the per-subject message CAS. F-110
    faults the zero-expectation MESSAGE admission and does not reach either operation, so
    it leaves this claim standing. Concurrent DELETION
    can defeat it: an operator deleting streams behind the scan, leaving `$MQTT_sess`
    for last, can be observed as ten-present, after which the broker adopts the
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
  unconditionally because the floor (2.15.1) is above 2.15.0: every supported peer has the
  option.
  Tracked as F-095, whose decision (2026-10-12) is to keep this a deployment
  prerequisite: the policy choice is closed, the limitation is not, and the broker still
  cannot validate the server setting through its own API access.

- **This project's own external conformance environments now meet both prerequisites,
  and assert them.** All three launches — the two CI conformance jobs
  (`.github/workflows/ci.yml`) and `test/conformance/docker-compose.yml` — run
  `synadia/nats-server:2.15.1-scratch` *by digest*, at the floor, with
  `default_max_consumers: -1`. The digest rather than a tag is deliberate and stays: a
  floating tag moves the backing server with no repository change and no paired
  configuration edit, which is how two environments come to disagree about the server they
  ran.

  **The image is the upstream publisher's, not the Docker Official Image.** When the floor
  moved to 2.15.1 the Docker Official `nats` repository had no 2.15.1 tag, and GitHub's
  container registry carries no public nats-server image. `synadia/nats-server` on Docker
  Hub is where nats-io/nats-docker publishes each release; its image configuration
  (entrypoint `/nats-server`, ports 4222/6222/8222) matches the official image's, and
  `run.sh` reads the version from `/varz`, never from the image tag.

  **The three did not share one remedy.** The Paho job and the Compose environment mount
  `test/conformance/nats.conf` and read the limit from it. The Rust conformance job
  previously ran `-js` with no configuration file at all, so it gained its own
  `test/conformance/nats-plain.conf` — unauthenticated, like the launch it replaces,
  because that job's broker runs without auth passthrough and the Paho environment's ACLs
  would break it.

  **An absent limit is indistinguishable from an armed cap, so the check is on the
  effective value, not on the version.** `test/conformance/run.sh` reads `/varz` before
  any suite runs and fails unless the server is at or above the floor **and** reports
  `default_max_consumers: -1`; an unreachable or unresponsive monitoring endpoint is also
  a failure, within a bounded window, because a skipped check reports green from a capped
  server and a hanging one never reports at all. Measured against the 2.15.0 GA binary
  and again against the 2.15.1 image: `-js` with no config file reports `1000`, a config
  with no `limits` block reports `1000`, and only the explicit `-1` reports `-1`. There is
  no opt-out.

  The check **decodes** `/varz` rather than pattern-matching it, and that is a correctness
  requirement rather than a style choice: a text match accepted `-1.5` as `-1`, accepted a
  duplicate key whose second value was malformed, and accepted a duplicate key written
  with a `\u` escape so that it did not match the literal text but decoded to the same
  name. Decoding rejects a truncated or ambiguous document
  outright and reads both fields at their exact paths. It costs a `python3` dependency,
  which is why the Rust conformance job now sets Python up even though it runs no Python
  suite. The floor also requires a bare `X.Y.Z`: prereleases are refused, because
  `sort -V` orders `2.15.0-RC.1` *above* `2.15.0` and a range test alone would admit
  precisely the population a GA floor excludes.

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
  compatibility boundary — all ten broker-owned streams are one recreation unit, so
  recreating the offending stream alone is refused. The support floor is nats-server
  2.15.1, which always understands `AllowMsgTTL` — as did every release back to 2.14.5,
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
**either** path. The live SUBSCRIBE's `!exists` create produces the same state as a
resume's repair. Every available deletion is refuted: the acquisition the create
materialized is the RECORD's, and after a takeover the new owner's record names it, so
releasing it here would retire an incarnation the new owner holds. Leaving it is the least
unsafe option and it is not inert — it holds stream interest and it can accumulate pending
messages until the new owner's connection consumes them or the group's last acquisition is
released, at which point the group record retires the incarnation and the orphan sweep or
the leaver deletes it; a later joiner mounts a FRESH incarnation and never attaches to it.

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
  is not bounded.** `writeActionlessFrameBy` reaches `writeSocket`, which installs
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
sweep, and the shared record validator. The same refusal meets a record whose `shared_subs`
values are the pre-transfer numeric QoS rather than a membership carrying acquisition
references (the shared-group lifecycle transfer, 2026-09-22). The one path that decides by
**absence of a reference** across the whole record set refuses in the way that withholds
its destructive act, because it may not act on a partial view: the orphan-consumer sweep
**aborts its whole pass**. (The last-holder scan, which used to be the second such path, no
longer exists.) The recovery is the compatibility boundary's: stop the account's brokers
and reset the bridge-owned `$MQTT_*` streams — all ten, which are one recreation unit.
This is not a rolling upgrade.

### The retained index is ordered by stream sequence, and a lost application is repaired

Retained state is durably arbitrated by `$MQTT_rmsgs` (one record per topic, `MaxMsgsPer:
1`, an empty body is the deletion). What every broker SERVES from is a volatile index —
`as.retmsgs` plus the `rmsCache` copy — fed by the retained observer (a per-broker ephemeral
`AckNone` consumer), the publishing broker's local seed and SUBSCRIBE-time authoritative
loads. [F-059](findings/runtime-correctness.md#f-059) orders all of them by stream sequence
and repairs a lost observer application by resuming the observer from the last sequence it
committed; see "The retained index is ordered by stream sequence" in `docs/ARCHITECTURE.md`
for the mechanism. What that does and does not guarantee:

- **An absent entry is repaired, within a bound.** A subject missing from the index because
  an observer delivery was LOST (a slow-subscription drop in nats.go, a reconnect, a
  consumer reaped by `InactiveThreshold` or deleted out of band) is redelivered once the
  loss is detected: at the next delivery for a mid-stream loss, at the next idle heartbeat
  (5 s) for a lost last delivery, after three missed heartbeats plus one reset attempt when
  the consumer is gone, and on the next eligible heartbeat tick after a reset attempt that
  could not complete (an ambiguous delete or a failed create). Until then that broker does
  not replay the subject to a wildcard filter; a literal filter's authoritative load indexes
  it at once. A broker whose JetStream stays unreachable does not converge until it is
  reachable again.
- **Reordering no longer restores, removes or evicts.** A delayed older seed, observer
  application, load result or undecodable record is refused against what the index holds
  and against the observer's watermark; a deletion seen ahead of the observer leaves a
  placeholder that refuses older values until the observer passes it.
- **A superseded value can still be served from the cache, bounded by the cache TTL
  (`mqttRetainedCacheTTL`, 2 minutes), in three interleavings:**
  1. *Between a loss and its detection.* A lost replacement's predecessor stays cached and
     served until the bound above, after which the reset flushes the cache.
  2. *A load in flight across a reset.* A load that read a record before the reset can
     hydrate the cache with it after the flush. It was current when read; it is superseded
     only if a replacement landed, was lost and expired inside that read's round trip.
  3. *A load stalled across an expiry.* A load reads record r for a subject indexed at c,
     with r past c and W below c; a later replacement lands, is lost and TTL-expires; a
     subscription's no-message answer turns the entry at c into a placeholder; the stalled
     load then applies and caches r. No reset is involved.
  Each ends when the cache entry ages out and the next matching SUBSCRIBE reads the stream.
  Closing them would need a fence on cache writes for windows inside one read's round trip
  in already-rare cases, so they are accepted.
- **Index memory for an expired record is reclaimed lazily**, as before: the entry stays
  until a matching SUBSCRIBE's load finds no message, and serving refuses it past its
  expiry meanwhile. JetStream subject delete markers would reclaim it earlier and are
  declined (they require `AllowRollup`; see the architecture section). A placeholder is
  volatile memory bounded by deletions seen ahead of the observer, and is pruned once the
  observer passes it.
- **During a reset** the index keeps serving what it holds; nothing is cleared except the
  cache.
- **A sourced `$MQTT_rmsgs` topology is untested (accepted coverage gap, 2026-10-12).**
  `docs/ARCHITECTURE.md` supports a cross-account sourced retained stream, but no
  executing native test stands one up: the only test of it is in the unbuilt
  `mqttport_clustertests` reference file. A record arriving through a source with a
  subject transform, its TTL expiry, and delete-marker propagation through the source are
  therefore not exercised. Arbitration and resume use destination sequences and apply
  unchanged there, but neither can recover an update lost before ingestion into the
  destination and expired upstream. No convergence defect has been reproduced there; this
  states the gap.

Supported current-format retained behavior is otherwise unchanged. Records the
broker cannot decode now fail closed everywhere — warned with the subject and the
error, and evicted rather than silently ignored — where the development-era JSON
fallback used to accept some of them; a record carrying a valid `Nmqtt-RFlags`
header from a sourced foreign account is still accepted, and malformed *optional*
headers (properties, expiry) are still ignored rather than rejected. Legacy
development records (a JSON body, or a `-1` marker tombstone) and mixed old/new
development fleets deliberately change behavior per the compatibility boundary and
are not supported topologies.

### A retained update the store did not confirm closes the publisher's connection

F-060. A publication whose retained update the store does not confirm is never
given final success: QoS 0 closes the connection, QoS 1 withholds PUBACK and
closes, QoS 2 withholds PUBCOMP and closes, and a delayed Will is left to be
re-fired (`docs/ARCHITECTURE.md`, "A retained update the store did not confirm").
What that contract does not deliver:

- **Recovery needs a surviving session and a client that re-sends.** Closing
  enables a re-send; it does not cause one. A 3.1.1 CleanSession=1 or v5 Session
  Expiry 0 publisher, or one that gives up, leaves the previous retained state in
  place: an old value still served, or a deletion that did not happen.
- **The publisher learns little.** The DISCONNECT (0x80, v5 only) is best effort,
  and a QoS 0 failure carries no packet identifier, so the publisher cannot tell
  which publication failed. PUBREL has no DUP flag.
- **Every re-attempt repeats the live fan-out.** The core-NATS fan-out precedes the
  retained store and cannot be retracted, so QoS 0 and plain NATS subscribers see a
  re-sent publication again. For QoS 1 the stream-backed copy can be delivered twice
  as well; for QoS 2 it is deduplicated only within the Nats-Msg-Id window (see
  "Inbound QoS 2 PUBREL is at-least-once across the unresolved move interval").
- **An ambiguous store is re-stored, and can overwrite a newer value.** A write that
  landed with its reply lost is unconfirmed to the broker, so the publisher is not
  acknowledged and re-sends it at a newer sequence. If another publisher stored a
  value in between, the re-send replaces it. The same holds for a concurrent peer
  QoS 2 releaser that resumes after another broker completed the exchange (the
  release claim is discard-exclusive, not releaser-exclusive), and for a stale
  delayed-Will worker that resumes after its lease was reclaimed. Stream-sequence
  order (F-059) orders applications; it does not preserve the newer publisher's
  value. Pre-existing for the last two.
- **A delayed retained Will re-fires every lease period** while its retained store
  keeps failing, each time with a duplicate live fan-out, and a persistent QoS 1/2
  publisher whose retained store is permanently refused reconnects and re-sends
  indefinitely. A permanently failing `$MQTT_msgs` store already behaves the same
  way.
- **An immediate Will's retained failure is logged and lost**, as any immediate-Will
  failure after its fan-out already is.

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
   gives up a shared delivery proven never transmitted (its exchange withdrawn) without
   acknowledging the group message and without destroying the group durable, so the group
   may re-offer it. A possibly-transmitted shared QoS 1 delivery keeps its identifier and
   custody like an ordinary obligation. Shared QoS 2 keeps its existing affinity rules.
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
   elapsed-time bound: since 2026-10-12 later SUBSCRIBEs cannot extend that wait, but it
   lasts as long as the slowest SUBSCRIBE in flight at its sample. Both are stated in
   `docs/ARCHITECTURE.md`.
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

### A connected packet's claimed length is bounded at its fixed header

A connected client's claimed Remaining Length is judged before its body is buffered
(F-120; see `docs/ARCHITECTURE.md`, "An inbound packet's length is bounded before it is
buffered"). A v5 client is held to the Maximum Packet Size its CONNACK advertised, as
before. What this states for MQTT 3.1.1, which has no Maximum Packet Size:

1. **A 3.1.1 packet over max_payload + 65,544 bytes closes the connection** at its fixed
   header, with no reason (3.1.1 has none to send). No PUBLISH whose payload fits
   max_payload reaches that size. What it does refuse that used to be served: a SUBSCRIBE
   or UNSUBSCRIBE whose total wire size exceeds it (default max_payload 1 MiB, so
   roughly 1.06 MiB of filters), and a PUBLISH to the reserved `$MQTT/` namespace, which
   used to be discarded with the connection kept (an ordinary PUBACK/PUBREC for QoS 1/2)
   because that branch returns before the payload check. A reserved topic earns no
   exemption from the bound.
2. **A fixed-length packet type claiming the wrong length** (a PINGREQ with a body, a
   3.1.1 PUBACK that is not 2 bytes) is refused at its fixed header rather than after
   the claimed body arrives. The disposition is unchanged, only earlier.
3. **The bound is per connection, not a memory budget.** A client that trickles a packet
   within its bound still makes the broker hold up to that bound until the packet
   completes or a liveness bound closes the connection; Keep Alive 0 leaves only the
   bounds stated in the previous entry. A max_payload near or above the variable byte
   maximum (about 256 MiB) leaves that maximum as the effective bound, for either
   protocol level.

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
**It no longer does, and the enforcement is LANDED**: the all-ten recreation contract is
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

**And the wedge used to reach the shared group durables too**, because their release ran
after `deleteTeardownTargets`. Since the shared-group lifecycle transfer the shared closing
(seal the generation's acquisition ledger, release every listed acquisition, gate on the
empty seal) runs on the record-backed routes BEFORE `deleteTeardownTargets`, right after
`recordGenRetirement`, so a refused register reap no longer withholds a shared release; a
refusal inside the shared closing withholds the record's release through its own gate
instead, and the adoption repeats the closing from the ledger.

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
| the shared group durables | already released by the shared closing, which precedes this on the record-backed routes; on the no-record branch there is no generation to seal |

**What can be retried, and by what.** This is where the drafts went wrong twice:

| Call site | Marker/record after a refused reap | Adoption retry possible? |
|---|---|---|
| `mqtt.go:9706`, no record (`seq == 0`) | none exists — `releaseTeardown` is not part of this branch at all | **No.** Nothing anchors a retry; the orphan sweep is the only path |
| `mqtt.go:9794`, ordinary discard | record and marker stand, since `releaseTeardown` was never reached | Yes |
| `sess_teardown.go:1052`, adopted teardown | as above | Yes |

**And a successful attempt is now ordered so a crash leaves work, not an orphan.** The
order is seal → release every listed acquisition → gate on the empty seal → `releaseTeardown`
deletes the record. A crash anywhere before the record delete leaves the record MARKED and
the ledger sealed, so the adoption repeats the release from the ledger; a crash between an
acquisition's release and its incarnation's consumer delete leaves a `retiring` entry in the
group record that the orphan sweep's retirement pass finishes. (An earlier passage here
described the record being removed BEFORE the shared release and a group durable surviving
"until a member rejoins that `$share` group and adopts it"; that ordering and that
disposition are gone — a later joiner mounts a fresh incarnation and never adopts a retired
one.) A durable another member still holds is retained by the group record's count, never
by a scan.

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

### A group durable's release is arbitrated by its group record, and what that leaves

Until 2026-09-22 five release paths deleted a group consumer on the answer of a walk over
every session record (`sharedDurablesHeldByOthers`), and this section recorded what that
walk cost: a retained durable nothing reclaimed while any record was unreadable,
retention unbounded in count, a same-client-ID skip that made the new owner's record
invisible after a takeover, and a join/delete race the walk could not close because a
read is not an arbitration. The shared-group lifecycle transfer replaced the walk with a
group record per logical group (physical incarnation, holder count, retiring
incarnations), a per-generation acquisition ledger the teardown seals, and release
verdicts in the session record executed against those two — `docs/ARCHITECTURE.md`, "A
group durable is held by arbitration, not counted by a scan". **Every residual of the
walk is CLOSED with it**: no record's readability spares another group's durable; the
same-client-ID question does not arise, because a release removes exactly the acquisition
its verdict names rather than deciding about other sessions; and the join/delete race is
closed by naming — a delete is issued only for an incarnation the record has moved into
`retiring`, and a join names the incarnation it CASes the record against, so a late
delete can never reach a successor. F-082's separation of the local release from the
consumer's fate stands (a refusal never strands this session's own state), and F-083's
surviving-membership property still decides the LOCAL half; neither decides an
acquisition, which is released unconditionally.

**What the mechanism leaves, stated.**

1. **A delayed `CONSUMER.CREATE` for a retired incarnation** — a create issued before the
   incarnation retired and delivered after — exists until the orphan sweep's grace window,
   when the sweep derives its retirement from the group record and deletes it. No member
   attaches to it: a later joiner mounts a fresh incarnation with its own name.
2. **A `retiring` entry whose consumer delete was ambiguous waits for the sweep.** The
   leaver stops at the ambiguity and leaves the entry standing; the sweep's retirement pass
   repeats the delete (a consumer already gone classifies as gone) and clears the entry.
3. **A release verdict left by a crash between its record write and its execution is
   executed by the next resume of that session, or by the orphan sweep's verdict pass once
   the same verdicts have been observed on two consecutive passes** —
   independent of the consumer's age and of the record's connection status, since a hard
   crash leaves `DisconnectedAt == 0` on a record nothing else revisits when the session
   never expires. During that window the consumer stays alive and holds interest; this is
   the same order of window as the orphaned-consumer residual and no larger than the
   pre-transfer one, where the crash left a committed membership nothing released.
4. **ONE client-visible refusal.** A join that must MOUNT a new incarnation while its
   group's `retiring` list is full (sixteen entries) first finishes the entries it can
   inline and otherwise fails that filter with a retryable reason. It is reachable only
   after sixteen consecutive ambiguous retirements of one group's incarnations, each left
   by a leaver whose delete was ambiguous and not yet finished by the sweep.
5. **A group durable is withheld where no readable, valid lifecycle authority licenses its
   deletion.** The orphan sweep deletes a group consumer only under its group record
   (`sweepSharedConsumers`, `shared_lifecycle_sweep.go`): it finishes the retirement of an
   incarnation the record reads `retiring`, and deletes one the record derives retired (a
   delayed create that landed after retirement). Everything else is retained: the live
   incarnation, an ordinal the record never issued, a group record that is absent or will
   not read or validate, and the pre-transfer name with no incarnation (an unsupported
   persisted shape, refused and reported). Such a durable and the interest it retains stay
   until its record licenses a deletion or an operator removes it.

**Storage accepted, stated.** One group record per logical group ever created in the
recovery epoch, and one acquisition ledger per session generation that ever acquires, closes
or is accepted — every teardown writes an empty seal and every acceptance installs its
tenure, a fresh generation included —
neither deleted within the epoch, for the reason the authority's closures and outcomes are
not: an expiry would rebuild the inference from absence the mechanism removes. And one
`$MQTT_shared` write on the critical path of every accepted CONNECT.

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
7. **Restated for the ordinal rule:** a malformed shared membership key names no
   acquisition, so it protects nothing and releases nothing; the orphaned-consumer sweep no
   longer reads memberships at all for shared consumers — it classifies each by its ordinal
   against the group record — so the consumer such a key was meant to hold is retained while
   its incarnation is live and deleted once the group record retires it. The pre-CONNACK
   preflight refuses the record carrying the malformed key, which makes the condition visible
   to an operator rather than served.

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
only by "the next resume that completes its scans and finds nothing unaccountable". While
allocation is withheld, an exchange already bound to its acknowledgement still completes
and releases its custody (F-097).

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
  sufficient: a later resume still knows not to treat the identifier as an owed delivery,
  while the allocation's unsent marker stands and no output token is outstanding on the
  register (F-088: otherwise the register is possibly attempted, and its restoration
  retires the verdict as superseded). If neither rung lands, the identifier stays
  reserved for the life of the session, which is the pre-existing cost of an unproven
  cancellation. The same holds for a record that is present but UNREADABLE, and for the
  same reason — a read that failed licenses no conclusion that the register is gone.
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

### A possibly-transmitted PUBLISH without a deliverable source holds its identifier

F-098. If a QoS 1 or QoS 2 PUBLISH remains nonterminal after its action drains, and its
source can no longer be delivered, the broker retains its durable slot, packet identifier
and Receive Maximum capacity. Any outstanding drain release request and dependent claim
remain durable; their dependent acknowledgement and drain retirement are withheld. A valid
identity-bound acknowledgement can advance the exchange; otherwise confirmed generation
discard ends the hold. Reconnect alone does not guarantee progress.

This is not a new hold — the broker already reserved this shape and said so under the
oversize-refusal cost above — but the drain release request and the claim now held with it
are new, and the withheld acknowledgement is what keeps the two consistent across a crash.
The request's settlement is attempted at every resume, not only on a source delivery, so
what remains held is a register whose release is genuinely refused rather than one nothing
is trying to release.

A supported `publish` retirement carrying contradictory cancellation evidence stays
reserved on the same terms. **`Retired`, or source completion, is never promoted to
terminal authority merely to end that hold**: retirement is backed by the client's own
PUBREC, so the transmission provably happened, and freeing the identifier would be exactly
the premature release the whole item exists to prevent.

## Recovery reservation lifecycle (F-111)

A complete successful resume transfers absent recovery reservations to
identifier exclusion before removing their scheduling charge. Incomplete
evidence, conflicting identity, or an active send retains the reservation.
A local successful read alone does not authorize absence release.
