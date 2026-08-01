# Architecture

```text
        MQTT clients (3.1.1 / 5.0)
     │        │        │        │
     ▼        ▼        ▼        ▼
┌──────────┐      ┌──────────┐        (stateless frontends,
│ bridge #1│ ...  │ bridge #N│         scale horizontally)
└────┬─────┘      └────┬─────┘
     │  nats.go (service conn + per-client auth-passthrough conns)
     ▼                 ▼
┌─────────────────────────────────────┐
│      NATS / JetStream cluster        │   (replicated → HA state:
│  sessions · retained · QoS1/2 · pub  │    sessions, retained msgs,
└─────────────────────────────────────┘    QoS state, message routing)
```

## Why standalone

NATS server ships MQTT **3.1.1** embedded. The **MQTT 5.0** work this project is
built on will not be merged upstream, so instead of maintaining a fork of the
whole server, the MQTT layer runs as a separate service that uses an external
NATS/JetStream cluster as its backend.

## Compatibility boundary

The bridge is **pre-release** and is being designed for its first public
release. Its internal persisted MQTT state and `$MQTT_*` JetStream stream
schemas do not have a backward-compatibility contract yet. During this phase,
schema changes may require development streams to be recreated; designs should
not carry migration, legacy-format, or mixed-version rolling-upgrade machinery
unless a concrete released contract is identified first.

The backend support target is the **latest JetStream** represented by the
current `nats-server/v2` dependency in `go.mod` and exercised in CI. The bridge
does not maintain alternate code paths for older JetStream APIs or semantics.
When the dependency advances, plans may adopt the new current behavior directly.

The floor is **nats-server 2.14**. Older releases are unsupported outright: there
are no fallbacks, and where a behavior below is load-bearing it is validated at
startup and failed closed rather than degraded.

This boundary applies only to bridge persistence and its JetStream backend. MQTT
3.1.1 and 5.0 wire-protocol compatibility remains a product requirement and is
covered by the conformance suites.

### JetStream behaviors this design depends on

Correctness rests on these specific semantics, each stated as the contract the
code assumes. This is the list a version bump has to re-verify — it is what makes
the "no fallbacks" stance reviewable rather than implicit.

- **Per-subject CAS** (`Nats-Expected-Last-Subject-Sequence`). A store carrying it
  is accepted only if the subject's last sequence matches, giving every state
  machine its linearization point. A rejection is reported structurally
  (`JSStreamWrongLastSequenceErrF` 10071, or 10164 under clustered inflight
  contention). 10164 means *transient contention*, not "the subject moved", so
  every rejection is resolved by re-reading rather than classified directly.
- **`MaxMsgsPer: 1`** turns a subject into a single-slot register: a new record
  evicts the old one, so a stream sequence names a record, never a subject.
- **Interest retention.** A store with no matching consumer interest **succeeds and
  keeps nothing** — the server skips the message, still records its dedupe entry,
  and returns an *ordinary* success acknowledgment carrying a sequence that holds
  nothing. A publish response therefore never proves a record exists; only delivery
  or a presence check at the returned sequence does. Correspondingly, deleting a
  consumer deletes the messages it still held, so "absent" never proves
  "delivered".
- **Deduplication entries outlive their messages.** `deleteMsg` does not clear a
  `Nats-Msg-Id` entry, so re-storing under the same id inside the window can be
  accepted as a duplicate that stores nothing. Anything that must exist is verified
  by presence, and identities that need to be re-attempted carry an attempt
  counter.
- **`PubAck.Duplicate`** flags a deduplication hit. Useful diagnostically; it does
  not remove the need for the presence check, because a no-interest skip is an
  unflagged success.
- **Per-message TTL** and **JetStream double ack** (`AckPolicy` explicit with a
  confirmed acknowledgment), used where a cleanup must be known to have landed.
- **Consumer inactivity limits.** `InactiveThreshold` gives a durable a delete
  timer; a stream-level `ConsumerLimits.InactiveThreshold` imposes one that a
  consumer cannot opt out of. Where a durable must survive an offline session, the
  stream is checked and the feature is refused rather than silently reaped.

## High availability

- The bridge is **stateless**: all durable state (sessions, retained messages,
  QoS1/2 in-flight, subscriptions' persistence) lives in JetStream.
- Run **N** bridge instances behind a TCP load balancer. A client can reconnect
  to any instance; **session takeover** works across instances because it is
  mediated by a NATS-message mechanism, not in-memory state.
- HA/durability guarantees come from JetStream **replicas** (`R3`/`R5`). Size the
  NATS cluster, not the bridge.

### Two distinct handoff mechanisms

These are independent and easy to conflate:

- **Bridge-replica handoff** — a *bridge* instance dies; another bridge picks up
  the work. Any bridge can serve any client and claim any durable task because all
  state is in JetStream. This works even against a single-node JetStream.
- **JetStream R3 leader failover** — a *NATS* node dies. The stream's Raft group
  elects a new leader among the surviving replicas and continues committing, as
  long as quorum survives. This is a property of the NATS cluster, not the bridge.

The **delayed-Will outbox** (`$MQTT_willq`, MQTT 5.0 Will Delay with a surviving
session) exercises both: a Will enqueued on one bridge is fired by a surviving
bridge (bridge-replica handoff), and its claim/publish/delete lifecycle keeps
committing across a NATS-node loss (R3 leader failover). The guarantee is
**at-least-once** *once the outbox record is durably acknowledged and while quorum
and storage remain available*: per-subject JetStream CAS serialises each state
transition, so a recovered duplicate is possible. It is not unconditional no-loss —
QoS 0 is best-effort, a sustained JetStream outage can defeat the immediate-publish
fallback, and a forced process kill can interrupt persistence before the record is
acknowledged. Both mechanisms are
regression-tested under the `mqttport_fulltests` tag — the shared-store simulation
in `will_outbox_test.go` and the real 3-node cluster in
`jetstream_cluster_harness_test.go` (see [CONTRIBUTING.md](../CONTRIBUTING.md)).

Two refinements keep the guarantee tight:

- **Durable termination.** A successful publish is committed `publishing -> done`
  (retried against the record's latest sequence) *before* any cleanup; only then is
  the record deleted (at the `done` sequence). Committing `done` first means a failed
  delete cannot leave a re-fireable `publishing` record — the scanner reaps the
  `done` record instead.
- **Takeover fence (durable cancellation watermark).** The primary suppression is a
  permanent per-client marker on the dedicated `$MQTT_willcancel` stream
  (`MaxMsgsPer:1`, no age/size limits). Every accepted CONNECT advances the client's
  marker to its session save sequence — a *gating* write: no success CONNACK is sent
  until it lands (and a failed advance runs a rollback transaction that rejects the
  CONNECT). Sequences are comparable only within one `$MQTT_sess` incarnation,
  identified by a generated epoch (`nuid:<NUID>`) stored in the session-stream
  metadata and carried on both the marker (`SessEpoch`) and each Will
  (`OriginSessionSeq`, `OriginSessEpoch`). The claimant fence suppresses a Will when a
  current-epoch marker is *strictly beyond* its origin sequence
  (`SessionSeq > OriginSessionSeq` — the connection's own delayed Will, at an equal or
  higher origin, still fires), or when the marker is from a different incarnation.
  Because the marker is permanent, this holds even after the newer connection
  finite-disconnects or its session record is deleted — the residual a session-record
  fence alone could not close. When no marker is present (administrative loss), the
  fence falls back to the numeric session-record rule (suppress iff a newer record
  with `DisconnectedAt == 0`), and a marker-stream outage defers pending Wills (never
  lost) until it recovers.

Residual that keeps literal exactly-once impossible:

- *At-least-once duplicate* — a crash between the external publish and the `done`
  commit republishes after the lease expires (the sanctioned duplicate).

The reconnect-suppression miss is now closed by the durable watermark; it reopens
only under administrative loss of `$MQTT_willcancel` (a documented degradation, not a
supported migration). This is a first-release storage format: pre-Layer-2 in-record
Wills, migrated outbox records, and a `$MQTT_sess` stream without a valid generated
epoch are unsupported and rejected with recreate guidance rather than carried forward.

Operational constraints for the marker:

- **Back up, restore, delete, or recreate `$MQTT_sess` and `$MQTT_willcancel` as one
  logical unit.** Recreating only `$MQTT_willcancel` loses suppression evidence and
  reopens the residual until each affected client reconnects.
- **Recreating `$MQTT_sess` requires restarting every bridge serving the account
  before MQTT traffic resumes.** The incarnation epoch is captured once per account
  manager (`as.sessStreamEpoch`); a live manager that outlives stream recreation
  retains the stale epoch and stale session-CAS state. A process restart recreates the
  manager and adopts the new epoch.
- **Retention is intentionally unbounded**: one permanent marker per distinct client
  ID ever seen (no `MaxAge`). Monitor `$MQTT_willcancel` message count and storage;
  administrative pruning weakens the guarantee for the pruned client IDs.

## Reserved `$MQTT.>` namespace

MQTT topics map to NATS subjects by swapping the separators: `/` becomes `.`, so
`foo/bar` is published as `foo.bar` and read back as `foo/bar`. MQTT wildcards
map the same way (`+` to `*`, `#` to `>`).

The broker reserves `$MQTT.>` for its own state, and that state moves as ordinary
NATS traffic on those subjects:

| Subject | Contents |
|---|---|
| `$MQTT.msgs.<subject>` | every QoS 1/2 message (the `$MQTT_msgs` stream) |
| `$MQTT.rmsgs.<subject>` | every retained message |
| `$MQTT.sess.<domain>.<hash>` | session records |
| `$MQTT.JSA.<...>` | JetStream API replies and session-persist broadcasts |
| `$MQTT.willq.*`, `$MQTT.willcancel.*` | delayed-Will outbox and cancel watermarks |
| `$MQTT.qos2.in.<hash>.<gen>.<pi>[.rel]`, `$MQTT.out.*` | inbound QoS 2 move state and its release claim, outbound PUBREL state, and session-affine shared QoS 2 copies |
| `$MQTT.qos2.assign.<group-durable>.<seq>` | shared-subscription QoS 2 assignment claims |
| `$MQTT.inflight.<hash>.<gen>.<pi>` | outbound in-flight registers: the durable identity of a started QoS 1/2 delivery |
| `$MQTT.sub.*`, `$MQTT.deliver.pubrel.*`, `$MQTT.deliver.q2.*`, `$MQTT.qsub.*` | per-client delivery subjects |

Anything able to read `$MQTT.msgs.>` reads every QoS 1/2 message in the account,
so the namespace is closed on every path a client can reach:

- **PUBLISH** into it is always dropped. MQTT 5.0 QoS 1/2 gets reason `0x87`
  (Not authorized) in the PUBACK/PUBREC. **MQTT 3.1.1 QoS 1/2 gets an ordinary
  success PUBACK/PUBREC**, because 3.1.1 has no reason codes -- the message is
  still discarded, and the broker logs a warning, but a 3.1.1 publisher cannot
  tell. QoS 0 is dropped silently in both versions.
- **SUBSCRIBE** to it is rejected with SUBACK `0x80` in both versions (5.0 also
  carries a reason string). This covers the whole prefix, not just the delivery
  subjects: a wildcard filter such as `#` already cannot match a `$`-topic
  ([MQTT-4.7.2-1]), but an explicit `$MQTT/msgs/#` filter is not "reserved" by
  that rule and would otherwise be honoured. A legacy subscription found in a
  resumed session record is dropped from the record, not merely ignored.
- **Will topics** in the namespace are rejected at CONNECT: `0x05` in 3.1.1,
  `0x87` in 5.0. A Will never passes through the PUBLISH path, and a *delayed*
  Will is replayed from the outbox over the service connection where the client's
  own NATS permissions no longer apply, so it is refused up front.

### Recommended NATS permissions

Deny both directions on the whole prefix for the identity MQTT clients
authenticate as:

```text
publish:   { allow: [">"], deny: ["$MQTT.>"] }
subscribe: { allow: [">"], deny: ["$MQTT.>"] }
```

Both layers are required and neither is redundant. The broker's own checks bind
anything speaking MQTT; the ACL is what binds someone who takes the same
credentials and connects to NATS directly. Under auth passthrough an MQTT client
holds real NATS credentials, so that is not a hypothetical.

This deny is safe because the broker creates its own delivery subscriptions
(`$MQTT.sub.*`, `$MQTT.deliver.pubrel.*`, `$MQTT.qsub.*`) on the **service**
connection rather than on the per-client one. If they were on the client
connection this ACL would break QoS 2 delivery and retained replay, and NATS
evaluates deny before allow, so the three could not be carved back out. The
conformance environment (`test/conformance/nats.conf`) ships this exact ACL, so
the suite passing is what keeps that separation honest.

## Inbound QoS 2 move protocol

An inbound QoS 2 PUBLISH is **held** on `$MQTT_qos2in` under
`$MQTT.qos2.in.<client-id-hash>.<generation>.<packet-identifier>` and **moved**
onto `$MQTT_msgs` when the client sends PUBREL. The move is recoverable rather
than destructive: the message is delivered onward **first**, and only then is the
hold record replaced. Implementation and the design invariants it obeys are in
`internal/mqtt/qos2_move.go`.

One subject carries two record states, and it is reused by every exchange that uses
the same packet identifier within one incarnation — a client is free to reuse an
identifier as soon as its exchange completes:

| state | shape | written by |
|---|---|---|
| `held` | the encoded PUBLISH plus `Nmqtt-Q2Gen: <22-char NUID>` | a **create-only** CAS (expected last subject sequence 0) |
| `delivered` | empty body, the same `Nmqtt-Q2Gen` plus `Nmqtt-Q2Done: 1` | a CAS at the held record's **exact** sequence |

`Nmqtt-Q2Gen` is the exchange's immutable identity, stamped once at PUBLISH time
and never rewritten. Because subjects are reused, a stream sequence is **not** an
identity: the finalizer compares generations and abandons without mutating anything
when it finds a foreign one, so it can never replace a **later exchange's**
undelivered payload with its own tombstone. The same comparison is what makes the
release claim's post-claim re-read safe (below). A record whose generation is
missing, malformed, or declared more than once is classified `corrupt` — never
`held` — and is disposed of rather than mistaken for a duplicate.

The subject also carries the **session incarnation generation**, so it is never
reused across incarnations and a discard names each hold subject exactly. The two
generations answer different questions and both are needed: the incarnation scopes
the subject, the exchange identity scopes the record on it.

The terminal tombstone is what makes a PUBREL retry safe indefinitely: a retry
that finds it is answered `0x00` with no second delivery, independently of any
deduplication window. The `Nats-Msg-Id` carried on the onward store (derived from
the generation, so it is identical across every retry of one exchange) only
narrows the window in which a retry duplicates the *stored* copy; correctness does
not depend on it. See `docs/CONFORMANCE.md` for the exact guarantee and its
residual deviations.

### The release claim

Delivering onward first makes the move recoverable, but it also means the hold
record's arbitration point comes *after* the irreversible act. A session discard
used to purge these subjects by filter, which competes with nothing: a PUBREL that
had merely *read* a held record went on to fan out, and its finalizer then read the
resulting absence as proof the exchange had completed. So a **release claim**
(`internal/mqtt/qos2_release_claim.go`) sits in front of the delivery — a small
create-only CAS record on a sibling subject, the hold subject plus a `rel` token:

| writer | value | when |
|---|---|---|
| the releasing PUBREL | `releasing` + the exchange generation | before the onward store |
| a session discard | `discarded` | before it destroys the hold |

Exactly one create can succeed, so exactly one of "release it" and "discard it"
wins and the loser does nothing irreversible. The claim is **discard-exclusive, not
releaser-exclusive**: the generation identifies the exchange rather than the broker,
so a second releaser of the same exchange reads its own generation back and
proceeds. Because the claim is a sibling record, the hold record is never
rewritten — its payload, properties and store time (which ages the Message Expiry
Interval) stay as the PUBLISH left them — and a discard can enumerate a session's
inbound QoS 2 state through `STREAM.INFO` without reading a single payload.

The claim is paired with an authoritative **re-read** of the hold record, and
neither replaces the other: the claim fences a *discard*, which a re-read cannot,
because read-then-fan-out is check-then-act; the re-read fences an
*already-completed* exchange, which the claim cannot, because a peer that committed
and reaped leaves nothing to contend with, so a stalled releaser's create-only claim
would simply succeed. A discard that finds a `releasing` claim waits briefly and
then leaves both records rather than destroying a payload a crashed releaser's retry
still needs; on the same broker an in-process fence refuses to admit a release at
all. A `discarded` marker is never reaped by a releaser or by the discard itself —
it is the only durable proof that a late releaser must lose — and is left to be
aged out.

`$MQTT_qos2in` must be `LimitsPolicy` + `DiscardOld` + `MaxMsgsPer: 1` (`DiscardOld`
is what makes the terminal CAS a *replacement*; `MaxMsgsPer: 1` is what stops it
appending and leaving a stale `held` record underneath), and must not withdraw the
write, CAS, subject, acknowledgement, retention, deletion or purge semantics the
protocol depends on. The claim adds no requirement of its own: it lives inside the
stream's existing subject space and needs the same single-slot CAS semantics the
hold record already does. A stream that does not match **fails broker startup** with
recreate guidance rather than being updated in place — updating it would be
actively unsafe, since a broker still running the pre-move code would silently
overwrite held records once the discard policy flipped. Upgrading a development
store therefore means stopping **all** brokers for the account and recreating the
stream, under the compatibility boundary above. This is not a rolling upgrade.

## Session-affine shared-subscription QoS 2

A QoS 2 message delivered to an MQTT 5.0 Shared Subscription is pending on the
queue-delivered **group** durable from the moment its PUBLISH is sent until the
client's PUBREC. Left there, a member leaving in that window makes AckWait offer
the started message to a different member — which MQTT 5.0 [4.8.2] forbids. The
broker therefore takes durable ownership of a started delivery and moves it off
the group durable entirely. Implementation and its invariants are in
`internal/mqtt/qos2_shared_affine.go`.

Two durable pieces:

| what | where | written by |
|---|---|---|
| assignment claim, one per `(group durable, source sequence)` | `$MQTT_qos2assign`, `$MQTT.qos2.assign.<group-durable>.<seq>` | a **create-only** CAS elects the winner; every later transition is a CAS at the record's exact sequence |
| affine copy of the message | `$MQTT_out`, `$MQTT.out.q2.<client-id-hash>.<generation>.<claim-hash>` | a **create-only** CAS, so exactly one of several legitimate materializers wins |

The claim is the arbitration record and it is honored **indefinitely** — not for
the lifetime of a deduplication window. `Nats-Msg-Id` on the copy store is an
optimization that narrows a duplicate-copy window; ownership never rests on it,
for the same reason the inbound move protocol does not rest on it.

**Session generation.** `<client-id-hash>` is a hash of the client ID, so a later
clean session of the same client reuses it. A random 22-character generation,
persisted with the session record, therefore scopes all three affine names — the
copy subject, the consumer durable `$MQTT_Q2AFF_<hash>_<generation>`, and its
deliver subject `$MQTT.deliver.q2.<hash>.<generation>` — so a leaked consumer from
an earlier generation can neither read a new generation's copies nor inject into
its delivery subscription. On resume the three are validated together and a
mismatch **fails closed**: the consumer that cannot be accounted for may still
hold an in-flight PUBLISH, so it is preserved for diagnosis rather than replaced.

**Claim state.** A claim carries a monotonic phase (`claimed` → `materialized`,
"does a copy provably exist?") plus three independent completion facts:
`SourceAcked` (the group delivery's ack is proven committed), `AffineDone` (the
copy reached a terminal state) and `Discarded` (the owning session ended). Every
transition preserves facts already proven, and a claim may be deleted only when
`SourceAcked ∧ (AffineDone ∨ (Discarded ∧ the copy is authoritatively absent))`.
`Discarded` deliberately does **not** imply `SourceAcked`: reaping a discarded
claim whose group ack never committed would let the source redelivery elect a
fresh winner. For the same reason the group ack is proven by a JetStream double
ack (`jsa.confirmAck`), not by the ordinary fire-and-forget `sendAck`.

A fifth field, `Lost`, records the one outcome that is neither completion nor a
permitted discard: the affine consumer was destroyed out of band, and interest
retention took its pending copies with it, so a QoS 2 message the broker accepted
was never delivered. `Lost` is terminal (no contender waits on it, and the group
delivery is released) but never reapable, because the record is the only durable
evidence that the loss happened and a clean reap would erase it. `Lost` and
`AffineDone` are mutually exclusive and are not independently merged: whichever
the durable record already proved stands.

Telling the two apart is a matter of WRITE ORDER, not of observation. `AffineDone`
is recorded **before** the copy is acked, so an absent copy under a claim that
never reached `AffineDone` was never acked by a delivery — it was dropped with its
consumer. Every broker reads that identically, which an "I saw the consumer
disappear" flag could not: the broker that observed it may be gone, or the
consumer may have been recreated, by the time anyone reconciles.

That inference is authoritative **while the owning session record still exists**.
Once it is gone, a legitimate teardown deleting the consumer and an out-of-band
destruction of it are indistinguishable — both leave a materialized claim with no
copy and no `AffineDone` — so orphan reconciliation records `Discarded` rather
than `Lost`. A `Lost` flag already committed is permanent and never reaped; what
cannot always be recovered is a loss that happened before the session record
disappeared and was never observed by the owner. See "Orphan reconciliation". The inverted
window (recorded, not yet acked) is benign and self-correcting: the copy stays
live, a live copy always keeps its claim (`reapable` requires the copy to be
authoritatively absent), and the redelivered copy is disposed of by the delivery
gate's terminal branch.

**Stream prerequisites.** `$MQTT_qos2assign` must be `LimitsPolicy` +
`DiscardOld` + `MaxMsgsPer: 1` — permanent retention is as load-bearing as the
per-subject singleton, because claims are read by direct subject lookup and are
never consumed, so interest or work-queue retention would drop them. `$MQTT_out`
must stay `InterestPolicy`: that is how a discarded session's undelivered copies
disappear with its consumer, and it is why a copy must never be stored before its
consumer exists (a store with no interest returns success and keeps nothing). Both
have validators (`validateQoS2AssignStream`, `validateOutStream`) that **fail
broker startup** rather than update a non-conforming stream in place, and the two
lists differ deliberately: each is derived from what that stream's owner actually
does.

**Mixed version.** The storage schema is rolling-compatible — an older broker
ignores the new stream and the new session-record fields. The delivery *protocol*
is not: an older broker serving the same shared group never consults
`$MQTT_qos2assign` and will redistribute a started delivery a newer broker has
claimed. Shared QoS 2 affinity holds only once every broker that can serve the
group is q2aff-aware. See `docs/CONFORMANCE.md` for this and for the
packet-identifier residual.

## UNSUBSCRIBE drain

[MQTT-3.10.4-3] requires the server to complete the delivery of any QoS 1/2
message it has already **started** sending, even after the client unsubscribes.
UNSUBSCRIBE used to tear the plain consumer down immediately, and since
`$MQTT_msgs` is interest-retained, deleting the durable deleted the messages it
still held: the started PUBLISH was simply lost.

A **drain** is what that teardown becomes when started deliveries remain
(`internal/mqtt/unsub_drain.go`). The durable and its delivery subscription
survive; already-started deliveries keep being redelivered and completed; fresh
messages on the filter are discarded. The drain is a field on the session record
(`Drains`), so it survives disconnect, restart and failover, and it lives until
every started delivery completes or the session ends. It is deliberately kept
**out** of the record's `Cons` map: an older broker restoring it there would find
`cons[sid]` on a re-SUBSCRIBE, take the early return, and never create a delivery
subscription.

The invariants, in the order they matter:

- **Persist before ack.** A delivery of a claimed sequence is acked only after a
  *confirmed* save in which the drain no longer claims it. One helper owns every
  such ack; a refused fence or failed save acks nothing and the message is simply
  redelivered.
- **The commit points.** QoS 1 has one phase. For QoS 2 the durable **PUBREL
  intent** — written *before* the PUBREL is stored — commits the exit from the
  PUBLISH phase, and only a **confirmed** PUBREL (stored *and* in delivery)
  authorizes removing and acking the source claim. The phase is three-valued, not
  a boolean, because "a PUBREL exists" and "the store outcome is unknown" must not
  authorize the same actions.
- **Only transmitted deliveries drain.** A message withheld by the in-flight cap
  was never sent, so it is discarded rather than enrolled.
- **Single ownership.** From staging, a drained consumer lives exclusively in
  `sess.drains` and its `cons[sid]` entry is gone, so nothing can reach it through
  `cons` and create a second durable on the same filter. A re-SUBSCRIBE therefore
  **adopts** the drain explicitly.

Recovery is positive, never an inference from absence. Because the phase is
memory-only, a restart would otherwise establish source interest and resend the
payload of an exchange already past PUBREC — before the correlated PUBREL callback
ever ran. So a barrier runs **before any source interest exists**: it reads the
persisted intent, indexes the PUBREL records still on the session's subject (they
are unacked, hence incomplete), and resolves each claim. Intent with a record →
confirmed. Intent without one → recreate it under the *original* identifier,
having first verified the PUBREL durable exists and then checking **presence at
the returned sequence** rather than trusting the store response. A missing record
can never mean "already completed", because the callback removes claim and intent
before forwarding. No intent → the client provably never PUBREC'd durably, so the
payload is redelivered with DUP and the exchange completes. Any failure withholds
that drain's source interest for the connection rather than denying the CONNECT.

A stored PUBREL names its source (`Nmqtt-Q2SrcDur` / `Nmqtt-Q2SrcSeq`), which is
what links it back after a restart — `cpending` is memory-only. A **drained**
PUBREL additionally carries a deduplication identity scoped by an attempt counter,
so a superseded broker's late store collapses into it; the ordinary path
deliberately carries none, because there the only legitimate re-store is the
post-restart one after a redelivery, and a surviving dedupe entry would silently
swallow it.

## Outbound in-flight registers

`pendingPublish`, `pendingPubRel`, `cpending` and the packet-identifier allocator
were memory-only. A same-process reconnect therefore re-sent a started QoS 1/2
delivery under its original packet identifier, but a broker restart, a failover, or a
resume on another broker did not: the tracking maps came back empty, the delivery
looked unstarted, and the redelivery went out with DUP set under a **freshly
allocated** identifier — which a client is entitled to read as a new message.
[MQTT-4.4.0-1] requires both sides to re-send unacknowledged PUBLISH *and* PUBREL
packets under their **original** identifiers, so this bounded the broker's
exactly-once guarantee to one process lifetime, for QoS 1 as much as QoS 2.

An **in-flight register** supplies the missing durable fact
(`internal/mqtt/inflight_register.go`): one small record per started PUBLISH-phase
delivery on `$MQTT_inflight`, at `$MQTT.inflight.<idHash>.<gen>.<PI>`, naming the
source durable and stream sequence it came from. Keyed by the identifier, so the
create-only CAS is an allocation arbitration as well as a record, and so one
incarnation is bounded at 65 535 subjects — the same bound, for the same paging
reason, as the inbound hold walk. The record carries an immutable nonce, because the
subject is reused by later exchanges on the same identifier and a stream sequence is
therefore not an identity.

It is a stream of its own for two reasons that are not preferences: `$MQTT_out` is
interest-retained, so a store with no consumer succeeds and keeps nothing, and a
register has no consumer by design; and a per-delivery write to the session record
would put every outbound QoS 1/2 message behind the record CAS and the record fence.

| phase | who owns the identifier |
|---|---|
| PUBLISH sent, not yet acknowledged | the in-flight register |
| past PUBREC | the correlated PUBREL record on `$MQTT_out` (unchanged) |

The handoff is the PUBREL store. On that path the source acknowledgement is
**confirmed** before the register is deleted — the standing rule wherever a durable
record is deleted on the strength of an ack — which keeps the register from being
removed over a source that was never actually released, and so keeps the redelivery's
identifier. Where a register and a PUBREL record name the same source at recovery,
the PUBREL record wins.

An acknowledgement that cannot be confirmed needs one more thing. The source may still
be pending, so JetStream will re-offer it — and its payload is past PUBREC, so sending
it again is a duplicate. The source is therefore **retired**: the next delivery attempt
of it is acknowledged (with confirmation, under that attempt's own reply subject) and
discarded rather than delivered, which both suppresses the duplicate and settles the
message.

The fact is recorded against the **source**, not against the packet identifier, and that
distinction is the whole design. Re-establishing the source's link to its identifier
would make the ordinary in-flight guard suppress the redelivery — but only until
PUBCOMP, because PUBCOMP removes the pending-PUBREL entry that guard tests. From then on
the payload of an exchange the client has already *completed* would simply be re-sent. A
fact about the source outlives the identifier's bookkeeping; this is the same shape the
affine path already uses (its claim's `AffineDone`) and for the same reason.

It is recorded in **both** places, in that order: in memory first, because the PUBREL
whose delivery is already in flight can bring a redelivery at any moment and the
suppression has to be in place before this path spends a round trip; then on the
register, as a `Retired` mark written by a CAS at its exact sequence. The durable half is
not optional. Without it the next resume sees an ordinary PUBLISH-phase register over a
still-live source — indistinguishable from a delivery that never reached PUBREC — and
re-sends a payload the client has already completed. Leaving the source deliberately
unacknowledged is precisely what makes that window reachable.

That mark is not best-effort either, and the reason is worth following. While the
exchange's **PUBREL record** exists a restart is safe whatever the register says, because
recovery reads register-plus-PUBREL as "past PUBREC" — and converts it into the mark it
could not write earlier. So there are two pieces of durable evidence, and the rule is
simply that the second is not destroyed before the first exists: a PUBCOMP does **not**
acknowledge its PUBREL away while the retirement it covers is not durably recorded. The
cost of withholding is a PUBREL redelivery, which [MQTT-4.4.0-1] makes legitimate and
which the client answers with another PUBCOMP — bringing the broker back to retry, paced
by AckWait. Persist before ack, the same rule the UNSUBSCRIBE drain follows.

The source is left unacknowledged at PUBREC rather than retried best-effort: a retry that
landed would settle the message but strand its retirement, since nothing else would drain
it. One discarded redelivery settles the source, releases the retirement, and deletes the
register.

The invariants, in the order they matter:

- **Persist before send.** A PUBLISH under a new identifier goes on the wire only
  after its register is confirmed at a sequence. Between allocation and confirmation
  the entry is *unsendable by anyone*: a concurrent redelivery declines it, the resume
  NAK path skips it, and an UNSUBSCRIBE drain does not enrol it. A definite store
  failure releases the identifier and sends nothing, which is safe precisely because
  nothing could have sent it. An ambiguous store is resolved by re-reading the subject
  and matching the nonce, never by the shape of the response. This is also what makes
  a superseded broker's late register write inert: an identifier the client can be
  holding implies a confirmed register, so it implies the next broker's scan sees it.
- **Reserve before interest.** On resume every identifier named by a register or by a
  PUBREL record is reserved *before* any source interest exists — the barrier runs
  ahead of the CONNACK, both because a client may publish the instant its CONNACK
  lands and because the drain barrier stores PUBRELs under recorded identifiers that
  must already be reserved. This also closes a pre-existing hazard: `pendingPubRel`
  was empty until the PUBREL callback replayed a record, so a fresh allocation could
  mint an identifier an unreplayed PUBREL still owned.
- **An incomplete identifier space withholds allocation, not the connection.** The
  session stops *allocating* new identifiers while everything else keeps working:
  QoS 0 flows, reserved exchanges complete, and drained PUBRELs are still recreated
  under their recorded identifiers. Allocation is the only operation an unknown space
  makes unsafe, and a withheld QoS 1/2 delivery is redelivered by AckWait exactly as
  one withheld at the in-flight cap is. Denying the CONNECT was rejected: it takes a
  whole session offline over a transient failure, and it contradicts the drain
  barrier's contract. Three conditions trigger it, and they are one condition in
  different clothes — the identifier space cannot be fully accounted for: a walk that
  could not complete (identifiers may be in use that were never seen); a register whose
  body is unreadable (the identifier is known, the delivery that owns it is not); and
  two registers naming one source on different identifiers (both identifiers are known,
  but which one the client holds is not, because a stream sequence orders the *writes*
  and not the sends). In each case reserving identifiers is necessary but not
  sufficient — what stays dangerous is issuing a new one, or re-sending a source under
  a guess. A session discard lifts the restriction along with the incarnation whose
  space it protected.
- **Absence is proven, not inferred.** A register is dropped only where its source is
  provably finished — acknowledged, or authoritatively absent while the consumer that
  would have delivered it still exists. A source whose *consumer* is gone is not
  "complete": that delivery was lost rather than finished, and the client may still
  hold its identifier, so the identifier stays reserved. The same rule covers a record
  whose *body* is unreadable: its subject still names an identifier this incarnation
  wrote, so the identifier is reserved and the record kept, rather than deleted as
  uninterpretable. Reading "I cannot interpret this" as "nothing is in flight" is how
  an identifier the client is holding gets handed to a second message — and because
  such a record's *source* cannot be identified either, it additionally makes the
  identifier space incomplete (above).
- **Every terminal path deletes the register**, at its exact sequence: PUBACK, a
  failed PUBREC, the PUBREL store, Message Expiry, Maximum Packet Size, and every
  consumer teardown that abandons started deliveries. A failed delete costs a stale
  record that the store path's reconcile, the next resume scan, or the orphan sweep
  removes; it never wedges an identifier.
- **A surviving register is assumed client-held unless it proves otherwise.** Two very
  different things can leave a register behind: a delivery that really went out and whose
  consumer was later destroyed, and one whose allocation was withdrawn before it was ever
  sent. The first must keep both its identifier and its Receive Maximum slot — that limit
  bounds what the *client* is holding unacknowledged, and a consumer disappearing proves
  nothing about the client — so a session stuck this way is a lost message being reported
  honestly, not an accounting bug. The second must keep neither. Since a confirmed register
  otherwise means "about to be sent", the only thing that can tell them apart is a durable
  **cancellation tombstone**, written on the record when its deletion could not be proven;
  recovery then frees that identifier outright. And where neither the deletion nor the
  tombstone can be written — one JetStream outage covers both — the obligation is persisted
  with the session record rather than held in memory, because a restart would otherwise
  lose it and leave exactly the register it was meant to remove. Two things about that
  persistence are easy to get subtly wrong, and both were. Attempting the record write is
  not the same as landing it: the writer is a delivery goroutine, the UNSUBSCRIBE that
  cancelled the delivery holds the record fence, and that transaction may already have
  marshalled its record — so the addition is refused its own save *and* missed by the save
  that does happen. Durability is therefore tracked as a revision the record is known to
  carry, and every convergence point (each later register store, each resume, and the tail
  of the UNSUBSCRIBE itself) retries until the two agree. And an obligation stays visible
  in the list until its delete is **proven**: emptying the list first and putting the failures
  back afterwards lets any save falling in that window persist a record with no obligations
  in it, leaving the entry that comes back in memory only.
- **The cancellation is durable one step before its sequence exists.** The in-memory
  handoff covers everything except a crash: the cancelling UNSUBSCRIBE persists its
  removal and deletes the consumer while the register's CAS may still be on the wire, so
  if that write lands and the broker dies before its confirmation runs, no tombstone and
  no owed sequence was ever written — and recovery then finds an ordinary register whose
  consumer is gone. So the cancellation is also recorded as a **nonce** on the session
  record, inside the UNSUBSCRIBE's own transaction, which is what makes it durable
  before the consumer goes. It is keyed by nonce and not by packet identifier or by
  source because the cancellation frees the identifier for immediate reuse and a drained
  source is re-offered under a new one: either would eventually name a genuine later
  delivery and read it as never transmitted. The nonce is minted with the allocation for
  exactly this reason, one step ahead of the store that writes it. Recovery treats such
  a register as a tombstoned one, deletes it with proof, and releases the intent only
  against that proof — restating it as an owed sequence when the delete fails, since one
  mutation must never leave the record holding neither.
- **A consumer teardown writes the tombstone itself, and holds the identifier while it
  does.** The session record is the wrong home for this fact on the generic teardown
  paths (a QoS downgrade's deferred consumer delete, a forced shared leave, a shared
  release): each runs AFTER its packet's save, so an intent recorded there may not reach
  the record for a while. A tombstone at the register's own subject needs no fence and
  no packet, so it is durable exactly when it has to be — which is before the consumer
  that would classify the register's source is destroyed. The identifier stays RESERVED
  across the in-flight store rather than being freed at once, because a new delivery
  could otherwise take it and write its own register at the same subject, and nothing
  would then tell that record from the tombstone. The write is create-only first and a
  replacement at the exact sequence its own nonce is found at; a foreign occupant is a
  failure, never a success. And because such a tombstone can outlive its owner — where
  the teardown wins the race, the store returns without ever writing — the next
  allocation of that identifier REAPS a cancelled occupant instead of refusing it. A
  record that says "never transmitted" cannot be holding an identifier.
- **The cancellation goes in the write that removes the membership.** Recording it in
  the teardown instead cannot be made equivalent, and keeping the consumer does not
  redeem it: once the membership is gone there is no subscription left to redeliver
  through, so a register that lands afterwards holds its identifier and its Receive
  Maximum slot for the life of the session whatever becomes of the durable — under a
  Receive Maximum of 1, a session that can no longer deliver at all. So every
  transaction that removes a consumer's membership (an UNSUBSCRIBE, a reserved-namespace
  purge, a QoS downgrade, a 3.1.1 forced shared leave, a SUBSCRIBE rollback) withdraws
  that consumer's in-flight registrations first, and its own save carries both facts.
  The drained sids are exempt because stageDrain already did it and their consumer
  survives. What the teardown itself then finds can only be a registration that arrived
  after the staging, for which the tombstone is written and the obligation is otherwise
  left to the convergence points — the consumer is deleted anyway, since keeping an
  unreferenced durable nothing reaps makes no identifier recoverable.
- **The delivery barrier comes down before the snapshot.** Staging is only a moment in
  time otherwise: the delivery callback stays live until the packet commits, so a
  message arriving after the snapshot allocates an identifier whose cancellation misses
  the write that was supposed to carry it. Marking the doomed consumer's delivery
  subscription closed first — under the same lock the snapshot is taken with, and the
  same lock the callback tests it under — is what makes the snapshot complete rather
  than current. A rollback lifts the barrier again, because a packet whose write did not
  land leaves those subscriptions the client's. Two details of that reversal are
  load-bearing: it is scoped per FILTER, because one filter of a SUBSCRIBE can fail
  while the packet succeeds and its membership then survives unchanged; and both halves
  of a multi-level filter go through the same staging helper, since the level-up
  companion is a consumer in its own right.
- **A teardown's register removals are proven, not fired and forgotten.** The rule above
  — most register deletes may be fire-and-forget — holds only because the record left
  behind names a source that stays live and classifiable. A consumer teardown breaks
  that premise on purpose: it is about to delete the very consumer that would classify
  those sources, after which a surviving register is deliberately not read as complete.
  So these deletes are confirmed, they run BEFORE the consumer goes, and one that cannot
  be proven becomes an owed sequence on the session record, saved under the caller's
  transaction and retried at every convergence point.
- **What cannot be proven is stated as a rule, not as an instruction.** The confirmed
  delete and the owed sequence are both instructions, and an instruction nobody proved
  is an instruction nobody has: with JetStream unavailable for the delete AND for the
  record save, a crash leaves a register whose consumer is gone and whose identifier is
  therefore held for the life of the session. So the transaction that removes a
  consumer's membership also records the durable as **abandoned** — a verdict, not a
  task. Nothing on the live path acts on it, which is exactly why it can be written
  before the teardown runs (an instruction staged that early could delete a live
  delivery's register), and every later resume reaches the same conclusion whatever
  became of any delete request. It is withdrawn when the packet rolls back, and a
  durable the session HOLDS overrides it at the point it is read: shared group durables
  are named deterministically, so a leave-then-rejoin brings the same name back, and
  honouring a stale verdict there would free an identifier the client is holding — the
  reverse of the bug, and a worse one. Two details decide whether the rule is safe. It
  is spent only when a pass finds NO register naming its durable — withdrawing it in the
  pass that issued the deletes would let a lost delete plus a persisted withdrawal bring
  the phantom back — and the live-durable override reads the shared MEMBERSHIP, not the
  consumer map, because a shared consumer is deliberately never persisted in Cons: a
  restored session holds the group in sharedSubs with no cons entry, which a cons-only
  check would read as abandoned.
- **Cleanup that cannot be reconciled later is confirmed and retried.** Most register
  deletes may be fire-and-forget, because the record left behind names a source that stays
  live and classifiable. Two cannot: a **drained** exchange's release (whose retry must
  also accept both authoritative "already absent" answers, or a crash between the delete
  and the claim save withholds that exchange forever) and a **cancelled** registration's,
  whose consumer is being deleted in the same breath — that obligation is kept and retried
  at every later register store and every resume until absence is proven.
- **Ownership transfers explicitly, it is never assumed.** Three teardown paths take a
  register out of the ordinary terminal path's reach, and each takes the delete with it.
  A **drained** QoS 2 exchange holds its source claim until its PUBREL is proven, so
  nothing else reaches the delete: the drain records the register's sequence in its own
  state (persisted, so it survives a restart) and releases it — confirmed, before the
  claim removal and the ack — in its finalizer. A **consumer teardown** releases both the
  registers its `cpending` entries name and those of its *retired* sources, which are not
  in `cpending` at all. And an **UNSUBSCRIBE that races a first-delivery register write**
  cancels that allocation rather than skipping it, so the store's own ownership
  revalidation refuses the send instead of putting a PUBLISH on the wire whose tracking
  the UNSUBSCRIBE has just destroyed. A register left behind by any of them outlives its
  consumer, and a consumer-gone source is deliberately not read as complete — so the next
  resume would reserve that identifier for the life of the session.
- **Registered if and only if transmitted.** A delivery withheld by the in-flight cap
  was never sent, so it is never registered. A retained-message send has no JetStream
  backing at all, so it has no register and no post-restart redelivery.

A session discard purges the retiring incarnation's registers by filter. That blind
purge is correct here and the contrast with the arbitrated inbound hold walk is the
point: a register has no second party that could still be committed to acting on it,
and the filter names a generation `clear()` has already re-minted.

**Cost, stated plainly.** One create-only CAS round trip per *first* transmission of
every outbound QoS 1/2 message, one delete per completion, and on QoS 2 a confirmed
rather than fire-and-forget source ack. Redeliveries and recovered deliveries pay
nothing. Batching the *writes* was rejected because it would delay first transmissions
behind a batch, and registering *after* sending is not a cheaper version of this design
but a reintroduction of the deviation as a narrower window.

**Resume reads are batched, and that is a correctness-adjacent property.** The barrier
runs before the CONNACK, and a session may legitimately hold up to 65 535 in-flight
identifiers, so one request per record would put tens of thousands of serial round trips
in front of every reconnect — pushing restart recovery past ordinary client timeouts and
making failover impractical for exactly the sessions that need it most. Instead the
registers are enumerated by subject in one request (the identifier's 16 bits bound the
filter below JetStream's subject-detail truncation point, which is why this enumeration
is safe here although the account-wide orphan sweep refuses it) and loaded in bounded
batches; sources are then classified once per *source*, with one consumer lookup per
durable and batched message reads. Every uncertainty is preserved exactly as the
one-at-a-time path preserved it: a missing response, an unreadable consumer and a
consumer that is gone are all "unknown", never "complete".

## Session incarnations and teardown

A client ID hashes to a stable token, and a session's JetStream resources used to
be named from that hash alone. But the hash is reused: a clean start, a session
expiry and a takeover-discard each destroy one session and let a later one adopt
the same hash. A deterministic name is therefore **not an identity**, and a
teardown issued by a superseded broker could delete a resource a fresh session had
just created.

**Incarnation generations** (`internal/mqtt/sess_incarnation.go`) fix that
structurally. Each session carries a persisted generation (record field `gen`) that
scopes every otherwise-hash-only name: the PUBREL durable, its `$MQTT_out` subject
and its delivery subject; the `$MQTT_qos2in` hold subjects and the filters a
discard purges; and each `$MQTT_willq` outbox record. It is minted when a session
is first created and re-minted by `clear()` — which every discard path runs,
including the two that *reuse* the session object as a fresh session — while a
plain reconnect or resume **preserves** it, because a resumed session must still
find its own PUBRELs, QoS 2 holds and Will. A late delete from a retired
incarnation can then only name something that is already gone.

This is the same conclusion the QoS 2 move protocol and the affine delivery path
reached independently, and it is why the fix is naming rather than a lock: a nonce
re-check before each delete is a check-then-act, and no in-process lock arbitrates
anything against another broker.

**The teardown claim** (`internal/mqtt/sess_teardown.go`) covers what naming
cannot: two brokers tearing down and resuming the same record at once. Before
deleting anything, a discard CASes a `Teardown{Origin, Nonce, StartedAt}` marker
onto the record at the sequence it resolved against, preserving the record's
content so any broker can finish the job and so the orphan sweep still reads its
durables as referenced. Losing that CAS means a newer owner exists, and the loser
deletes **nothing** — a complete outcome, not a failure. Releasing is a
sequence-targeted delete whose result is classified by **re-reading**, because
`MaxMsgsPer: 1` may already have removed that sequence on a peer's behalf: absent
means done, our own nonce means retry, and a foreign nonce or no marker means a
newer owner exists. The release is always confirmed even on the no-wait discard
path, since an unclassifiable release is not a release.

Nobody resumes a marked record. Every CONNECT (and the record-driven expiry path)
that finds a marker **finishes** the teardown — repeating the idempotent,
generation-scoped deletions and releasing the record — and only then starts a fresh
session with a fresh generation and Session Present false. That also makes a
crashed teardown self-healing instead of leaking until the orphan sweep.

Shared group durables stay outside all of this: they are not owned by one session
and are released last, by the existing last-holder record scan, only once this
session's record is gone.

**Every session carries a generation.** A record without one is **rejected** at the
boundary rather than resumed on hash-only names: it predates this design, and under
the compatibility boundary above the development store is recreated. Accommodating
it would not be a smaller version of the same safety — a hash-only session has no
identity to scope an arbitration with, so the inbound QoS 2 release claim could not
be arbitrated for it and the discard would fall back to a blind hash-wide purge,
which destroys a held payload whose releaser claimed it and then died. A
partially-arbitrated path is worse than a rejected one.

**Mixed version.** An older broker ignores the `Teardown` marker and would
overwrite it, and uses the old names — so, as with the affine protocol, both halves
hold only once every broker understands them. See `docs/CONFORMANCE.md`.

## Orphan reconciliation

A broker that dies in the wrong window leaves durable state nothing else ever
revisits: an inbound QoS 2 hold and its release claim on `$MQTT_qos2in`, a
session-affine `$MQTT_Q2AFF_*` consumer on `$MQTT_out`, an assignment claim on
`$MQTT_qos2assign`, and the teardown marker that pins all three in place. None of
them loses a message — a stalled claim is honored, never released — but all of
them accumulate. A periodic per-account sweep converges them
(`internal/mqtt/orphan_qos2_sweep.go`), beside the existing session-expiry and
orphaned-consumer sweeps.

**Teardown markers are adopted first.** A marked record deliberately reads as a
reference to everything it names, so until it is finished none of that session's
QoS 2 state can become eligible. Once the marker has been *stably observed* for the
grace — keyed by the record's sequence and teardown nonce, so a resave or a peer's
teardown restarts the wait — the sweep re-reads it, requires that same sequence and
nonce, and runs `finishAdoptedTeardown`. No stored timestamp participates: neither
the record's store time nor `Teardown.StartedAt`. A young marker, an unreadable
record or a failed adoption leaves everything referenced.

**Absence is proven, never inferred.** Candidates are listed *before* a complete
walk of `$MQTT_sess`, so a resource created between the two is never judged
against a snapshot that predates it; local in-memory sessions count as references,
covering the create-before-save window on this broker. Ownership is the
`(client-ID hash, generation)` pair carried in the subject or durable name, never
the hash alone. An incomplete walk, an undecodable record, a client ID that does
not hash back to its subject token, or an unvalidatable generation aborts the
whole destructive pass.

**Two grace policies, and neither is an age.** `mqttOrphanSweepGrace` (five
minutes) covers construction windows — a resource created just before the record
save that references it — and applies to teardown markers, holds, assignments and
affine consumers. `mqttQoS2ReleaseOrphanGrace` (one hour) applies to release
claims, which arbitrate against a releaser that may be paused between loading a
payload and delivering it.

**Nothing subtracts two timestamps.** There is no clock to compare against: a
JetStream store time is assigned by whichever server led that stream's Raft group
at that instant, four streams are four groups, and one stream spans several clocks
across a leadership change — so a fast leader's timestamp minus a slow leader's
says nothing about elapsed time. Comparing either against the bridge's own
`time.Now()` is worse, for the reason the release claim's self-reported `At` was
rejected: the bridge is an external client on its own host.

So a grace is a **stable-observation requirement** measured on this process's
monotonic clock. A candidate becomes eligible only once it has been seen,
unchanged and authoritatively orphaned, across a monotonic span at least as long
as the grace — and **never on the pass that first saw it**: the first valid
observation only records the identity, and a later complete authoritative pass is
what may qualify it. Any identity change restarts the wait (a record's stream
sequence, a consumer's durable plus `Created`, a teardown nonce), as does the
resource becoming referenced again. Stored timestamps are diagnostic only. Failed
or incomplete scans record nothing and authorize nothing, and a mutation is still
revalidated at the moment it happens, separately from that eligibility check.

The observation table is bounded, and when it is full it **refuses new entries
rather than evicting**. Evicting is what a cache does and is exactly wrong here: a
deterministic scan revisits candidates in the same order, so admitting each new key
by evicting the one due next would make every sighting look like a first sighting
and nothing past the cap could ever qualify. Refusing keeps a stable resident
cohort that ages, is acted on, and releases its slot; entries nobody has revisited
in a very long time are pruned so a vanished resource cannot hold a slot forever.
State that is **retained by design** — a `Lost` assignment, or a `Discarded` one
whose source is not yet proven closed — is resolved before any observation and
holds no slot at all: it needs no wait, since it is already durably terminal, and
it is exactly what accumulates. Letting it occupy the table would let one account
fill it and refuse every other candidate permanently.

Two consequences, both erring towards leaving state alone: a broker that restarts
more often than the grace never reaps, because quarantine is in-memory and starts
over; and convergence takes several passes rather than one.

**The hold dies before its fence.** A `discarded` marker is reaped by nobody at
teardown time — it is the only durable proof that a late releaser must lose — and
the sweep may retire an aged one **only** after destroying the hold record and
confirming it gone. A releaser that preloaded the payload and resumes afterwards
then wins a fresh create-only claim against an empty subject, but its mandatory
authoritative re-read returns `mqttQoS2VerifyGone`, so it delivers nothing.
Reversing the two steps returns `mqttQoS2VerifyHeld` and fans the message out
again. An unclaimed hold is fenced by a fresh `discarded` marker before it is
purged, and every claim delete targets the exact sequence just read.

A record that cannot be *read* is still a fence — the claim write is a create-only
CAS, which fails while any record occupies the subject — so an aged unreadable
claim retires by the same route: purge behind it, then delete its exact sequence.
It is deliberately not deleted first and replaced with a fresh marker, because
that empties the subject while the payload is still there.

**One definition of an interpretable record, shared by every path that reads one.**
The reference walk fails closed on a record it cannot interpret, but the
post-snapshot re-reads are what actually authorize a mutation — so they apply the
same validation, not a subset of it. A record written after the walk whose client
ID does not hash back to its subject, whose generation is malformed, or whose
affine pair is inconsistent is **indeterminate**, never proof of absence: reading
it as "a valid record for a different generation" would discard a live assignment
or delete a referenced consumer along with its interest-retained copies.

**Every deletion target is validated against the record that names it.** A teardown
adoption deletes the `Cons`, `Drains`, `PubRel` and `Q2Aff` durables a marked record
carries, all copied verbatim, so a record naming another session's durable would
have that consumer deleted and its interest-retained messages with it. Each name is
bound to its session — a plain durable is `<idHash>_<nuid>`, the PUBREL and affine
ones are fully derived — and adoption checks all four. An affine consumer is
additionally re-looked-up immediately before deletion and must still be the same
instance quarantine observed: one deleted and recreated under the same name is a
different consumer, and deleting it by name would destroy a peer's fresh consumer
inside the very create-before-save window the wait exists to protect.

**The affine side is the mirror image.** An assignment is committed `Discarded`
*before* its consumer is deleted, because `$MQTT_out` is interest-retained and
deleting the consumer drops the copies it still held. The consumer is deleted only
after a complete assignment inventory, a second complete inventory as a
verification barrier, and a final re-read of the session record; a capped or
failed scan authorizes nothing. Assignments are deleted only through
`reapQ2AffClaim`, whose existing predicate refuses a claim marked `Lost`.

No stream gains a `MaxAge`: JetStream age eviction cannot perform this ordering or
these cross-stream proofs, so protocol state stays explicitly reconciled. The
inbound walk is bounded per tick by both a record count and a byte budget (the
message-get API returns whole records, so scanning a hold transfers its payload),
with a cursor seeded pseudo-randomly once per process and advanced sequentially
from there — random so a restart loop does not rescan the same prefix forever,
sequential so coverage inside one process is linear. The cursor is scheduling
state only.

## JetStream domain

The broker is a plain client of exactly one external JetStream, so `-js-domain`
(`MQTTNB_JS_DOMAIN`) is the single source of truth. There are no leafnode or
per-account topologies to resolve between, unlike the NATS Server this code came
from. Empty -- the common case -- means the domain of the server the bridge is
connected to.

The value is used in two places that must agree:

- **JS API subjects.** Requests are rewritten from `$JS.API.<x>` to
  `$JS.<domain>.API.<x>`.
- **Session subjects.** Sessions are stored at
  `$MQTT.sess.<domain>.<client-id-hash>` in `$MQTT_sess`.

Because the domain is part of the session subject, **changing `-js-domain` on an
existing deployment orphans every existing session**: the records remain in the
stream under the old token and the broker looks for them under the new one.
Treat a domain change as a session reset, not a configuration tweak.

The same setting also drives the `internal/backend` JetStream client
(`jetstream.NewWithDomain`), so both paths address the same domain.

`-js-domain` must be a **single subject token**: no `.`, `*` or `>`, no
whitespace, no control characters. This is deliberately narrower than the NATS
Server, which rejects `.`, `*` and `>` but would accept a domain containing
invisible whitespace or control characters -- such a value looks right in config
and logs while silently producing a JS API prefix and a session namespace that
are not what anyone typed. Ordinary non-ASCII is accepted, so a legitimate
Unicode domain the server would take is not refused here. Invalid values fail at
startup rather than surfacing later as unroutable requests.

## Backend seams (see docs/PORTING.md)

| Concern            | Mechanism                                             |
|--------------------|-------------------------------------------------------|
| Sessions           | JetStream stream (last-msg-per-subject), keyed by CID |
| Retained messages  | JetStream stream + subject tree metadata              |
| QoS1/2 persistence | JetStream durable consumers                           |
| QoS0 routing       | NATS core pub/sub                                     |
| Asset management   | `mqttJSA` request/reply over the service `*nats.Conn` |
| Per-client authz   | Auth passthrough: one `*nats.Conn` per MQTT client    |

## Auth passthrough

On CONNECT, the MQTT username/password are used to dial a per-client
`*nats.Conn`. NATS then enforces that client's publish/subscribe permissions for
every MQTT topic (mapped to a NATS subject). JetStream asset operations use a
separate service connection with its own credentials. In v1 all MQTT users must
belong to the same NATS account as the service connection, because the
session/retained/QoS streams are account-scoped.
