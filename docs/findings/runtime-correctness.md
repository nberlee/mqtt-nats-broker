# Runtime correctness and liveness findings

These are bounded implementation defects in connection teardown, subscription rollback,
synchronization, and pending-state release.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10.

## F-035

**Title:** Bound every server-initiated courtesy DISCONNECT write

- Status: Closed 2026-09-06.
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 35

**How it closed, which is not how the finding proposed.** The finding asked for a write
watchdog armed at every courtesy site. Two plan-review rounds faulted that design's
ENUMERATION five times — the arming-site count went 3 to 7 to 9 — while never faulting its
shape, which is the signal that the enumeration is itself the defect.

The property those sites approximate has one home, and upstream nats-server already holds
it while this port dropped it: every client write is bounded by `opts.WriteDeadline`,
CLEARED after the write, with a miss closing the client as `SlowConsumerWriteDeadline`
(`DEFAULT_FLUSH_DEADLINE` = 10s). The shim replaced upstream's `writeLoop` and outbound
buffer with a synchronous write under `wmu` and a no-op `writeLoop`, and the deadline went
with them, leaving `writeSocket`'s `deadline` parameter with a single caller.

So F-035 is a RESTORED bound, at upstream's value. `writeSocket` computes and installs
`mqttWriteDeadline` itself and clears it before returning; `queueOutboundBy` (since F-105
WP-F W1, `writeActionlessFrameBy`) carries the shutdown drain's absolute cap; and
`writeDirect` is deleted. No per-site arming, no licence audit, no source guard, no
timer, no new goroutine anywhere.

A planned fourth part — moving the notification into the close goroutine at the three
displacement tails that own both — was DROPPED during implementation by a Codex scope
ruling. It failed NINE existing tests; reverting only that move turned all nine green. It
is not load-bearing for the claim, because the deadline already bounds those writes, and
one of the nine asserts a durable register delete rather than mere observability, so the
scheduling change needs its own analysis and its own oracle strategy rather than riding on
a bounded-writer repair. Nothing is owed: the inline form is correct and bounded.
`docs/ROADMAP.md` carries the evidence.

**The clear is load-bearing, and its absence was the one real defect a review found in the
replacement mechanism.** `c.nc` may be a `*tls.Conn`, and TLS writes bytes that never pass
through `writeSocket` — a TLS 1.3 KeyUpdate response is written from `Read`. A deadline
left installed expires through idle time and hands that write a permanent outbound error no
later MQTT write can repair.

**Residuals**, all in `docs/CONFORMANCE.md`: the slow-consumer disconnect and its
session, QoS, shared-group and Will consequences; a frame that can be truncated mid-write;
no gate-duration bound is claimed, because one `c.mu` section may contain many bounded
writes and a gate-held Will phase can wait on `subsBarrier.wait`, which has no elapsed-time
bound (its extension by later SUBSCRIBEs was closed 2026-10-12; the slowest sampled
SUBSCRIBE still bounds it); and two pre-existing durable-loss exposures that become more
reachable rather than new. Two follow-ups were filed as roadmap bullets rather than
absorbed: anchoring a delayed Will's `FireAt` to the close instead of to consumption, and
the `subsBarrier` bound (its extension half closed 2026-10-12).

### Original finding record

- [ ] Every server-initiated COURTESY DISCONNECT write is unbounded, and one of them is
  performed with the ownership gate held. `mqttEnqueueDisconnect` → `queueOutbound` →
  `writeSocket` with a ZERO deadline (`shim_engine.go`), so a client that has stopped
  reading blocks the writer indefinitely: the parse-error footer's 0x89 defers the
  teardown that follows it, the bound settlement's foreign DISCONNECT pins its own readLoop
  (it said 0x8E when this was filed; F-084 made that route 0x89, which changes nothing about
  the unbounded write), and
  the periodic pass's `disposeForeignSessionLocked` performs that write while holding the
  session gate, which blocks a reconnecting CONNECT for as long as the stalled client
  lasts. The DISCONNECT claim above removes a write only when it suppresses one (a second
  notification, or a pre-CONNACK displacement); the first claimant still writes unbounded,
  so
  every site here stands. A deadline is not enough on its own: `writeSocket` installs it
  only AFTER acquiring `wmu`, so an earlier unbounded writer holding that mutex leaves the
  "bounded" write unbounded — a gap `shutdownDisconnect` shares. Three fix shapes were
  tried and each was killed by a review round of the bound settlement, so do not repeat
  them: (1) a
  SYNCHRONOUS write on the readLoop bounds nothing and pins the loop; (2) an ASYNCHRONOUS
  goroutine that writes then closes never reaches the close while the write blocks, and
  with the read deadline cleared the connection stays open forever; (3) `writeDirect` with
  a deadline does not bound the `wmu` acquisition. What is needed is a real write
  watchdog (arm the connection deadline before contending for `wmu`, or close from a timer
  independent of the writer), designed once and applied to every courtesy write.

## F-039

**Title:** Restore shared-subscription membership during SUBSCRIBE rollback

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 39

### Original finding record

- [x] Make `readd` restore `sharedSubs` alongside `cons`, or release from both.
  **Done by restoring both maps** on the definite-failure arm; "release from both" was
  refuted as leaving a permanent hole after a broker restart or a record-derived expiry.
  `rollbackSubscribePacket`'s `readd` (`mqtt.go`) restores the packet's added
  `cons` entries and reopens their delivery, but not the `sharedSubs` key, so a
  session can end in a state where a live shared consumer is outside the
  membership. The pending-state-derived purge above is immune, but `clear()`'s
  shared-durable RELEASE still derives from `sharedSubs` alone (`mqtt.go`), so
  such a session never releases the re-added shared consumer's group durable —
  which `sharedDurablesHeldByOthers` then keeps alive for a member that no
  longer exists. (That scan was deleted 2026-09-22; a group durable's fate is now
  its group record's, `docs/CONFORMANCE.md`, "A group durable's release is
  arbitrated by its group record".) Either restore both maps in the rollback, or
  make the release
  walk both. Found by the plan review of the purge bullet above.

## F-045

**Title:** Release stranded shared pending state when membership is removed

- Status: **Closed**, by two separate items — see the attribution below.
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 45
- Narrowed since: the preserved record below names three stranding paths;
  `purgeReservedSubs` was deleted by [F-057](resolved.md#f-057), leaving two.

### Closed at both remaining sites, by different items

Neither item changed both sites, so the closure is **joint** and says so rather than implying
one fix covered everything:

- **UNSUBSCRIBE's no-`cons` branch — closed by [F-083](shared-qos2.md#f-083)**, whose commit
  half releases the leaving key's own state (pending deliveries, identifiers, ack-budget charge
  and deliver-subject subscription) independently of the last-holder answer.
- **The 3.1.1 forced leave — closed by the forced-leave item** in `docs/ROADMAP.md`
  §0.2. It
  derives its pending purge from the pending maps (`cpending`, `deferred`, `srcAckOwed`,
  filtered by the shared durable prefix) rather than from either structural view, and it does
  so at the **staging** half as well as the commit — because the write that removes the
  membership must also carry "these allocations were never transmitted", and a crash in between
  would otherwise leave a live register with no cancellation recorded and no membership
  able to
  trigger another leave.

**Two facts found while closing it**, recorded because they are not in the original record.
A pending-only target is purged but must **never** be deleted: pending state is not evidence
that this session was the group durable's last holder. And an **unparseable** membership key
strands its durable silently — `mqttSharedDurablesForKey` returns nil, so the delete loop
iterates nothing while the membership is removed anyway; the forced leave now refuses
such a
resume pre-CONNACK instead.

**What this did NOT close:** the stranding was an identifier and Receive-Maximum hold,
not a
spurious NAK. A candidate claim that the stranded entry gets NAK'd into the group on
resume was
**refuted** — both resume NAK loops require a live ack subject, which a register-rebuilt entry
does not have.

### Original finding record

- [ ] Free stranded shared pending state at the membership removal, not at the
  close. The 3.1.1 forced leave, UNSUBSCRIBE's no-`cons` branch and
  `purgeReservedSubs` all stage register verdicts and deliberately leave a
  TRANSMITTED delivery's `cpending`/`pendingPublish` entries behind, because
  their own purge walks only `cons`-derived durables and a record-restored
  session has none. The fix above frees them at the close (previously: never),
  but the removal-to-close hold remains: for the rest of the connection those
  entries hold packet identifiers and Receive Maximum slots for a subscription
  that is already gone — under a Receive Maximum of 1, a session that can no
  longer deliver at all. The removals should purge what they strand, by the same
  pending-state derivation. Spec5 [4.8.2], [4.9].

## F-059

**Title:** Arbitrate retained-index application by stream sequence

- Status: Open
- Classification: Confirmed correctness defect
- Release treatment: Alpha blocker
- Filed: 2026-08-10, while resolving [F-056](resolved.md#f-056)

### Evidence

The durable retained state is arbitrated: `$MQTT_rmsgs` holds one record per
subject (`MaxMsgsPer: 1`), the store evicts the record a write replaces, and an
empty body is the deletion. The VOLATILE index every broker serves from —
`as.retmsgs` plus `rmsCache` — is not. Three writers apply what they observe
without comparing it to what the index already holds:

- `processRetainedMsg`, the retained stream's consumer callback: `addRetainedMsg`
  accepts any sequence and overwrites the stored one, and the tombstone arm calls
  `removeRetainedMsg(subject, 0)`, which removes whatever is there.
- `mqttHandlePubRetain`'s synchronous local seed, applied on the publishing broker
  right after `storeMsg` returns, concurrently with that broker's own callback for
  the same record.
- The decode-failure eviction added by F-056, also unconditional.

The consumer is a per-broker ephemeral `AckNone` `DeliverAll` consumer, so a
dropped delivery is never redelivered. Loss is NOT delay, and conflating the two
would frame this finding as an ordering problem it is not: a delayed callback
eventually runs and the index settles on what it applies, whereas a lost one runs
no writer at all.

What ends a loss depends on whether the subject is still indexed, and only one of
the two cases is unbounded:

- **Indexed subject.** Any later matching SUBSCRIBE, wildcard or exact, puts the
  subject through `loadRetainedMessages`, which reads the stream once the cached
  copy has expired; its tombstone arm calls `removeRetainedMsg(subject, 0)` and a
  replacement refreshes what is served. These arms are therefore bounded by the
  retained cache TTL plus the next matching subscription. That load does not call
  `addRetainedMsg`, so the index's stored SEQUENCE is left stale even after the
  served content is right.
- **Absent subject.** Nothing repairs it. Serialization enumerates `as.retmsgs`,
  so the subject is not a wildcard candidate at all, and an exact filter's forced
  authoritative load caches it without making it deliverable. It stays
  undeliverable on that broker until a later application for the subject arrives,
  the broker restarts and replays the stream, or the reconciliation described
  under "Fix direction" exists.

### Observable surface

- **Missed or reordered add, or an over-eager eviction (absent index entry)** —
  retained replay is suppressed for WILDCARD and EXACT filters alike.
  `serializeRetainedMsgsForSub` matches candidate subjects against `as.retmsgs`
  and returns immediately when that index is empty, so the exact filter's forced
  authoritative stream load fetches the record into the cache without making it
  deliverable. That forced load narrows only the window in which a just-published
  record's callback lands during the SUBSCRIBE's own round trip. A delayed
  decode-failure eviction lands in this arm. **This is the arm no subscription
  repairs**, and so the only one that is unbounded on loss.
- **Missed or reordered replacement** — the OLD value stays in `retmsgs` and
  `rmsCache` and is served stale to wildcard AND exact subscribers, since a lookup
  returns the cached record without reading the stream. A DELAYED OLD add can
  actively restore a stale value over a newer one, because `addRetainedMsg`
  overwrites the sequence and the cache entry with no comparison. Bounded: after
  the cached copy expires the next matching subscription reloads the stream and
  serves the current record, though the index entry's sequence stays wrong until
  the next application.
- **Missed or reordered tombstone** — stale retained delivery to wildcard and
  exact subscribers, under the same bound. Here the reload does more than refresh:
  reading the tombstone removes the index entry outright, so the arm self-heals at
  the next matching subscription after cache expiry.

### Disclosure

[F-056](resolved.md#f-056) removed the `RD` notification protocol, which was an
additional best-effort removal path for the specific case of a remote broker
whose `AckNone` tombstone delivery was lost. Its removal widens this finding's
tombstone window — that arm now waits for cache expiry plus a matching
subscription instead of possibly being retired sooner by the notification. It
does not make the arm unbounded and adds no failure class. F-056 also added the
decode-failure eviction, which is itself unarbitrated and lands in the
absent-index arm — the one no subscription reaches. The fix must therefore cover
LOST deliveries, not only reordering, and must arbitrate the eviction alongside
the add and the tombstone.

### Fix direction

This needs two mechanisms, and it is worth being explicit that the first does not
subsume the second.

**Ordering.** A per-topic last-applied stream sequence watermark that also covers
deleted state (a tombstone must raise the watermark, not clear it, or a delayed
older add walks back over the deletion), rebuilt from the stream on restart. Every
index writer — the callback's add, the callback's tombstone removal, the local
seed, and F-056's decode-failure eviction — compares against it and drops what it
has already passed. Recording the watermark alongside the index reference keeps it
under one lock. `MaxMsgsPer: 1` means the stream never holds a superseded record,
so the watermark is only ever compared against, never used to fetch.

**Loss.** The watermark cannot repair a LOST application, and must not be
described as if it could: when a remote broker's `AckNone` delivery is dropped no
writer runs at all, so there is nothing for a watermark to reject and nothing to
notice the gap. Scope this precisely, because a repair already exists for half of
it: an INDEXED subject is reconciled by the next matching subscription's
`loadRetainedMessages` read once its cached copy expires. What that repair cannot
reach is an ABSENT subject, since serialization only ever enumerates the index —
so the fix has to make a broker able to discover a per-subject state it does not
know it is missing. That needs either a consumer whose deliveries are replayable
(an acknowledged or durable consumer in place of the ephemeral `AckNone` one,
with the retention cost that implies) or a reconciliation comparing observed
stream progress against the index. The two mechanisms are complementary:
reconciliation bounds how long a lost application can persist, and the watermark
is what keeps its repair from being undone by a straggler.

## F-060

**Title:** Report a failed retained store to the publisher

- Status: Open
- Classification: Confirmed correctness defect
- Release treatment: Alpha blocker
- Filed: 2026-08-10, while resolving [F-056](resolved.md#f-056)

### Evidence

A failed retained store is invisible to the publishing client.
`mqttHandlePubRetain` logs a `storeMsg` error and returns, and the delivery path
reports success regardless. [MQTT-3.3.1-5] requires the server to store the
message; a client that publishes retained state has no way to learn that the
retained copy does not exist, and no retry is attempted. This spans all three
quality-of-service levels, and they do not share a remedy:

- **QoS 1 and 2** are acknowledged anyway — the PUBACK is sent, and the QoS 2
  handshake completes — so the client is told the publication succeeded.
- **QoS 0 has no acknowledgement to withhold.** The same swallowed error occurs
  there, and a fix framed only around acknowledgement leaves that publisher
  completely unaware. It is the arm most likely to be missed, which is why it is
  named here.

This is independent of the compatibility mechanisms F-056 removed; it was
surfaced by that item's crash-window analysis. The neighbouring window — live
fan-out precedes the retained store, so subscribers can see a PUBLISH while the
retained authority is still the previous record — is a separate ordering question
and is not part of this finding.

### Fix direction

Decide the reporting contract first, and decide it for all three QoS levels
together. For QoS 1 and 2 the options are failing the acknowledgement (v5 has a
reason code for it; 3.1.1 has none for QoS 1, so a PUBACK is either sent or the
connection is closed) or retrying the store before acknowledging. For QoS 0 the
only signals available are a logged failure the client cannot see and terminating
the connection, which under MQTT 5.0 can carry a DISCONNECT reason code — so its
contract has to be chosen deliberately rather than inherited from whatever the
acknowledged path does. Whatever is chosen must not acknowledge a store whose
outcome is still unresolved, which is the same ambiguity class the session-record
work already settled by re-attempting rather than reading back.

## F-061

**Title:** An unauthorized CONNECT is closed with no CONNACK

- Status: Open
- Classification: Confirmed conformance gap (a SHOULD)
- Release treatment: Post-alpha candidate; promote if a conformance suite or an
  operator report contradicts the evidence below

### Evidence

The shim's `authViolation` (`internal/mqtt/shim_engine.go`) is `closeConnection`
and nothing else. It is the CONNECT-authorization failure path: when auth
passthrough is configured and the per-client NATS dial is refused,
`mqttProcessConnect` calls it and returns `ErrAuthentication`. So a client whose
credentials NATS rejected sees its socket close with no CONNACK at all — it cannot
distinguish bad credentials from a network failure, a full broker, or a listener
that is not an MQTT broker.

The call site believes otherwise. `mqtt.go` comments that "the CONNACK is enqueued
inside `authViolation`", and traces an outbound CONNACK with return code
`mqttConnAckRCNotAuthorized` when the call returns true — which it always does.
The trace therefore records a packet that was never written.

MQTT 3.1.1 [3.1.4]: the Server "SHOULD send an appropriate CONNACK response with a
non-zero return code ... and it MUST close the Network Connection". The MUST is
met; the SHOULD is not. The MQTT 5.0 wording and its reason code (0x87 Not
Authorized against 3.1.1's 0x05) are to be verified against the specification text
when this item is planned rather than assumed to match.

Found while reading this path for F-051 (the CONNECT deadline), not caused by it.
F-051 deliberately left the false comment and the trace untouched, because this
finding owns that path and that change does not enter it.

Both conformance suites pass today — vibesrc/mqttconformance 124/124, Paho 106/106
— which is the evidence for the post-alpha treatment: no suite currently exercises
a rejected CONNECT against a broker that authenticates.

### Fix direction

Write the CONNACK before the close, at the version the connection negotiated, and
make the returned value report whether it was actually written — the same
"a delivered CONNACK is a checked fact" discipline `mqttEnqueueConnAck` already
applies on the success path, whose write error is part of its verdict. Then the
call site's trace stops claiming a packet that was not sent. A connection whose
CONNECT lost the race to the connect deadline is a different case and must stay
silent: its socket is already closed, and it was never authenticated.

## F-067

**Title:** Refuse a pre-release delayed-drain record shape instead of repairing it

- Status: Open. Filed 2026-08-22 by F-034's persisted-identity census.
- Classification: Pre-release persistence compatibility
- Release treatment: Post-alpha cleanup. Not an alpha blocker: the accommodation is
  benign at runtime, and what it violates is a project rule about how unsupported
  shapes are treated rather than a conformance guarantee.

### The finding

`mqttPersistedDrainSeq.RegSeq` documents itself as tolerating a record written before
the field existed:

> *Optional: a record written before this field existed carries none, and the resume
> barrier repairs it from the register scan rather than treating it as "no register".*

`docs/ARCHITECTURE.md`'s **Compatibility boundary** is normative and says the opposite:
there are no releases and no mixed fleet, so an older broker is a population that does
not exist, and an unsupported persisted shape is **refused** rather than migrated or
repaired. That is the rule F-056, F-057 and F-058 each implemented for their own record
type. This is the same class, still open.

### The analysis its fix must do FIRST, and why the obvious fix is wrong

**A zero `RegSeq` is not proof of a legacy record.** A current-schema drain can carry
zero legitimately, by at least two routes verified in the code:

- the finalizer clears `regSeq` after a **proven** release but before acquiring and
  saving the claim-removal transaction, and another concurrent save can persist that
  intermediate state;
- recovery deliberately clears it for **ambiguous duplicate** drain registers, where two
  registers named one delivery and neither may be trusted.

So a refusal keyed on `RegSeq == 0` would refuse live sessions in an ordinary crash
window. The fix must first distinguish *provenance* — a shape this broker no longer
writes — from *a value this broker writes today*, and only the former may be refused.

Until then the accommodation stands, and it is recorded here rather than left as a
comment nobody owns.

## F-068

**Title:** Revalidate an enqueue against the session it was allocated under

- Status: Open, CHAPTER P's (re-scoped 2026-10-06, F-105 WP-F W3, consult 133). No
  supported-path form remains; the remaining gap is the experimental affine route's raw
  output. Closure for the supported path is conditional on F-105's integrated branch
  review, which has not passed at the time of writing.
- Classification: Runtime correctness
- Release treatment: chapter P (experimental session-affine shared QoS 2), not alpha.

### The finding as filed (2026-08-22, F-034's consequence audit)

An outbound QoS 1/2 delivery persisted its in-flight register **before** the send, and
the two were adjacent but not atomic: `storeInflightRegister` returned through
`confirmInflightRegister`, which cleared `regPending` and so released the entry for
transmission, and only then did the caller enqueue. A local `clear()` inside that
window erased `pendingPublish` entirely, and `mqttEnqueuePublishMsgTo` then queued the
packet without revalidating the session generation, the client binding, the pending
entry's nonce, or whether the subscription was closed. It was bounded by destination:
the enqueue targeted the captured connection object, so the packet was dropped or
reached that same discarded session's own old client, never another session's.

### Why the supported path no longer has that route

- **Ordinary and shared-group QoS 1/2 delivery** admits through the outbound obligation
  authority (`admitInflightPublish` / `admitSharedInflightPublish`), not
  `storeInflightRegister`, whose one production caller is the affine writer. After
  admission the callback RE-ENTERS the send section under the session lock:
  `beginSendLocked` refuses unless `sess.c` is still the delivering connection and the
  pending entry still exists with the same durable, stream sequence and an unvoided
  nonce, so an entry a `clear()` erased opens no section. The frame is then written only
  through `writeAuthorizedFrame`, which executes the section's acquisition once, for its
  live holder, on the connection it was acquired on, and emits nothing when the output
  gate is sealed or the holder no longer holds it.
- **An acquired action executing on its original still-open connection after a
  retirement is PERMITTED** by F-105's contract (`docs/CONFORMANCE.md`, "A displaced
  connection may receive a late packet"): bytes handed to a transport are beyond durable
  arbitration, and a retirement is not treated as stopping a remote socket. That
  behaviour is not this finding.

### What remains, and where it lives

The experimental affine route (`qos2_shared_affine.go`) also opens its send section
through `beginSendLocked`, so the filed binding and entry checks run there too. What it
lacks is the authorized write: its acquisition is the slot machine's register
compare-and-swap with no action record, and its prepared frame carries no acquisition
requirement (`bindSection` exempts the slot-only class), so
`mqttEnqueuePreparedPublishMsgTo` writes it through the raw `queueOutbound` arm. Nothing
at the write itself re-checks the binding, the output gate or the holder after
`beginSendLocked`. That raw-output and revalidation gap is the affine output conversion
chapter P owns (`docs/CONFORMANCE.md`, "What the output boundary does not cover"); this
finding's in-process enqueue fence, if still wanted after that conversion, goes with it.

### What a fix looks like (chapter P)

Convert the affine output to the authorized boundary, or at minimum an **in-process**
enqueue fence that revalidates binding and holder under the lock that owns them at the
write. Deliberately **not** durable arbitration: there is no second party contending for
anything here, and inflating an in-process ordering problem into a durable protocol is
the mistake F-034's own review cycle rejected twice.

## F-079

**Title:** A denied publish is accepted, stored and delivered

- Status: Open. Filed 2026-08-25 while auditing upstream nats-server PR 8415.
- Classification: Authorization
- Release treatment: **Alpha blocker.** The broker's stated authorization model does
  not hold for publishing, and the broker never acts on the denial: no MQTT
  refusal is returned, no publication-correlated enforcement happens, and the
  message reaches subscribers. It is not literally log-silent — nats.go's default
  asynchronous error handler prints the permission violation to stderr (DialClient
  installs no custom handler), and the NATS server logs it too. Nothing
  *correlates* either log with the MQTT publication or stops it.

### The property this broker must satisfy, and does not

> For each MQTT-originated application publication, authorization of the
> **CONNECT-authenticated identity** to publish **that publication, to its final
> effective NATS subject**, must be established **before or atomically with its
> first application-visible or durable effect**.
>
> None of these is evidence of it: successful CONNECT authentication; authorization
> observed for a different publication or a different subject; a `PublishMsg` that
> returned nil; or the authority of the service or internal client the work was
> moved onto. The decision must **remain attached through persistence and any later
> replay**.

It is stated as a property, not a list of broken call sites, deliberately. Three
review rounds each produced another route, which is the signal that enumeration is
the wrong instrument — `feedback_named_sites_are_a_sample`. The routes below are
**evidence for the property, explicitly not the set**.

### Why the property fails today

Auth passthrough dials a per-client `*nats.Conn` from the CONNECT credentials and
binds it as `c.nrc`, so NATS can enforce that identity's permissions. For
**subscribe** it does: `processSub` issues a post-`Subscribe` `Flush`, the violation
surfaces in `LastError`, and the broker answers SUBACK 0x80 (see
`docs/CONFORMANCE.md`, *Subscribe failure*).

For **publish** there is no equivalent, and three facts compose into a bypass:

1. `pubAllowedFullCheck` returns `true` unconditionally — in-broker checks are
   deliberately permissive *because* NATS is supposed to enforce.
2. A core-NATS publish denial is **asynchronous**. The server answers `-ERR
   Permissions Violation`, which `c.nrc.PublishMsg(m)` never observes.
   `processInboundClientMsg` therefore returns `permIssue = false` on every path.
3. Consequently the `permIssue` arm in `mqttInitiateMsgDeliveryJSA` is **dead
   code**. It must not be mistaken for coverage.

The live fan-out does ride `c.nrc` and is genuinely dropped. Everything that does
not ride `c.nrc` proceeds:

- **QoS 1/2** — `jsa.storeMsg(mqttStreamSubjectPrefix + ...)` runs on the account
  manager's JSA, i.e. the **service connection**. The message lands in `$MQTT_msgs`
  and is delivered to durable subscribers through service-owned delivery
  subscriptions.
- **Retained, any QoS including 0** — `mqttHandlePubRetain` runs inside
  `processInboundClientMsg` after `PublishMsg` returns nil, and its store is also on
  the service connection. The copy is persisted and replayed to future subscribers.
- **QoS 2 hold** — `mqttProcessPub` case 2 goes straight to `mqttStoreQoS2MsgOnce`;
  it does not attempt the per-client publish at all until PUBREL.
- **Internally replayed Wills** — `pubAllowedFullCheck`'s three real callers are all
  Will authorization (delayed capture and takeover publication in `mqtt.go`,
  uncertainty parking in `sess_uncertain.go`), and the Will is later published by an
  **internal client** via `deliverWillRecord`. These bypass **including their live
  fan-out**, which is strictly worse than the other three routes.

  An **immediate** Will is different and must not be lumped in: it calls
  `mqttInitiateMsgDelivery` on the *original* client, so its core publish rides
  `c.nrc` and **is** denied; only its persistence bypasses. A non-retained immediate
  QoS 0 Will has **no bypass at all**. The delayed path's own comment explains why
  the permissive check is load-bearing: a delayed Will cannot re-check permissions at
  fire time, so the check at capture is the only gate — and it always says yes.

The service connection holds full rights by design (`test/conformance/nats.conf`
grants `service` `publish: ">"`), which is exactly what makes it the bypass.

### Measured, not merely read

Confirmed empirically on 2026-08-25 with an in-process NATS server denying user
`alice` publish on `denied.>`, one MQTT client publishing and another subscribing
(both as `alice`, subscribe allowed). The server logged
`Permissions Violation for Publish to "denied.q1"` for each denied publish, and:

| publish      | NATS verdict  | client outcome                | delivered to subscriber |
|--------------|---------------|-------------------------------|-------------------------|
| `allowed/q1` | allowed       | PUBACK                        | yes (control)           |
| `denied/q0`  | **violation** | no ack exists at QoS 0; the send completed without rejection | **no** |
| `denied/q1`  | **violation** | **PUBACK (success)**          | **yes**                 |
| `denied/q2`  | **violation** | **PUBREC/PUBCOMP (success)**  | **yes**                 |

That is the property's failure in one table: QoS 0 rides `c.nrc` and is dropped;
QoS 1 and QoS 2 go through the service connection and are delivered. The QoS 1 and
QoS 2 publishes were **acknowledged as success** — the publisher is told its denied
message was accepted. QoS 0 has no acknowledgement to be wrong about; its only
observable is that the send raised nothing.

The probe was a measurement instrument and is not committed: a fixture asserting
today's behavior would encode the defect as expected. The fix's fixtures are named
below, and this matrix is what they must invert.

### The current conformance suite is not evidence against this

Its only publish deny is `$MQTT.>`, and `mqttProcessPub` rejects the reserved
namespace *before* any store is attempted, so the deny never reaches the bypass.
There is no ordinary-topic publish deny in the config at all. A green suite here
says nothing about publish authorization —
`feedback_fixture_must_reach_its_route`, at the configuration level.

### An inherited fixture for this already exists, and cannot run

`TestMQTTv5WillDelayPermViolation` (`internal/mqtt/mqtt_v5_cluster_ref_test.go`)
makes two assertions about a delayed Will on a denied topic, and **only one of them
is this finding's**:

- *not persisted with the session record* — holds today, but **vacuously**:
  `mqttPersistedWill` is vestigial, nothing assigns `sess.will`, so no Will is ever
  written there regardless of permissions. Porting this assertion would produce a
  fixture that passes for the wrong reason.
- *dropped when the timer fires* — **this is the applicable one**, and it is what
  F-079 breaks. The active durable path is the `$MQTT_willq` outbox, and the replay
  publishes through the internal client whatever the ACL says.

Its own comment gives this finding's rationale — *"a restart cannot re-check
permissions, so a restored Will would bypass them."*

It has never been able to fail, because it sits behind the `mqttport_clustertests`
tag, which **does not compile** (`go vet -tags mqttport_clustertests` fails with
`undefined: cluster`) and which no `make` target builds. `docs/PORTING.md` records
that tag as carrying unbuilt references.

So the assertion whose violation this finding reports was inherited and is
invisible. Whoever fixes F-079 should port the **fire-time suppression** assertion
and retarget it at `$MQTT_willq`, and should not port the session-record absence
check as-is — it would pass against a completely broken broker.

### What a fix must add

Fixtures are **evidence for the property, not a definition of it**. Four, all
deny-publish on an *ordinary* topic: QoS 0-retained, QoS 1, QoS 2, and — the one that
must not be generic — a **delayed or takeover Will, non-retained QoS 0**. That shape
is chosen because every competing *delivery or message-storage* route is absent from
it (retained handling returns immediately; QoS 0 skips the `$MQTT_msgs` store), so a
live-subscriber assertion can only be satisfied by the internal client's fan-out. Note
the precision: a *delayed* Will does write its own durable outbox record, so the claim
is about competing **delivery** routes, not "no persistence at all" — use the
**takeover** variant if the fixture must also assert no preceding outbox record. An
unqualified "Will" fixture would most likely exercise an immediate Will, which is not
bypassed live, and would prove the opposite of what it claims.

The fix design is **not settled here**. The subscribe side's `Flush` + `LastError`
is the obvious starting point and is not automatically correct: it cannot probe QoS 2
before PUBREL without publishing prematurely, cannot probe a delayed Will without
firing it, reintroduces a synchronous round trip on the hot path, and does not address
F-080 at all.

## F-080

**Title:** The authenticated identity's NATS account is never bound

- Status: Open. Filed 2026-08-25, split out of F-079.
- Classification: Authorization / tenancy
- Release treatment: **Alpha blocker.** A documented constraint that nothing enforces.

### The finding

`docs/ARCHITECTURE.md` states the v1 constraint that all MQTT users must belong to
the same NATS account as the service connection, because the session, retained and
QoS streams are account-scoped.

Nothing enforces it. Every MQTT client is bound to the synthetic service account
*before* authentication, and `isClientAuthorized` — on a successful dial — replaces
only `c.nrc`. It never derives or compares the authenticated connection's account,
and session/JSA state is still selected through `c.acc`.

So credentials valid in a **different** NATS account authenticate successfully while
every durable operation lands in the service account's streams.

This is kept separate from F-079 rather than folded into it because the mechanism and
the fix differ: F-079 is about observing authorization for a publication, F-080 is
about binding an identity to an account at CONNECT. A fix for either leaves the other
standing.

## F-081

**Title:** A subscribe deny overlapped by a wildcard is not enforced on delivery

- Status: Open. Filed 2026-08-25 by the sweep F-079 triggered.
- Classification: Authorization
- Release treatment: **Alpha blocker.** Sibling of F-079 on the subscribe side.

### The finding

Subscribe authorization is enforced **at SUBSCRIBE time only**: `processSub` issues
a post-`Subscribe` `Flush` and reads `LastError`, so a filter the account denies
outright becomes SUBACK 0x80 (`docs/CONFORMANCE.md`, *Subscribe failure*). That is
real, and it is the whole of the enforcement.

It does not cover a **wildcard subscription that overlaps a narrower deny**. NATS
would filter the denied subjects during delivery on the client's own connection,
and for QoS 0 on a user topic it still does, because that subscription rides
`c.nrc`. But **QoS 1/2 delivery and retained replay ride the service connection**
(`$MQTT.sub.<nuid>`), which holds full rights, so nothing filters them there.

The port inherited upstream's per-message guard for exactly this case — both
delivery sites carry the comment *"A broad wildcard subscription can overlap a
subscribe deny clause"* and test `c.mperms != nil && c.checkDenySub(...)`. **Both
halves are dead:**

- `mperms` is declared on the shim's `client` and has **no writer anywhere** — only
  the field declaration and these two nil-guarded reads.
- `checkDenySub` is a shim stub returning `false` unconditionally.

So the guard is always false and the message is delivered. A client allowed
`foo/#` but denied `foo/secret` receives stored QoS 1/2 and retained messages on
`foo/secret`.

### Relationship to F-079

Same root cause — the broker moved delivery and persistence onto the full-rights
service connection and kept upstream's client-connection permission hooks as inert
stubs — but the opposite direction, and a separate fix. F-079 is about a publish
whose authorization was never observed; F-081 is about a delivery whose denial was
observed at subscribe time and then not applied per message. Neither fix implies
the other.

### What a fix must add

An authenticated end-to-end fixture with a wildcard subscription overlapping a
narrower subscribe deny, asserting non-delivery over **both** the QoS 1/2 path and
retained replay — and, as a control, that the QoS 0 path on a user topic already
filters, so the fixture cannot pass by accident on the wrong mechanism. Then a
per-client subscribe ACL that the service-owned delivery paths can actually
consult, replacing the two dead stubs.
