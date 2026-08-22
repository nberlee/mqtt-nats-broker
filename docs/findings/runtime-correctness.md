# Runtime correctness and liveness findings

These are bounded implementation defects in connection teardown, subscription rollback,
synchronization, and pending-state release.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10.

## F-035

**Title:** Bound every server-initiated courtesy DISCONNECT write

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 35

### Original finding record

- [ ] Every server-initiated COURTESY DISCONNECT write is unbounded, and one of them is
  performed with the ownership gate held. `mqttEnqueueDisconnect` → `queueOutbound` →
  `writeSocket` with a ZERO deadline (`shim_engine.go`), so a client that has stopped
  reading blocks the writer indefinitely: the parse-error footer's 0x89 defers the
  teardown that follows it, the bound settlement's foreign 0x8E pins its own readLoop, and
  the periodic pass's `disposeForeignSessionLocked` performs that write while holding the
  session gate, which blocks a reconnecting CONNECT for as long as the stalled client
  lasts. The DISCONNECT claim above removes a write only when it suppresses one (a second
  notification, or a pre-CONNACK takeover); the first claimant still writes unbounded, so
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

- [ ] Make `readd` restore `sharedSubs` alongside `cons`, or release from both.
  `rollbackSubscribePacket`'s `readd` (`mqtt.go`) restores the packet's added
  `cons` entries and reopens their delivery, but not the `sharedSubs` key, so a
  session can end in a state where a live shared consumer is outside the
  membership. The pending-state-derived purge above is immune, but `clear()`'s
  shared-durable RELEASE still derives from `sharedSubs` alone (`mqtt.go`), so
  such a session never releases the re-added shared consumer's group durable —
  which `sharedDurablesHeldByOthers` then keeps alive for a member that no
  longer exists. Either restore both maps in the rollback, or make the release
  walk both. Found by the plan review of the purge bullet above.

## F-043

**Title:** Fix the removeSessFromFlappers data race

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 43

### Original finding record

- [ ] Fix the `removeSessFromFlappers` data race, with a reproduction first.
  `mqttDeferDisconnectEx` calls `asm.removeSessFromFlappers` WITHOUT holding
  `asm.mu` (`sess_uncertain.go`), while that helper documents "Lock held on entry"
  and deletes from `as.flappers` directly (`mqtt.go`) — and CONNECT reads the same
  map under `asm.mu`. A real pre-existing race at both of that function's call
  sites. Found by the plan review of the `clear()`-recovery item and verified
  against the code; that item's design does not go through
  `mqttDeferDisconnectEx`, so it neither widens nor fixes it. Needs, in this order:
  a `-race` reproduction covering BOTH call sites; a lock-order and deadlock
  analysis for `sess.mu` against `asm.mu`, since the call sites hold or take
  `sess.mu` and the naive fix takes `asm.mu` inside it; and then a focused fix with
  a regression test.

## F-045

**Title:** Release stranded shared pending state when membership is removed

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 45
- Narrowed since: the preserved record below names three stranding paths;
  `purgeReservedSubs` was deleted by [F-057](resolved.md#f-057), so two remain (the
  3.1.1 forced leave and UNSUBSCRIBE's no-`cons` branch). The finding stands
  unchanged on those.

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

- Status: Open. Filed 2026-08-22 by F-034's consequence audit.
- Classification: Runtime correctness
- Release treatment: Post-alpha cleanup, on the bound established below.

### The finding

An outbound QoS 1/2 delivery persists its in-flight register **before** the send, and
the two are adjacent but not atomic: `storeInflightRegister` returns through
`confirmInflightRegister`, which clears `regPending` and so releases the entry for
transmission, and only then does the caller enqueue. A local `clear()` inside that
window erases `pendingPublish` entirely, and `mqttEnqueuePublishMsgTo` then queues the
packet without revalidating the session generation, the client binding, the pending
entry's nonce, or whether the subscription was closed.

### Why it is bounded, and why F-034 did not fix it

Measured against F-034's two owned consequences it is **neither**: the register was
confirmed, so no identifier is freed — the whole identifier space retires with the
incarnation `clear()` ended — and none is reserved, because the entry is gone.

And the exposure is bounded by where the packet can go. The delivery callback captures
the connection object and the enqueue queues onto **that same object**; it never
resolves a destination through the session. A takeover rebinds the *session* to another
client object, it does not retarget the old one. So the packet is either dropped because
that connection is closing, or delivered to the discarded session's **own** old client.
**It cannot reach a different session's client**, which is what keeps this out of the
data-leak class.

An earlier draft of F-034's plan posited that third outcome and made it the one branch
F-034 would fix. Review refuted it on the code, and the whole race was filed here
instead.

### What a fix looks like

An **in-process** enqueue fence: revalidate generation and binding under the lock that
owns them, before queueing. Deliberately **not** durable arbitration — there is no
second party contending for anything here, and inflating an in-process ordering problem
into a durable protocol is the mistake F-034's own review cycle rejected twice.
