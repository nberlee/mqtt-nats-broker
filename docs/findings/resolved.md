# Resolved findings

These are the complete finding records that previously occupied the resolved portion of
roadmap section 0. They remain here as design evidence, including invariants, rejected
approaches, verification, and residual risks.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10 — or, for a finding resolved after the dossiers were split out,
from its own dossier. Each record names its source.

## F-001

**Title:** Remove the accidental review sentence from the README attribution section

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 1

### Original finding record

- [x] Remove the accidental review sentence from the README attribution section.

## F-002

**Title:** Make MQTT TLS startup fail closed

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 2

### Original finding record

- [x] Make MQTT TLS startup fail closed. An invalid or unreadable certificate,
  key, or CA must never result in a plaintext listener.

## F-003

**Title:** Reconcile the README TLS/mTLS feature claim with the missing
certificate-rollover behavior

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 3

### Original finding record

- [x] Reconcile the README TLS/mTLS feature claim with the missing
  certificate-rollover behavior. The README now documents working TLS and mTLS
  and states explicitly that live rollover is not implemented; rollover itself
  remains tracked under "TLS and certificate rollover" below.

## F-004

**Title:** Resolve the MQTT core TODO concerning JetStream domain assignment

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 4

### Original finding record

- [x] Resolve the MQTT core TODO concerning JetStream domain assignment.
  `-js-domain` is now the single validated source of truth (the unreachable
  per-account and server-level branches are gone), and the broker no longer
  fabricates a `ClientInfo.Cluster` from its node name.

## F-005

**Title:** Resolve and document the policy for subscriptions to internal MQTT subjects

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 5
- Superseded in part: the "legacy entries are purged from resumed session records"
  half of the original record was replaced by a fail-closed REFUSAL at the restore
  boundary under [F-057](#f-057). The namespace denial itself is unchanged.

### Original finding record

- [x] Resolve and document the policy for subscriptions to internal MQTT
  subjects. SUBSCRIBE now denies the whole reserved `$MQTT.>` namespace (not
  only the three delivery prefixes), legacy entries are purged from resumed
  session records, reserved Will topics are rejected at CONNECT, and the
  broker's own delivery subscriptions moved to the service connection so a
  full-namespace deny is deployable. Documented under "Reserved `$MQTT.>`
  namespace" in `docs/ARCHITECTURE.md`.

## F-006

**Title:** Replace brittle JetStream error-string matching in the QoS 2 path with a
robust error classification

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 6

### Original finding record

- [x] Replace brittle JetStream error-string matching in the QoS 2 path with a
  robust error classification. `isJSErrCausedBy` matches the JetStream API error
  code structurally and confines the unavoidable cause-text comparison to the
  generic `{err}` errors (`JSStreamStoreFailedF`, `JSStreamMsgDeleteFailedF`),
  which carry their real reason only in `ApiError.Description`; the QoS 2 dedup
  store and the session-record delete now go through it, and `mqttProcessPubRel`
  classifies its hold lookup and hold delete instead of discarding both errors.
  API error identity no longer depends on the rendered `ApiError.Error()` form,
  but a change to the flattened cause wording itself would still defeat
  recognition.

## F-007

**Title:** Close the destructive PUBREL move window

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 7

### Original finding record

- [x] Close the destructive PUBREL move window. The QoS 2 hold record is now a
  two-state machine (`internal/mqtt/qos2_move.go`): a create-only CAS writes
  `held`, the message is delivered onward FIRST, and only then does a CAS at the
  held record's exact sequence replace it with a terminal `delivered` tombstone. A
  PUBREL that finds the tombstone answers `0x00` without re-delivering, for as
  long as the tombstone exists — indefinitely, and independently of any
  deduplication window. Every record carries an immutable `Nmqtt-Q2Gen` generation
  ID; the finalizer reconciles by generation and abandons without mutating when it
  finds a foreign one, so it can never overwrite a newer session generation's
  record on a reused `(client, packet identifier)` subject. A stable
  `Nats-Msg-Id` on `$MQTT_msgs` (whose deduplication window is a nats-server
  default) narrows the remaining window in which a retry duplicates the *stored*
  copy, but correctness does not rest on it. `$MQTT_qos2in` moved from
  `DiscardNew`/`DiscardNewPer` to `DiscardOld` so the CAS is a replacement rather
  than a rejection, and a non-conforming stream now fails startup with recreate
  guidance — **this is not a rolling broker upgrade**. Residual: the live
  core-NATS fan-out is at-least-once across the store-to-commit interval, and the
  duplicate *stored* copy is suppressed only while deduplication survives (not
  across a nats-server process restart). See "Inbound QoS 2 PUBREL is
  at-least-once across the unresolved move interval" in `docs/CONFORMANCE.md`.

## F-008

**Title:** Keep a started QoS 2 delivery with its chosen shared-subscription client

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 8

### Original finding record

- [x] Keep a started QoS 2 delivery with its chosen shared-subscription client.
  Resolved by session-affine ownership (`internal/mqtt/qos2_shared_affine.go`). A
  create-only CAS on the new `$MQTT_qos2assign` stream elects one winner per
  `(group durable, source sequence)` and every contender honors that claim
  **indefinitely**; the message is copied onto the winner's own
  generation-scoped `$MQTT_out` subject and delivered by a durable only that
  session generation can read, so the exchange completes off the queue-delivered
  group durable entirely. The group delivery is acked only once the claim proves
  the copy exists, and that ack is proven by a JetStream double ack before any
  claim is reaped — a queued ack would let a lost ack reopen the assignment.
  Assignment deliberately does **not** rest on the `Nats-Msg-Id` deduplication
  window (finite; using it recreates the violation). Completion facts are
  orthogonal and monotonic, so a discard can never stand in for an unproven
  source ack. Residuals, both stated in `docs/CONFORMANCE.md`: packet-identifier
  continuity across a broker restart or failover (the message stays
  session-affine and is neither lost nor redistributed, but is redelivered with
  DUP=1 and a fresh identifier), and mixed-version operation — the storage schema
  is rolling-compatible, the delivery protocol is not, so affinity holds only once
  every broker that can serve the group is q2aff-aware.

## F-009

**Title:** Arbitrate session teardown across brokers, and stop deriving deterministic
resource names from…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 9

### Original finding record

- [x] Arbitrate session teardown across brokers, and stop deriving deterministic
  resource names from the client hash alone. Found while planning the UNSUBSCRIBE
  drain, which the race would have applied to a drain durable as well. `clear()`
  deleted a session's resources with no cross-broker arbitration — the record
  transaction and `lockSession` are in-process — so after broker A resolved
  against record sequence S, broker B could take the session over and save a newer
  record before A reached its deletions. A then deleted the durables B had just
  restored (the plain ones share their names, since B restores `Cons` verbatim,
  and the PUBREL durable was deterministic) and finally deleted sequence S, which
  `MaxMsgsPer: 1` had already removed — so the delete reported "sequence not
  found", which `clear()` read as success. B was left holding a valid record
  referencing deleted consumers, and on the interest-retained streams the messages
  they held went with them. Not specific to any one path: clean start, session
  expiry and takeover-discard all took it.

  Fixed in two parts, of which the naming one is load-bearing. (1) A persisted
  **session-incarnation generation** now scopes every name that used to derive from
  the client hash alone — the PUBREL durable, subject and delivery subject, the
  inbound QoS 2 hold subjects and their purge filter, and each delayed-Will
  outbox record — so a delete issued by a retired incarnation can only name a
  resource that no longer exists. A claim alone could not have achieved that: a
  nonce re-check before each delete is a check-then-act, and a paused broker can
  verify its claim, let a peer finish the entire teardown and create a fresh
  session, and only then complete its delete. (2) A **teardown claim** on the
  record itself (`Teardown{Origin, Nonce, StartedAt}`, CAS'd at the sequence the
  broker resolved against) keeps two brokers from tearing down and resuming the
  same record at once: a lost claim deletes *nothing*, and the release —
  a sequence-targeted delete — is classified by re-reading rather than assumed
  (absent means done, our nonce means retry, a foreign nonce or no marker means a
  newer owner exists). Every CONNECT that finds a marker **finishes** it and then
  starts fresh with a fresh generation and Session Present false, which also makes
  a crashed teardown self-healing instead of leaking until the orphan sweep.

  Residuals, both in `docs/CONFORMANCE.md`: a session record written before this
  carries no generation and is now **rejected** rather than resumed on hash-only
  names (see the release-claim item below, which is what made a hash-only session
  unsafe rather than merely unprotected); and, like the affine protocol,
  both halves hold only once every broker in the cluster understands them — an
  older broker ignores `Teardown` and would overwrite a marker, and uses the old
  names.

## F-010

**Title:** Decide and test the expected QoS 2 behavior for an unacknowledged PUBLISH
after UNSUBSCRIBE

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 10

### Original finding record

- [x] Decide and test the expected QoS 2 behavior for an unacknowledged
  PUBLISH after UNSUBSCRIBE. Decided and implemented:
  comply with [MQTT-3.10.4-3] for
  plain consumers by **draining** (`internal/mqtt/unsub_drain.go`). The durable
  and its delivery subscription survive the UNSUBSCRIBE while deliveries already
  started remain pending; messages published to the filter afterwards are
  discarded. The drain is a session-record field, so it survives disconnect,
  restart and failover, and it lives until every started delivery completes or the
  session ends.

  Closed by two halves: the plain drain here, and session-affine shared QoS 2 in
  `ee80a5c`. Shared QoS 1 redistribution to another member remains permitted and
  documented rather than changed, and the QoS 1/2 to QoS 0 re-SUBSCRIBE downgrade
  (`execDeferredConsumerDelete`) stays out of scope: it replaces a subscription
  rather than removing one.

  The state machine rests on: persist-before-ack on every claim removal (one
  helper owns every such ack); a durable **PUBREL intent** written *before* the
  store, which is what makes recovery positive rather than a guess; a three-valued
  phase, because "a PUBREL exists" and "the store outcome is unknown" must not
  authorize the same actions; single ownership, so a drained consumer is invisible
  in `sess.cons` and a re-SUBSCRIBE **adopts** it instead of building a second
  durable on the same filter; and a resume **barrier** that reconstructs each
  phase before any source interest exists, so a restart cannot resend the payload
  of an exchange already past PUBREC. A recreated PUBREL is verified by presence at
  its returned sequence, never by the store response, and escapes a poisoned
  deduplication identity by bumping a persisted attempt counter.

  Two previously-disabled assertions in
  `TestMQTTQoS2InflightMsgsDeliveredAfterUnsubscribe` are now enabled, and
  `TestMQTTUnsubscribeWithPendingAcks` is inverted (upstream expects the started
  delivery to be dropped, which [MQTT-3.10.4-3] forbids). Residuals are in
  `docs/CONFORMANCE.md`: an offline backlog on the drained filter, a refusal to
  drain at all where `$MQTT_msgs` carries a stream-level
  `ConsumerLimits.InactiveThreshold` (which no consumer config can escape, so
  UNSUBSCRIBE falls back to today's destructive teardown and warns), a
  best-effort clear of the escapable *per-consumer* inactivity timer whose failure
  leaves the durable reapable while offline (reported as loss, never as
  completion), packet-identifier continuity across a restart, and mixed-version
  operation — an older broker ignores `Drains` and its orphan sweep can delete a
  drained durable.
  **Verification.** The acceptance matrix from the implementation plan is complete
  and every case is mutation-checked — a mutation that breaks the behavior must
  fail its test, or the test is not evidence. Four test files:

  - `unsub_drain_test.go` — fresh-message discard, reconnect survival, adoption and
    its consumer accounting (`consCharged`/`tmaxack` unchanged, no double charge),
    a QoS 0 re-SUBSCRIBE **not** adopting, the withheld-delivery rule (I5),
    `clear()` deleting drain durables, `sweepOrphanedConsumers` treating a drain as
    referenced, offline-backlog discard, a `foo/#` filter draining both its main
    sid and its `fwc` companion, survival of an offline period longer than the
    per-consumer inactivity threshold and that clear retried on resume, refusal
    under a stream-level `ConsumerLimits.InactiveThreshold` with the warning
    asserted and the destructive fallback observed, stage-time rollback (v5 0x83),
    completion-save failure, and a restored claim redelivered with DUP across a
    restart.
  - `unsub_drain_pubrel_test.go` — the combined restart state (no subscription, a
    QoS 2 drain **and** a stored PUBREL); the resume barrier finalizing a stored
    PUBREL with PUBREL delivery deliberately stalled so the source redelivery gets
    its chance first; persist-before-forward proven by PUBCOMP-then-crash;
    recreation from a persisted intent under the original identifier; both
    deduplication-poisoning shapes (record deleted with its entry surviving, and a
    store *skipped* for lack of interest); the barrier failing closed on an
    unreadable scan; definite and ambiguous intent-write failure, the latter
    proving the three-way readback adopts its own record rather than rejecting the
    first same-broker reconnect; and a restored claim carrying a Message Expiry
    Interval redelivered with DUP rather than dropped by the `!started` gate.
  - `unsub_drain_recovery_test.go` (default tree) — reconciliation and record-fence
    rules: an intent-bearing claim is never pruned, barrier-skipped durables are
    left alone, a failed save is undone, the fence opens before any mutation, and a
    message absent above `FirstSeq` is pruned by the exact lookup.
  - `unsub_drain_cluster_test.go` (R3, two brokers) — resume of a PUBLISH-phase
    claim on a different broker; a PUBREC-to-PUBCOMP exchange completed by a broker
    that never saw it, under the **original** packet identifier; and a superseded
    broker's late PUBREL store collapsing into the peer's recreate, so exactly one
    PUBREL reaches the client.

  Writing these, and the reviews around them, found a series of implementation
  defects — all fixed and mutation-pinned.

  In the drain itself: a successful PUBREL **store response** finalized and acked a
  drained source, so on the interest-retained `$MQTT_out` a store with no consumer
  discarded the payload for a PUBREL that was never kept (only a confirmed PUBREL
  may release the claim — I2b, I12); reconciling on `NumAckPending == 0` deleted
  every claim and its durable on **every broker restart**, because a push consumer
  with no subscriber reports zero pending for deliveries merely reset to
  not-yet-delivered; the per-consumer inactivity clear was never retried after a
  failure; and a failed re-SUBSCRIBE both lost the drain (the SUBSCRIBE snapshot
  did not carry it) and DELETED its durable, by counting the adopted consumer among
  the ones that packet had created.

  Around the record fence: definite rollbacks ran *after* `endRecordTx`, leaving a
  window in which another writer could persist the state being undone. And — the
  largest class — **ambiguous** record writes were rolled back in memory as if they
  had definitely failed. An ambiguous write may have landed, so memory must end up
  agreeing with whichever outcome the readback proves: rolling back immediately
  commits to "it did not land" at the one moment nobody can know that, and if it did
  land the next write overwrites it. (The write's identity is safe either way — the
  digest and the exact framed bytes are copied into `pendingWrite` when the failure is
  recorded, so later memory changes cannot alter what the settlement compares against,
  or re-sends.) Keeping the mutation is therefore the whole answer for the state
  itself: the settlement re-attempts those bytes, so the record ends up agreeing with
  memory unless a foreign owner intervened, and `retainOnLanded` registers only what a
  caller withheld for want of PROOF. For a drain claim that distinction is the point:
  an unconfirmed removal treated as durable gets the message **acked away** (I1), or
  lets a forwarded PUBREL complete an exchange the record still owes (I10). A retained
  removal is therefore also marked `unclaimUnconfirmed`, which the two delivery
  callbacks check before doing anything irreversible — the QoS 1/2 callback before
  acking a redelivery, the PUBREL callback before forwarding — because each makes its
  own decision after the helper returns and would otherwise act on in-memory "not
  claimed"; the marker comes off when the settlement PROVES the removal durable, and a
  definite failure restores the claim inside the writer's own transaction.

  Resolution is itself a record-state transition, not a read that observes one: it
  takes the record fence (never waiting — a caller may hold one on the same
  goroutine), decides, clears the uncertainty and runs the retained continuations all
  under it, and re-validates that the attempt it read is still the live one before
  applying anything. Continuations receive the resolver's token and must write under
  it. A **foreign** verdict goes through one ownership-gated disposition that detaches,
  evicts by identity and closes the stale connection, so it can neither damage a
  replacement session a competing CONNECT installed nor leave a live client acting on
  an evicted one.

  Resolution is driven rather than waited for, because an unresolved session cannot
  persist anything at all — every later save is refused — so "wait for the
  disconnect" bounds nothing on a connection that stays open. The periodic resolver
  therefore handles BOUND sessions too, taking the ownership gate as the bound client
  (so a CONNECT still wins), and each ambiguous path also resolves immediately;
  redeliveries of an unconfirmed sequence, on both the source and the PUBREL
  consumer, act as the retry clock when a readback itself fails. A **foreign**
  verdict goes through one ownership-gated disposition: it detaches, evicts by
  identity (never by client ID, or a replacement a competing CONNECT installed is the
  thing removed), cancels the expiry timer only when that removal applies, and closes
  the stale connection — leaving it merely detached would let a live client keep
  publishing and acking against an evicted session.

  A caller that keeps a mutation also owes the SETTLEMENT its **landed** continuation
  wherever its success path does work after the save: UNSUBSCRIBE commits the staged
  drains, clears their inactivity timers, releases shared durables and drops the
  client-facing subscriptions, and skipping that for a write proven durable would
  leave the record saying "unsubscribed" over a half-applied runtime.
  `saveOutcome` distinguishes the
  two, and each of these callers classifies its own write and rolls back only a
  definite failure: the five drain paths (`finalizeDrainDelivery`, `recoverDrains`,
  `recordPubRelIntent`, `bumpDrainPubRelAttempt`, `recoverDrainPubRels`),
  `mqttProcessUnsubs`, the SUBSCRIBE packet's final `update` and
  `rollbackSubscribePacket`, its per-filter shared membership save (which now
  ABANDONS the packet rather than reporting a failure for a membership that is
  durably stored), `rollbackSharedSub` — which is itself a mutate-before-save caller,
  in both its remove-new and restore-prior branches — `purgeReservedSubs`, the
  affine unwind in `processSubs`, `ensureQ2AffineConsumerSubscription`, and the
  3.1.1 forced shared leave.

  That list is the audited set, not a claim about every future caller: the rule is a
  discipline each new mutate-then-save site has to apply, and nothing in the type
  system enforces it. A new one is easy to spot — it mutates session state, calls
  `save`/`update`, and has an error branch that puts the state back.

  Two further findings outside the drain: the displaced-broker Will cancel was
  scoped by session generation alone, but a persistent session KEEPS its generation
  across reconnects — so a later connection's Will could be destroyed; it is now
  bounded by the record sequence the displaced broker knew. And a stored incarnation
  generation was never validated before names and **purge filters** were built from
  it, which a malformed value could have widened across other incarnations.

  Two narrow test seams were added rather than accepting a nondeterministic
  assertion: PUBREL delivery can be stalled (the barrier is otherwise
  indistinguishable from its callback, which always wins the race), and the
  JetStream per-message TTL derived from a Message Expiry Interval can be
  suppressed, since otherwise "expired for delivery" and "still in the stream"
  coexist only during the reaper's lag.

## F-011

**Title:** Serialize session discard against inbound QoS 2 PUBREL release

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 11

### Original finding record

- [x] Serialize session discard against inbound QoS 2 PUBREL release. A PUBREL
  racing a cross-broker session discard could deliver a message from a discarded
  session once (residual 4 of the move protocol), and record that as success: the
  hold record's only arbitration point was the terminal CAS, which runs *after* the
  onward delivery, while the discard side was a blanket `STREAM.PURGE` that competed
  with nothing. A PUBREL that had merely *read* the held record fanned out anyway,
  and its finalizer then read the resulting absence as `Gone` — "the exchange
  completed".

  Fixed by a **release claim** (`internal/mqtt/qos2_release_claim.go`): a create-only
  CAS on a sibling of the hold subject (`…<pi>.rel`), which the releaser writes as
  `releasing` before the onward store and a discard writes as `discarded` before it
  destroys the hold. Exactly one create can succeed, so the loser does nothing
  irreversible. The discard now arbitrates **per subject** — one `STREAM.INFO` with a
  subjects filter, then a claim and an exact-subject purge per hold — instead of
  purging by filter, and reads no payloads at all.

  This item originally prescribed a `held -> releasing` CAS **on the hold record**.
  That was rejected during implementation review, for reasons worth keeping: it
  removes the only code that reclaims `$MQTT_qos2in` while making the record left
  behind payload-sized (the stream forbids every limit that could evict it); adding a
  state to `mqttQoS2State` makes `finalizeQoS2MoveInner`'s `default: Superseded` arm
  reachable, and that arm sends PUBCOMP over a surviving nonterminal record, so an
  omitted switch case costs a message; and re-encoding the record from a
  reconstructed `pp` is a lossy projection that would durably drop properties failing
  `mqttValidateForwardProps` and re-base the Message Expiry Interval on every hop. A
  sibling register leaves the hold record byte-identical for the whole exchange, and
  is the shape the Will outbox, the affine claim and the teardown marker already use.

  The claim alone is not sufficient, and the pairing is the load-bearing part. A
  releaser that stalls after loading the held record while a peer commits the
  tombstone and reaps both records finds an **empty** claim subject, so its
  create-only claim *succeeds* and it would fan out a second time from the payload it
  still holds in memory — a hazard the pre-change code had too. So the claim is
  followed by an authoritative re-read requiring both the original sequence and the
  original generation, in which **every** accepting outcome is generation-qualified:
  a later exchange's tombstone on the same packet identifier is not proof that this
  one completed. Neither fence substitutes for the other — the claim fences a
  *discard*, which a re-read cannot because read-then-fan-out is check-then-act; the
  re-read fences an *already-completed exchange*, which the claim cannot because
  there is nothing left to contend with.

  Also load-bearing: every claim delete targets the **exact sequence** of the claim
  being reaped, or a delayed reaper removes a later exchange's claim on a reused
  identifier; the discard claims strictly **before** it purges, which is what lets a
  releaser holding the claim read absence as "terminated" rather than "discarded";
  a `discarded` marker is reaped by **nobody** at teardown time, since "no releaser is
  still in flight" is not provable; that a session record carrying **no** generation
  is now rejected outright (`mqttSessGenFromRecord`) rather than resumed on hash-only
  names, because such a session cannot be arbitrated and the discard would fall back
  to a blind hash-wide purge that destroys a claimed hold — silent loss, and worse
  than the deviation being fixed, so the whole legacy path and its filters are gone;
  and `Origin` is sanitized and truncated rather
  than derived from the local node name, because names differ per broker and a bound
  taken from one would not hold on the broker that releases. `At` is required but
  never gates readability — the unreadable path drops a message. It originally fell
  back to the record's store time for the then-unwritten orphan sweep; that helper is
  **gone** (see the sweep item below), because no two JetStream timestamps come from
  one clock, so no stored timestamp participates in eligibility at all and `At` is now
  write-only forensic data. A same-broker discard additionally refuses to admit a
  release at all (`closeQoS2ReleaseAndDrain`, mirroring `closeQ2AffAndDrain`),
  closing the local case outright.

  Residual, reworded rather than removed in `docs/CONFORMANCE.md`: a release that
  already won its claim may still fan out after the discard completed. The delivery
  *decision* is ordered against the discard; the wire is not. A discard that observes
  a claim waits briefly, which covers every case where the releasing broker is alive,
  but a broker that dies mid-release cannot be waited out.

  **Verification.** `qos2_release_claim_test.go` (default tree) covers the
  create-only CAS exclusivity as a dependency contract, subject naming, identity
  validation, the `At`/`Origin` rules, both reconciliation tables, non-mutating
  defaults, the arbitration table, the claim-before-purge ordering, that the discard
  loads no hold records, and that a stale reaper cannot remove a newer claim.
  `qos2_release_claim_e2e_test.go` drives real exchanges against a witness
  subscriber; `qos2_move_cluster_test.go` adds two R3 two-broker tests. Every
  assertion was mutation-checked: removing the claim makes the discard test deliver
  the message, removing only the re-read makes the stalled-loader test deliver it
  twice, reaping by subject instead of by sequence destroys a newer claim, and
  widening the unarbitrated purge filters back over the live generation breaks three.

## F-012

**Title:** Reap orphaned inbound QoS 2 hold records, the two structures session-affine
shared QoS 2 adds…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 12

### Original finding record

- [x] Reap orphaned inbound QoS 2 hold records, the two structures session-affine
  shared QoS 2 adds (an `$MQTT_Q2AFF_*` consumer whose session record is gone, and
  an assignment claim whose winner generation no longer exists), and the inbound
  release claims the discard serialization adds. None of them loses a message — a
  stalled claim is honored, not released — but all of them accumulate.

  Implemented as a per-account periodic sweep (`internal/mqtt/orphan_qos2_sweep.go`)
  running beside `sweepSessionExpiries` and `sweepOrphanedConsumers`. See
  "Orphan reconciliation" in `docs/ARCHITECTURE.md`.

  The item's original wording described the symptom as a **wedged packet
  identifier**. That has not been true since incarnation generations landed: a hold
  subject carries its generation and the next CONNECT mints a new one, so a lost
  record wedges nothing. The real symptom is unbounded accumulation, and the
  end-to-end test asserts that instead — retired-generation records gone, a
  concurrently live generation's untouched — because the packet-identifier
  assertion would have passed with the whole sweep deleted.

  Load-bearing, in the order it matters. **A teardown marker is adopted first**:
  a marked record reads as a reference to everything it names, so without that pass
  a never-returning client's QoS 2 state could never become eligible. **Candidates
  are listed before a complete `$MQTT_sess` walk**, with local in-memory sessions
  counted as references; any incomplete or uninterpretable part of that walk aborts
  the whole destructive pass. **Ownership is `(hash, generation)`**, never the hash.
  **The hold is destroyed before its fence**: that ordering is the sole licence for
  ever retiring a `discarded` marker, because a late releaser then re-reads the hold
  as `Gone` instead of `Held`; reversed, it fans the message out a second time. That
  applies to an *unreadable* claim record too — it is every bit as effective a fence,
  since the claim write is a create-only CAS — so it is purged behind and then
  removed at its exact sequence, never deleted first and replaced with a fresh marker.
  **An assignment is committed `Discarded` before its consumer is deleted**, since
  `$MQTT_out` is interest-retained and the delete drops the copies it held.

  **Nothing subtracts two timestamps.** The claim's `At` was rejected because a
  self-reported timestamp from a possibly skewed broker clock cannot be validated —
  and the same objection defeats every other pairing. A JetStream store time comes
  from whichever server led that stream's Raft group at that instant; four streams are
  four groups, and one stream spans several clocks across a leadership change, so a
  fast leader's timestamp minus a slow leader's says nothing about elapsed time.
  Comparing either against the bridge's own `time.Now()` is worse still.

  So a grace is a **stable-observation span** on the sweeping process's monotonic
  clock. A candidate qualifies only once it has been seen, unchanged and
  authoritatively orphaned, for at least that long — and never on the pass that first
  saw it: the first valid observation records the identity and authorizes nothing, and
  a later complete authoritative pass is what may qualify it. Identity changes restart
  the wait (stream sequence for records, durable plus `Created` for consumers, nonce
  for teardown markers), as does the resource becoming referenced again. Failed or
  incomplete scans record nothing and authorize nothing, and the mutation is still
  revalidated at the moment it happens. Stored timestamps are diagnostic only.
  Restarting loses quarantine and therefore delays cleanup, which is the safe
  direction; convergence takes several passes rather than one. The table is bounded
  and **refuses** new entries when full rather than evicting — a deterministic scan
  would otherwise evict each candidate just before its second sighting, so nothing
  past the cap could ever qualify.

  Eight defects three Codex reviews caught after the first implementation, all fixed
  and each pinned by a mutation-checked test: teardown adoption validated the record's
  incarnation generation but not its affine pair, so a record naming a foreign
  `Q2Aff.Durable` would have had that consumer deleted (and its interest-retained
  copies with it) — the validation is now shared with the reference walk rather than
  duplicated; the per-tick mutation budget was charged for already-`Discarded`
  claims even though `advanceQ2AffClaim` writes nothing for them, so an inventory
  headed by a budget's worth of retained-by-design claims starved everything behind
  it; and the quarantine table evicted rather than refused when full. `mqttQoS2RelClaimAt`
  and `mqttQoS2ClaimAtFloor` are **removed** rather than reused: the helper preferred
  a plausible `At` over the record's store time — and neither belongs in this
  decision, since no two JetStream timestamps come from one clock — and
  keeping a range check would have meant putting `At` inside the readability predicate
  it must stay out of (the unreadable disposition drops a message). `At` is now
  write-only forensic data, and scope rule 5 of `qos2_release_claim.go` says so.

  Two grace policies, deliberately different: five minutes for construction-window
  state (teardown markers, holds, assignments, affine consumers) and one hour for
  release claims, which arbitrate against a releaser that may be paused between
  loading a payload and delivering it. No stream gains a `MaxAge`; age eviction
  cannot honour the orderings above.

  The sweep-only affine validator (`validateOrphanQ2AffClaim`) checks every derived
  name, but is deliberately **not** folded into `mqttQ2AffClaimIdentityOK`. That
  predicate gates session resume and `clear()`, where one failure anywhere in an
  account-wide scan aborts the caller — and for `clear()` that fails the discard,
  strands the teardown marker and locks the client ID out of its next CONNECT. Since
  the new checks are derivations from naming helpers, folding them in would turn any
  future change to `mqttQ2AffineNames` into an account-wide teardown outage instead
  of one deferred sweep pass. Changes to those helpers must be coordinated with the
  orphan validator.

  Two new conformance residuals, both recorded in `docs/CONFORMANCE.md`: a releaser
  that preloaded a hold answers `0x00` rather than the marker-backed `0x92` once the
  marker has been retired (nothing delivered, nothing lost, only a more optimistic
  reason code); and once a session record is gone, a genuinely `Lost` affine copy is
  indistinguishable from a permitted session teardown, so reconciliation records
  `Discarded`. `docs/ARCHITECTURE.md`'s `Lost` discriminator is qualified accordingly
  — it is authoritative only while the owning record still exists.

  Also fixed in passing: `mqttSessionExpirySweepInterval` became an atomic accessor.
  Every account manager's sweep goroutine reads it and those goroutines outlive the
  test that created them, so the existing plain-var swap was a latent data race that
  this item's tests made reproducible.

  **Verification.** `orphan_qos2_sweep_test.go` (default tree) covers the strict
  subject/durable parsers, the fail-closed age policy, both graces applied
  independently, the full inbound disposition table, exact-sequence reaping, the
  reference snapshot aborting on six kinds of uninterpretable record, cursor seeding
  and sequential resume, the derived-name validator and its boundary against the
  shared floor, absent-winner reconciliation preserving proven facts, `Lost`
  retention, and the consumer barrier in all four ways it can come up short.
  `orphan_qos2_sweep_e2e_test.go` (`mqttport_fulltests`) drives real exchanges and
  adds five two-broker R3 races: a release claim aging under a releaser parked in its
  load..claim window, a teardown marker adopted while its owner resumes, a session
  resume racing the final revalidation, a late assignment claim written by the other
  broker between the inventory and verification scans, and two sweepers reconciling
  the same state concurrently.

  Mutation-checked rather than assumed. Performing the fence steps in the forbidden
  order must make the stalled releaser observe `mqttQoS2VerifyHeld`; deleting an
  unreadable claim before its hold must trip the delete-ordering hook; and trusting
  the first inventory instead of re-scanning must delete a consumer whose generation
  another broker has just claimed. Each of those was run against the mutated code and
  fails, and the ordering test additionally asserts its own hook fired so it cannot
  pass vacuously.

## F-013

**Title:** Give outbound QoS 1/2 in-flight deliveries a durable identity, so a packet
identifier survives a…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 13

### Original finding record

- [x] Give outbound QoS 1/2 in-flight deliveries a durable identity, so a packet
  identifier survives a broker restart or failover rather than only a same-process
  reconnect. `pendingPublish`/`cpending` were memory-only, so after a restart a started
  delivery was re-offered with DUP set and a **freshly allocated** identifier, and a
  client is entitled to treat that as a new message. This was the single remaining
  cause of the HA exactly-once deviation, and it was never specific to shared
  subscriptions: it applied to every QoS 1/2 outbound path.

  Implemented as an **in-flight register** (`internal/mqtt/inflight_register.go`): one
  small record per started PUBLISH-phase delivery on the new `$MQTT_inflight` stream,
  at `$MQTT.inflight.<idHash>.<gen>.<PI>`, naming the source durable and stream
  sequence. See "Outbound in-flight registers" in `docs/ARCHITECTURE.md`.

  The phase split is what keeps it small. Past PUBREC the correlated PUBREL record on
  `$MQTT_out` already owned the identifier and already survived restarts
  (`pubrel_correlation.go`), so only the PUBLISH phase was missing a durable fact. The
  handoff is the PUBREL store; where both exist at recovery the PUBREL record wins.

  Load-bearing, in the order it matters. **Persist before send**: the PUBLISH is not
  transmitted until the register is confirmed at a sequence, and between allocation and
  confirmation the entry is unsendable by *anyone* — a concurrent redelivery declines
  it, the resume NAK path skips it, `stageDrain` does not enrol it. That is also the
  takeover fence: an identifier the client can be holding implies a confirmed register,
  hence one the next broker's scan sees, so a superseded broker's late write is inert.
  **Reserve before interest**: the barrier runs ahead of the CONNACK, not merely ahead
  of `processSubs`, because a client may publish the instant its CONNACK lands and
  every JetStream round trip in between widens the window in which a QoS 0 subscription
  does not yet exist (adding it after the CONNACK made `TestMQTTPersistedSession` fail
  reliably). **Absence is proven, not inferred**: a register is dropped only where its
  source is acknowledged or authoritatively absent *while its consumer still exists* —
  a source whose consumer is gone was lost rather than finished, so its identifier stays
  reserved, and a record whose BODY is unreadable still reserves the identifier its
  SUBJECT names rather than being deleted as uninterpretable — reading "I cannot
  interpret this" as "nothing is in flight" is how an identifier the client holds gets
  handed to a second message. Two review rounds were needed there: the first
  implementation freed such identifiers, and the fix for that reserved the identifier
  but still let the register's SOURCE redeliver under a fresh one, putting the same
  message in flight under two (two exchanges for one message, at QoS 2). An
  unreadable register therefore also marks the identifier space incomplete — and a
  third round found that the condition has to read what the walk FOUND, not what the
  pass installed: on a same-broker reconnect the session object survives, so the
  reservation is already in place, nothing is installed, and counting installations
  silently lifted the restriction on the second resume. A fourth round replaced the
  duplicate-source rule: two registers naming ONE delivery on different identifiers used
  to converge on the newest, but a stream sequence orders the WRITES and not the sends —
  by R1 each writer confirmed before transmitting, so the later record may belong to the
  earlier send, and restoring it can re-offer under an identifier the client never saw.
  That now fails closed like an unreadable register: every identifier reserved, every
  record kept, the source mapped to none of them, allocation withheld. The same round
  found that `clear()` never reset the restriction, so a reused session object could
  never allocate again — a permanent outbound QoS 1/2 outage for that client — and that
  an unproven source acknowledgement on the PUBREC path had to suppress its source's
  redelivery, which PUBREC leaves unsuppressed: without it the re-offered source re-sends
  a payload already past PUBREC. A FIFTH round rejected the first attempt at that, which
  restored the source's link to its identifier — that works only until PUBCOMP, because
  PUBCOMP removes the pending-PUBREL entry the in-flight guard tests, and from then on
  the payload of a COMPLETED exchange is re-sent. The fact is now recorded against the
  SOURCE (`srcAckOwed`), so it outlives the identifier's bookkeeping: the next delivery
  attempt of a retired source is acknowledged with confirmation and discarded, which
  suppresses the duplicate and settles the message in one step. That is the same shape
  the affine path already uses with its claim's `AffineDone`. A SIXTH round then found
  that a memory-only retirement is lost across a restart — and that leaving the source
  deliberately unacknowledged is what makes that window reachable — so the register now
  carries a `Retired` mark, written in memory first and durably second, and recovery
  routes a retired register into the retirement instead of reserving it as
  PUBLISH-phase state. Without the mark, a resume sees an ordinary register over a live
  source and re-sends a completed payload. A SEVENTH round closed the last gap in that:
  the mark was best-effort, so a write failure was a residual. It is not, because a second
  piece of durable evidence exists — while the exchange's PUBREL RECORD survives, recovery
  reads register-plus-PUBREL as "past PUBREC" and writes the mark itself. So the ordering
  rule is that the second evidence is not destroyed before the first exists: a PUBCOMP does
  not acknowledge its PUBREL away while the retirement it covers is unmarked, and the
  register+PUBREL recovery rule no longer DELETES the register (which would destroy the
  record that has to carry the mark) unless the source is provably complete. The cost of
  withholding is a legitimate PUBREL redelivery, which brings the client's next PUBCOMP
  round to retry. An abandoned register also
  marks its sequence withheld, so the eventual first transmission carries DUP=0
  ([MQTT-3.3.1-1]) rather than inheriting a delivery count above one.
  **Every delete targets the exact sequence**, because the subject is reused
  by later exchanges on the same identifier and only the record's immutable nonce
  proves ownership.

  Two design points were changed during implementation, both because the first answer
  was wrong. A scan that cannot complete originally **denied the CONNECT**; it now
  withholds *allocation* only (`piSpaceIncomplete`), leaving QoS 0 flowing and reserved
  exchanges completing. Allocation is the sole operation an unknown identifier space
  makes unsafe, and denying the connection both takes a session offline over a
  transient failure and contradicts the drain barrier's contract, which withholds a
  durable's interest rather than the connection. And the QoS 2 source ack was described
  as preventing a completed exchange from being re-offered; it does not — a lost source
  ack means JetStream redelivers regardless. It is confirmed (`jsa.confirmAck`) because
  a durable record is deleted on its strength, which buys *continuity*: the register is
  never removed over a source that was not released. What actually stops a completed
  exchange being resurrected is the source-completeness proof above.

  Also closed in passing: a fresh allocation could previously mint an identifier that an
  unreplayed PUBREL record still owned, because `pendingPubRel` was empty until that
  record was delivered. And `dropUnredeliverableRetained` identified retained sends by
  an empty ack subject alone, which a recovered entry also has — it now requires an
  empty durable too, or the barrier's work would be undone on the next reconnect.

  Residuals, all in `docs/CONFORMANCE.md`: retained-message sends (no JetStream
  backing, so nothing to redeliver and no register); promptness after a restart, which
  still waits out `AckWait` because a recovered entry has no per-attempt ack subject to
  NAK; the withheld-allocation degradation above; one identifier reserved for the life
  of the session per delivery lost out of band; and mixed-version operation, since an
  older broker never reads `$MQTT_inflight`.

  **Verification.** `inflight_register_test.go` (default tree) covers naming, the
  strict subject parser, record identity and codec, the derived source stream, the
  validator table (every rejected knob, and the deliberate non-check of
  `ConsumerLimits.InactiveThreshold` — nothing consumes this stream), the store's
  reconcile table (create, ambiguous-resolved-by-nonce, definite failure, a live
  foreign register refused, a provably complete one reaped), the terminal and
  consumer-purge deletes, and the recovery table: identifier restored, completed source
  dropped, consumer-gone identifier retained, PUBREL identifiers reserved with their
  sources, PUBREL-beats-register precedence, duplicate sources keeping the newest, an
  unusable register deleted, in-memory state winning, and the withheld-allocation
  degradation with its recovery. `inflight_register_e2e_test.go` (`mqttport_fulltests`)
  drives real clients across a real restart: QoS 1 and QoS 2 identifiers surviving,
  the PUBREL reservation blocking a colliding allocation, a stale register leaving no
  phantom Receive Maximum slot, and a retained send writing no register.
  A late round corrected a mistake of my own worth recording, because it was tempting
  and wrong: a delivery whose consumer had vanished was briefly exempted from Receive
  Maximum, on the reasoning that it could never complete and so should not hold a slot
  forever. But that limit bounds what the CLIENT holds unacknowledged, and a consumer
  disappearing proves nothing about the client — the exemption let the broker exceed the
  negotiated limit. Only a delivery this session provably gave up may free anything.

  The review rounds that followed kept finding gaps in one failure mode — a register
  outliving the consumer that would classify its source — and the eventual fix was to
  remove a mechanism rather than add one. An intermediate design carried a
  sequence-scoped delete obligation (`OwedRegs`) beside the evidence, and about half the
  findings were against it; it never shipped and is gone. What ships is two durable
  homes for one fact, chosen because they fail in different windows: a **cancellation
  tombstone** on the register itself, which needs no record fence and so covers a
  cancellation that lands while the store's CAS is still on the wire, and a
  **void-register verdict** — the register's NONCE in the session record's
  `CancelledRegs` — staged by the same transaction that removes the consumer's
  membership, so the write that abandons the delivery is the write that says so.
  Nonce-keyed because every other identity is recycled (the identifier is freed for
  reuse, subjects are reused within a generation, shared group durables return on a
  rejoin under the same deterministic name); durable-scoped verdicts were tried and
  either resurrected an abandoned register or condemned a genuine new one. Verdict
  durability is a tracked revision retried at every convergence point, since the
  recording goroutine is often refused the fence by the very packet that caused the
  cancellation; a verdict is retired only against proof (the delete confirmed, the
  tombstone landed, or a complete scan finding no register carrying it), which also
  bounds `CancelledRegs` across a client that subscribes and unsubscribes forever.

  Teardown itself walks one ladder — confirmed delete, then tombstone at the exact
  sequence, then persisted verdict — before the consumer is deleted, with two deliberate
  exceptions. A RETIRED QoS 2 register is delete-or-leave: it is the durable suppression
  of a completed-but-unproven exchange, and a tombstone or verdict would have recovery
  drop it without reinstating the suppression, resending the completed payload if the
  crash preceded the consumer delete. And a withdrawn allocation's identifier is
  released only SETTLED and PROVEN: before the store settles, a new allocation could
  race the old CAS onto the same subject; before some rung is proven, a crash could
  resurrect the register as an ordinary delivery holding a Receive Maximum slot forever.
  The staging is preceded by the delivery barrier (closed under the same lock the
  snapshot is taken with, reopened per filter on rollback, both halves of a "foo/#"
  filter through one helper), rollback withdraws the verdicts of transmitted deliveries
  — they are live again and still owe their identifiers — while cancelled allocations
  stay void on every branch, and a surviving disconnect's shared purge walks the same
  ladder without ever deleting a consumer. A panic was fixed on the way: the cleanup
  helpers log on failure paths, and that shared purge calls them with no bound client.

  Two further review rounds hardened the lifecycle rather than the protocol, and their
  findings are worth naming because each is a way for register ownership to be silently
  dropped: a **drained** QoS 2 exchange now records its register's sequence in its own
  persisted state and releases it in its finalizer (nothing else can, since its claim is
  held until the PUBREL is proven), with the retry accepting both "already absent" answers
  so a crash between delete and save cannot withhold completion forever; an UNSUBSCRIBE
  racing a first-delivery register write **cancels** that allocation rather than skipping
  it, records it as withheld so a rolled-back save keeps DUP=0, and its register removal is
  confirmed, with the verdict standing until it is proven; a consumer teardown releases
  its **retired** sources, which are not in `cpending` at all; and where several registers
  name one drained delivery — a superseded broker's second write — recovery fails closed
  rather than releasing one and orphaning the rest. Resume also stopped reading
  one-record-at-a-time: it enumerates subjects in one request and loads registers,
  consumer info and source messages in bounded batches, because the barrier runs before the
  CONNACK and a session may hold up to 65_535 identifiers. Batching had to be taught the
  same distinction the serial path already made — only "no message found" proves absence,
  and one shared classifier now enforces it for both batched paths.

  `inflight_register_cluster_test.go` adds the case the item is actually about — a
  client resuming on a **different** broker of a real R3 cluster, which has none of the
  first one's memory, for QoS 1 and QoS 2 — plus an always-run assertion that
  `$MQTT_inflight` really is `Replicas=3` and current on every node, without which
  those two could pass against a single-replica stream and prove nothing about HA.

  Eighty-four mutations were run against the code and each fails its test: skipping the
  recovery barrier, removing the register write, dropping the PUBREL reservation loop,
  removing the source-completeness proof, reverting the retained-send predicate,
  removing the `regPending` send fence, removing the teardown purge, making
  `bumpPI` ignore the incomplete-space flag, deleting an unreadable register instead of
  reserving its identifier, letting an unreadable register leave allocation enabled,
  governing that restriction by installations rather than findings, dropping the
  register-sequence exclusion from `dropUnredeliverableRetained`, converging duplicate
  registers on the newest instead of failing closed, dropping the `clear()` reset,
  dropping the source retirement on an unproven acknowledgement, keying that
  retirement by identifier rather than by source, making the durable `Retired` mark a
  no-op, having recovery ignore that mark, acknowledging a PUBREL away while its
  retirement is unmarked, having recovery delete a register a surviving PUBREL still needs,
  and dropping the withheld marker when abandoning a register.
  The stabilization pass re-ran every mutation touching the reworked teardown functions
  and added its own: recovery ignoring the nonce verdicts, the record not carrying them,
  the staging voiding nothing, each rung of the ladder skipped (the double-failure
  verdict, the tombstone's forget, the proven delete's forget), the busy-fence and
  unlanded-save persistence paths, the rollback unvoid as a no-op, the delivery barrier
  not closed and not reopened, the settled-release guard dropped and the settle-point
  release made unconditional, a retired register tombstoned like any other, the shared
  purge deleting its consumer, and the recovery drop keyed by durable instead of nonce —
  the durable-scoped mistake, kept as a mutation so it cannot come back quietly.

  The holistic review then found three races the matrix had missed, each fixed and
  mutation-pinned: a landed save released reservations for verdicts a concurrent
  teardown recorded after its marshal (verdicts now carry their insertion revision); the
  scan retired absent verdicts that a superseded broker's late CAS could still
  contradict (absence now retires nothing — the verdict waits for its register to be
  seen and deleted, and only the provably-no-CAS path retires its own); and a securing
  tombstone's proof could park its reservation forever, because the settle point
  recognised only save-keyed proof. A stop-time review added a fourth: a STALE store's
  settle — stalled across its allocation's cancellation, release, and the identifier's
  reuse — could release the newer allocation's reservation and forget its verdict while
  that allocation's CAS was still on the wire; every reservation touch is now keyed by
  the toucher's own nonce, read under the lock that allocated it. And a fifth: release
  had to become order-independent between proof and settlement, since a verdict durable
  before the settle leaves no later save to release, while a stream-rung proof after the
  settle retires the membership a save-keyed release would need — one predicate now
  serves every edge, and retirement itself releases the settled reservations, through a
  small nonce-keyed index rather than a pendingPublish walk (once per voided register
  against a map that can hold tens of thousands of live deliveries is quadratic
  teardown work). A verdict whose register is found TOMBSTONED is likewise retired
  against that register's confirmed deletion — the register is proof in hand, so the
  keep-on-absence policy does not apply, and keeping it would leak one record entry per
  crash between the tombstone rung and the forget's save. What remains unreclaimable is
  the sliver where the register was confirmed-DELETED and the broker crashed before the
  very next session save: retiring that verdict on scan-time absence would need a fence
  against a superseded broker's late CAS, and a time-based one would rest a
  release-blocking invariant on a timing assumption. One nonce per such crash, stated as
  the residual.
  Three tests were rewritten *because* a
  mutation exposed them as vacuous: the restart tests originally used packet identifier
  1, which a fresh allocator produces anyway; the completed-exchange test asserted
  a silence that JetStream guarantees on its own; and the unproven-acknowledgement test
  first scanned every consumer's correlation, so it passed on the PUBREL's own entry
  while the SOURCE's was missing (and was then rewritten again when the mechanism moved
  from the identifier to the source, and a third time when the retirement turned out to
  be transient by design — the settling redelivery removes it — so asserting on it needed
  the default AckWait rather than a shortened one, with the redelivery itself covered by
  the restart test).

## F-014

**Title:** Audit the remaining $MQTT_* streams for protocol prerequisites, deriving each
list from what…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 14

### Original finding record

- [x] Audit the remaining `$MQTT_*` streams for protocol prerequisites, deriving
  each list from what that stream's owner actually does rather than copying
  `validateQoS2InStream` wholesale. **Done** — all nine broker-owned streams now
  validate at startup, provisioned through one convergence shape
  (`ensureBrokerStream`: create-or-lose → unconditional authoritative re-read →
  per-owner validation) driven by an ordered spec table no stream can bypass.
  New validators: `validateSessStream` (including the incarnation epoch,
  composed onto the same snapshot — `resolveSessStreamEpoch` is gone),
  `validateWillQStream`, `validateMsgsStream`, `validateRetainedStream`;
  `validateWillCancelStream` completed beyond state shape via the shared
  `validateBrokerOwnedStreamShape`, which also rejects `AllowRollup`
  (reviewer-caught: a rollup header purges up to the whole stream on a plain
  publish — the evict-live-state hazard class every owner already rejects in
  global limits). Non-prerequisite findings, waived with
  written reasons and pinned by acceptance tests: `DenyDelete` on
  `$MQTT_willcancel` (never deletes) **and on `$MQTT_rmsgs`** (deletes are
  best-effort compaction; the empty-body tombstone + `MaxMsgsPer:1` replacement
  carry correctness), `Sources` on `$MQTT_rmsgs` (supported cross-account
  retained topology), and `ConsumerLimits.InactiveThreshold` on `$MQTT_msgs`
  (the drain and shared/affine RUNTIME refusals remain the authority — that
  prerequisite was already delivered by `ab683d9`, and the acceptance test now
  pins it as a non-startup-failure). The `$MQTT_qos2in` list was re-derived for
  the `<idHash>.<gen>.<PI>` + `.rel` layout: fields unchanged, two stale
  rationales corrected (the purge `DenyPurge` protects is `purgeQoS2Hold`'s
  confirmed exact-subject purge; the transform hazard now includes the claim
  sibling and walk filter). Two existing-code gaps closed:
  `validateOutStream` gained `ConsumerLimits.MaxAckPending` (fatal only when
  non-zero and below the broker's effective cap; boundary pinned) and
  `MaxConsumers > 0`. `$MQTT_msgs`'s `MaxMsgSize` stays a
  blanket rejection, and the review tested it in both directions: one round
  proposed a threshold at the broker's max payload (the PUBLISH admission
  preflight bounds the complete stored form by it, and JetStream's cap
  compares the same quantity), the next round disproved the threshold — QoS
  1/2 Will stores bypass the preflight (`mqttHandleWill` /
  `deliverWillRecord` call `mqttInitiateMsgDelivery` directly), so a cap at
  the bound can still silently lose an immediate Will and no positive cap is
  provably inert. Two user-approved scope
  additions: the `$MQTT_out` fix
  above, and deletion of the pre-release migration/degrade machinery (the
  pre-2.10 session-stream and pre-2.10.10 retained-message transfers, the
  `AllowMsgTTL` degrade mode — `AllowMsgTTL` is now a prerequisite of
  `$MQTT_msgs`/`$MQTT_rmsgs` on brokers accepting MQTT 5.0, the default; a
  `max_protocol_version: 4` broker never emits `Nats-TTL` and waives it) under
  the compatibility boundary: "there is no release yet, so no migration
  needed". `TestStreamValidatorFieldCoverage`
  walks every `StreamConfig` field by reflection against every validator
  (bijection with the mutator table, exactly one disposition per pair, waivers
  must be accepted, reasons must be non-empty). Residual, stated in
  `docs/CONFORMANCE.md`: startup validation is a snapshot — post-start config
  mutation is not re-checked until the next initialization; a nonconforming
  stream splits a mixed fleet and recovery is stop-all, per the
  `$MQTT_qos2in` precedent. See "Stream prerequisites" in
  `docs/ARCHITECTURE.md`.

## F-015

**Title:** Verify (or recreate) plain QoS 1/2 durables on session resume instead of
trusting the persisted…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 15

### Original finding record

- [x] Verify (or recreate) plain QoS 1/2 durables on session resume instead of
  trusting the persisted record. Found by the stream-prerequisite audit's
  review (round 3): a plain subscription's durable reaped by an inactivity
  threshold — the broker's own `consumer_inactive_threshold` option or a
  stream-forced `ConsumerLimits.InactiveThreshold` on `$MQTT_msgs`, both of
  which reach consumers that request 0 (`server/consumer.go:658-668`) — was
  silently trusted on resume, so the subscription was dead until a clean
  start. DONE (2026-08-01, 15 plan-review rounds): a dedicated pre-CONNACK
  resume phase (`sess_resume_reconcile.go`) collects every plain QoS 1/2 sid
  (level-up companions and absent-identity minting included), installs
  closed, flushed, phase-owned delivery subscriptions **before** the bounded
  batched verification (interest-before-lookup closes the present-consumer
  TOCTOU), repairs authoritative absence under a **fresh identity** (never
  the recorded name — a same-name recreation flips *Lost* registers to false
  *Complete* via the ack floor and reanimates stale ack/NAK subjects;
  subscribe-first; stale charge permanently retired), and persists the
  repaired identity in one save that is the first record write after the
  accept save by construction — 10071 is foreign
  (`disposeForeignSessionLocked`, DISCONNECT suppressed pre-CONNACK), 10164
  gets a bounded same-base/same-body re-read-and-retry with sequence
  adoption, ambiguity refuses with both continuations registered.
  Restoration adopts the phase's subscriptions **in place**
  (`processJSConsumer` restore exists-branch); a post-`processSubs` sweep
  plus a deferred fallback reclaims whatever restoration skipped. Every
  pre-CONNACK refusal goes through `mqttAbortAcceptedConnect` (no Will, no
  pre-CONNACK DISCONNECT; `sendConnAck` now surfaces its write error). The
  scope is deliberately **CONNECT-only** (user-directed after review rounds
  3-7 dismantled every repair-inside-SUBSCRIBE design against the
  `processSubs` write surface): live re-SUBSCRIBE is not a recovery
  mechanism, and broader re-SUBSCRIBE repair is a possible follow-up, not an
  omission. Residuals stated in `docs/CONFORMANCE.md` (no guaranteed backlog
  across a reap; loss surfaces as reserved identifiers, never completion;
  indeterminate verification refuses the CONNECT; mid-connection out-of-band
  deletion undetected; mixed-version blind trust; adopted-drain and
  restoration-skip staleness until the next CONNECT; stale unmatched `Cons`
  entries unverified). Mechanism documented in "Plain QoS 1/2 consumer
  reconciliation on resume" in `docs/ARCHITECTURE.md`.

## F-016

**Title:** Make purgeSharedPending reach a RECORD-RESTORED session's shared state

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 16

### Original finding record

- [x] Make `purgeSharedPending` reach a RECORD-RESTORED session's shared state.
  Found by the resume-reconciliation review (round 5). It enumerated
  `sess.cons` (`mqtt.go`), but shared group consumer configs are deliberately
  excluded from the persisted `Cons` (`buildPersistedSessionLocked`) and are
  rebuilt only by `processSubs`, so for a session restored from its record the
  purge walked nothing and returned having freed nothing. Meanwhile
  `recoverInflightRegisters` reconstructs `pendingPublish`/`cpending` for those
  same shared deliveries straight from the durable registers, so a session that
  disconnected (or was refused) before subscription restoration kept in-memory
  pending entries the purge was supposed to drop — leaking their packet
  identifiers and Receive Maximum slots, and risking a resume that NAK-steals
  group messages AckWait would have redelivered to other members. Pre-existing
  and independent of the reconciliation phase: the ordinary close path had the
  same blind spot. Spec5 [4.8.2].
  **Both derivations from a view of the group were wrong**, and the plan review
  proved each in turn. This bullet's own suggestion — derive from `sharedSubs`
  via `mqttSharedDurablesForKey` — reopens what the `cons` walk did cover:
  `rollbackSubscribePacket`'s `readd` restores the packet's `cons` entries and
  reopens their delivery but does NOT restore the `sharedSubs` key, so a live
  shared consumer sits outside the membership. The **union** of the two views
  then still misses the no-`cons` membership removals: the 3.1.1 forced leave
  (`mqtt.go`) runs before `processSubs`, nils `sharedSubs`, and stages only
  register verdicts — `voidRegistrationsLocked` deliberately leaves a
  TRANSMITTED delivery's `cpending`/`pendingPublish` entries "for the teardown's
  purge to remove" — and UNSUBSCRIBE's no-`cons` branch and `purgeReservedSubs`
  leave the same shape, after which BOTH views are empty while the pending maps
  still hold the state. Two rounds each finding one more view the previous
  view-set missed is the delete-the-redundancy signal, so the fix takes one rule
  instead of a third view: **the purge derives its targets from the pending
  state it exists to drop** — every durable named by `cpending`, `deferred` or
  `srcAckOwed` whose name carries `mqttSharedConsumerDurablePrefix`. That is a
  superset of the old behavior (`cons` itself holds no pending state) and it
  covers all four shapes. The name is a sound discriminator because
  `mqttSharedNames` is the single source of the prefix and no other durable
  class can collide; qualification stated in `docs/ARCHITECTURE.md`: exclusive
  for broker-minted state, trusted for state restored from a register.
  Mutations run, each confirmed to fail its paired test and reverted: revert to
  the `cons` walk (M1), delete the group consumers (M2), skip
  `deleteInflightRegisters` (M3), drop the prefix filter (M4), skip the
  `srcAckOwed` keys (M5), derive from `sharedSubs` membership (M6), skip the
  `deferred` keys (M7). Residuals stated in `docs/CONFORMANCE.md` (a crash
  between the in-memory purge and the register deletes loses that close's purge;
  recovery's opaque/duplicate-source reservations carry no durable and are not
  purged). Mechanism documented in "Outbound in-flight registers" in
  `docs/ARCHITECTURE.md`.

  The surviving-close purge is also arbitrated against cross-broker takeover.
  Every session *object* carries an unpersisted register claim, and every register
  write stamps it. During the pre-CONNACK barrier a resumed object CAS-claims each
  readable live or retired shared register at the exact sequence it loaded, in a
  bounded concurrent window, and installs only the resulting sequence — BEFORE the
  duplicate-source grouping, because a duplicate group fails closed as reservations
  and a reservation protects nothing while the registers behind it still sit at
  sequences a stale close knows: the close would delete one member and the next
  resume would install the survivor as authoritative, resolving by deletion the
  ambiguity the group refused to guess about (a review round found exactly that).
  A live member whose claim FAILS (a concurrent resumer owns it, or it is
  unreadable) keeps its source identity and poisons its whole group — every member
  reserved, none installed — because installing the sibling as a singleton would
  resolve the duplicate by the same guess through the split-claim door (the next
  review round found exactly that). And the negative dispositions reconcile a
  REUSED object's memory: "in-memory state wins" is the right install rule, but a
  consumed close decision retires the live entry it backed, and a poisoned group
  demotes a live member's entry to a bare reservation and removes the source
  mapping — nonce-guarded, so a newer allocation is never disturbed — or a
  same-object barrier re-run keeps delivering under a mapping the arbitration
  just refused to trust (the round after found exactly that). A reservation left
  by a LOST arbitration is marked FOREIGN: its sequence may be the rival's, so no
  terminal path settles the record behind it — the register delete on the
  completion path carries no nonce or claim check, and without the mark a late
  PUBACK on the stale object would destroy the rival's only durable identity
  (the round after that found exactly that). The acknowledgement paths decline a
  foreign reservation outright: a successful PUBREC would otherwise convert it
  into an UNCORRELATED durable PUBREL (the bodyless entry has no source), which a
  later resume reads as identifier-first against the rival's still-live register
  — QoS 2 duplication either way the conflict resolves (the round after that
  found exactly that; the reservation stands and the owner answers the
  retransmission). And a READABLE live record under an ACTIVE rival's claim is not
  an ambiguity at all: the rival claims only after its own accept save, so
  observing it mid-barrier is durable displacement proof, and the resume is
  REFUSED outright rather than binding a client to a superseded session whose
  foreign reservations would decline acknowledgements forever with only a
  missable takeover notification left to evict it (the final review round found
  exactly that). A dead rival's standing claim never trips this — the ordinary
  CAS at the loaded sequence re-claims it — so foreign reservations arise only
  from UNREADABLE records, which prove nothing and stay fail-closed. And the
  claims are ORDERED, or two simultaneous resumers could split a pair of
  registers and refuse each other forever: each claim carries its accept save's
  record sequence (unique by record CAS), a contested claim from an OLDER accept
  is re-targeted and taken over, and only a not-older one displaces — exactly one
  side of any race is newer, so the contest terminates deterministically (the
  round after found exactly that). The contest budget is generous and its
  exhaustion reads as DISPLACEMENT, never as an opaque reservation: contenders
  are unbounded while any fixed budget is not, and exhausting behind a chain of
  older claims would otherwise strand the rightful NEWEST owner with its
  allocation withheld — a refused resume retries into a settled contest instead
  (the round after that found exactly that). There is deliberately NO own-claim
  fast path: every shared register is rewritten on every barrier run, because
  moving the sequence is itself load-bearing — a terminal path that untracked
  its entry and paused before its durable half holds the OLD sequence, and a
  same-object rebind would otherwise reinstall an entry whose exchange the
  paused delete-and-ack then completes invisibly, a permanently consumed
  Receive Maximum slot. Rewritten, the paused delete no-ops, and registers
  dropped on PROOF OF COMPLETION reconcile memory exactly as consumed closes do
  (cancelled reservations exempt: they are held until settled AND proven), so
  the phantom heals at the next barrier run (the final round found exactly
  that). And an own-claim re-read proves the rewrite only when the sequence
  MOVED: with no fast path, the loaded record was own-claimed to begin with, so
  a CAS that failed without landing re-reads the same record at the unmoved
  sequence — adopting it would hand the paused delete its target back. Unmoved
  means retry; the move is the fence — and retry EXHAUSTION refuses the resume
  rather than falling to an opaque reservation, which would sit at exactly the
  unfenced sequence a paused terminal delete may hold (the round after found
  exactly that: a persistent store failure must cost a refused CONNECT whose
  retry meets a working store, never a permanent Receive Maximum slot — and an
  INDETERMINATE re-read after a failed claim CAS refuses for the same reason,
  since sequence movement then cannot be proven either way; the round after
  found exactly that).
  That rewrite invalidates the sequence a stale close knows. Conversely, if the
  close's delete or matching-nonce tombstone won first, recovery consumes that
  durable decision and
  frees the identifier. Every nonce-keyed reconcile that could otherwise teach a
  stale object the moved sequence or let it rewrite the record (store, retirement,
  cancellation and pending cancellation) additionally requires the current object's
  claim — and so does the one reconcile the nonce gates cannot cover: the store's
  same-delivery replacement, whose occupant carries the OTHER writer's nonce, and
  which would otherwise take the slot back at the claimed sequence in the
  three-owner interleaving (a review round found exactly that). The complete-source
  reap is deliberately not gated: a provably complete source means the exchange
  terminated, and identifier continuity protects only incomplete exchanges. Plain
  registers are deliberately outside the gate because they are not claimed on
  resume. The ordinary exact-sequence delete needs no extra read: the claim rewrite
  itself is its fence. The claim is object identity rather than the persisted
  session generation, because that generation correctly survives a resume and
  therefore cannot distinguish the two owners. Each register's close decision is
  durable and consumed exactly once at its own subject sequence.

  Mutations run for the arbitration, each confirmed to fail its paired test and
  reverted: remove the claim pass (M9), reconcile cancellation on nonce alone (M10),
  keep the pre-claim sequence in memory (M11), reconcile a stalled store on nonce
  alone (M12), reconcile retirement without the claim (M13), reconcile pending
  cancellation without the claim (M14), reserve rather than drop a register whose
  claim CAS re-read finds absent (M15), replace a same-delivery occupant without
  the claim (M16), leave duplicate-source group members unclaimed (M17), strip a
  claim-failed member to a bodyless reservation before the grouping sees it
  (M18), skip the consumed-close memory reconciliation (M19), skip the
  poisoned-group demotion (M20), drop the foreign gate from the terminal
  register delete (M21), drop the foreign decline from the acknowledgement
  path (M22), classify a displaced result as opaque instead of refusing the
  resume (M23), treat an older accept's claim as displacement instead of
  taking it over (M24), cap the contest at three attempts with exhaustion
  falling to an opaque reservation (M25), restore the own-claim fast path so
  the terminal race lands its delete (M26), skip the complete-source memory
  reconciliation (M27), adopt an own-claim re-read at the unmoved sequence
  (M28), let unfenced-claim exhaustion fall to an opaque reservation (M29), and
  let an indeterminate claim re-read fall to an opaque reservation (M30). M1-M7
  were re-run against the combined tree.
  Residuals remain stated in `docs/CONFORMANCE.md`: unreadable registers cannot be
  claimed and the ordinary-close in-memory/delete crash window is unchanged. The
  deferred close now runs this purge (next bullet). Three follow-ups it deliberately
  left are the bullets below.

## F-017

**Title:** Call purgeSharedPending from the deferred-disconnect close path — as ONE
classified operation…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 17

### Original finding record

- [x] Call `purgeSharedPending` from the deferred-disconnect close path — as ONE
  classified operation with that path's own record write, not as a bare call.
  DONE. `applyDeferredDisconnect`'s session-survives branch (`sess_uncertain.go`)
  now opens the record transaction BEFORE its first mutation, stamps the ORIGINAL
  disconnect time, runs a purge variant that only STAGES (the register-stream
  rungs plus the nonce in memory — the two cleanup ladders were split into
  stream-only cores, `voidRegistersOnStream` and
  `tombstoneCancellationsOnStream`, so who writes the record is the caller's
  business), and makes ONE `saveClassified` carrying the deadline and the
  fallback verdicts together. An ambiguous save needs no continuation: the deadline
  and the verdicts are in the bytes it attempted, and the settlement re-attempts
  exactly those and credits the revision they marshalled (the void-verdict re-save
  continuation this item introduced is deleted with the not-landed verdict itself);
  a definite failure keeps the in-memory purge and the armed timer. Since the action is CONSUMED
  before the discharge runs, the discharge also re-checks that the record state is
  still resolved (once on entry, once with its own fence held) and RE-PARKS the
  untouched action if an off-readLoop writer went ambiguous in between — otherwise
  every write is refused with nothing left holding the original deadlines.
  Two mechanisms fell out of the review rounds and replaced the per-branch
  retries the first design had. **Every landed record write credits the
  void-register revision it MARSHALLED** (`saveWithDetail` samples it in the
  marshal's own critical section; the sample travels in `mqttPendingWrite` and
  `mqttSessSaveDetail`, so every settlement that proves those bytes durable — a read
  finding them at a newer sequence, the resolver's own re-attempt landing them — and
  each landed classification of `resolveContendedRecordSave` credit it too), never on
  an unresolved, definitely-failed or foreign outcome, with the accept ROLLBACK's raw
  write a stated exemption. And **the release handoff** owns void-verdict persistence
  liveness for UNBOUND sessions: the record-fence release (decided inside its own
  `sess.mu` critical section) and both ownership-gate release forms
  (`unlockSession` and CONNECT's inline delete) re-check what was staged while
  they were held, and attempt persistence under the gate, taken fail-fast, with a
  watermark that advances only when an attempt ACQUIRES the fence, plus a periodic
  BACKSTOP that re-arms that watermark for a resident unbound session whose store
  failed definitely — without it the watermark itself ended the sequence and a
  never-expiring session could keep the verdict memory-only indefinitely. Bound
  sessions keep converging through their packet tails. The resolver also re-checks at BOTH
  levels and reports `mqttUncertainUnresolved` when a continuation or a discharge
  re-armed uncertainty, which is what stops a CONNECT from binding on a stale
  "ours" and evicting the object holding the continuation.
  Twenty-five native tests (`sess_deferred_purge_test.go`) plus one end-to-end test
  through a real reconnect (`inflight_register_e2e_test.go`, in the ported suite: a
  surviving close that can persist nothing, then a CONNECT whose accept save is the
  first landed write and must carry AND credit the inherited verdict): the purge
  itself, the
  bullet's own one-landed-write case with restart survival, the never-reached-the-stream
  case settled by the re-attempt, the ambiguous re-attempt that keeps its identity,
  the definite-failure convergence, a stale close
  fenced by a register claim, a real late settle converging at the gate release,
  the ambiguous-plus-late-staging arms, successive-staging convergence and its
  termination, the fence-losing race, the bound-session and generation-reuse
  scoping, the gate-yield, the helper's resident/nonresident contract, both
  resolver re-check sites, the inherited-verdict credit, four contended-save
  credit variants, the rollback exemption, both re-park windows (a discharge that
  finds the state unresolved again at either of ITS two checks, and a discard
  refused at either of `clear`'s), the contention hand-back, and the periodic
  backstop with its evicted-snapshot arm. Mutations killed by their paired
  test: MU1 (the bare purge), MU2 (no purge), MU3 (stamp the disconnect time after
  the save), MU4 (no continuation), MU5 (a plain re-save), MU6/MU8/MU9 (each
  resolver re-check, MU8 and MU9 independently), MU7 (no fence-release trigger),
  MU10 (watermark burned at decision time) and its inverse (unconditional
  re-trigger), MU11a (no `unlockSession` trigger), MU13c (the unbound scoping gone
  from both guards), MU14 (no watermark reset in `clear()`), MU15/MU15b (each credit
  site), MU16 (the contended-save credits), MU17 (an ungated attempt) and MU18 (the
  rollback credits), MU19 (no unresolved re-check at the discharge), MU20/MU20b
  (`clear` without its in-fence re-check, and a discharge that does not opt in;
  MU20b is no longer expressible, the opt-in flag having been deleted by the
  late-uncertainty-recovery item below),
  MU21 (no re-park on the late-refusal sentinel), MU22 (the entry refusal drops
  that sentinel), MU23 (the handoff recurses through its own releases instead of
  looping), MU24/MU24b (a contended attempt hands its loop ownership back outside
  the gate's critical section, or never) and MU25/MU25b (no periodic backstop, and
  a backstop that does not re-arm the watermark), MU26 (the handoff gate does not
  check residency), MU19b (no in-fence re-check in the surviving discharge) and
  MU13d (the unbound rule removed from both its homes, caught in the rebind
  window) and MU27 (the Will's fate decided from the entry sample of the clock
  rather than a fresh one) — the last fourteen pin the twelve findings the holistic
  review rounds produced. THREE survive, honestly, and each for a stated reason.
  MU11b, dropping only
  CONNECT's inline release call, because round 14 established no reachable path
  leaves a resident unbound session with a gap there (defensive wiring, tested at
  the helper); MU12, sampling the fence-release decision after the unlock, which
  is observationally equivalent because the revision gap is monotonic and every
  other writer's release re-checks (the in-section sample is kept as the
  discipline, not because a test distinguishes it); and MU13/MU13b, each unbound
  guard alone. The unbound rule has two homes — one shared predicate, consulted
  both by the trigger and by the attempt's own re-validation under the gate, and
  the gate CLAIM itself, made as NOBODY so it decides against fresh state rather
  than a pre-gate sample. Either home enforces the rule, so removing one is
  unobservable; removing BOTH is killed twice over, by T9 for a session already
  bound when the trigger fires (MU13c) and by T18 for a rebind that lands in the
  window between the sample and the claim (MU13d). The claim is the load-bearing
  one — a pre-gate sample cannot see a CONNECT that binds after it — and T18 is
  what pins that, positively: no write, no uncertainty, ownership handed back.
  M1-M7 were re-run against the final tree (the extraction and the
  collect/stage split touch exactly the code they pin) and each failed its paired
  test.
  Residuals stated in `docs/CONFORMANCE.md`: the parked action (deadline and Will)
  is memory-only across a crash, a crash between the rungs and the save loses that
  close's purge, a definite save failure loses restart-survival of the deadline, a
  crash after a landed save but before Will publication loses the Will, and a
  verdict staged during the accept rollback is lost with the aborted object (with
  its three-way register residue).
  Spec5 [4.8.2], [4.9].

## F-018

**Title:** Make the late-uncertainty refusal unconditional in clear(), give the three
newly-exposed callers…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 18

### Original finding record

- [x] Make the late-uncertainty refusal unconditional in `clear()`, give the three
  newly-exposed callers a recovery, and prove the two transient ones cannot reach it — so
  a discard never runs on a sequence nobody has classified. (Retitled at close-out. The
  original wording promised every `clear()` caller a recovery; two of them ship an
  unreachability proof plus a fail-closed refusal instead, and the shared helper declines
  for a parked shape it cannot arbitrate. The delivered invariant is the narrower one in
  this title, and the reviews that caught both overclaims are in the ledger.)
  The COMPLETE partition is SIX production call sites, which the plan miscounted as five
  by omitting the one that already recovered. FOUR are REFUSAL-AWARE: reachable while the
  record state is unresolved, and each acting on the sentinel rather than destroying. What
  each then does differs, and none of them recovers unconditionally — the deferred
  discharge in `sess_uncertain.go` re-parks its consumed action (the pre-existing case this
  item generalized); CONNECT's clean start rejects without evicting; and the clean close
  and session expiry park the discard only when the shared helper ACCEPTS, failing closed
  to the historical evict-and-log when it declines. TWO clear a transient object built
  from a record just read and carry an unreachability proof.
  `clear` refused at ENTRY while a record
  write was unresolved, and that check is a check-then-act: the fence is what
  serializes writers, so an off-readLoop one — a register store settling — can arm
  `seqUncertain` while the call is already waiting for the fence. The in-fence
  re-check was OPT-IN, taken only by the deferred discharge, because it was the one
  caller that could put its consumed action back.
  What landed: the opt-in flag is gone and BOTH refusals always return
  `errMQTTClearRefusedUnresolved`, the in-fence one releasing with `endRecordTx` rather
  than ending the generation, since nothing was mutated. The
  ordinary clean close and session expiry share `reparkRefusedDiscard`
  (`sess_uncertain.go`): park a discard-only obligation (`expiryInterval` 0, so the
  discharge takes the deferred discard branch) and restore RESIDENCY, which is what
  the retry pass and CONNECT both need and what an expiry-0 record has no other
  reaper for. The close does NOT park the Will — a memory-only Will is dropped by a
  takeover, a regression against today — and falls through to `mqttHandleWill`,
  which publishes immediately for the expiry-0 session a discard always is. CONNECT's
  clean start rejects without evicting, exactly as its early path does. The helper is
  strictly IDEMPOTENT: it accepts an already-parked action only when that action ends
  at disconnect and carries no Will, never mutating or replacing it, and declines
  every other shape back to today's evict-and-log — a parked action carries no owner
  identity, so nothing can prove whose deadline or Will a retarget would overwrite.
  `expireSession` additionally stops taking its superseded shortcut while its own
  write is unresolved: an ambiguous write that LANDED with its response lost leaves
  `sess.seq` stale and the record's sequence higher, so that shortcut evicted the
  only object able to classify it, making the recovery unreachable for half the
  ambiguities it exists for. Sequence is not identity; the decision now runs inside
  ONE record transaction spanning the load, the `sess.seq` read and the decision, and
  defers ownership to the digest classifier.
  The TWO transient callers were REFUTED as unexposed, so the original promise that
  "the restore path keeps the object it was about to discard" is withdrawn rather
  than implemented: `expireSessionFromRecord` and `createOrRestoreSession`'s overdue
  discard both clear a transient object built from a record they just read, which is
  registered nowhere and written through by nobody, so no save can arm uncertainty on
  it. They keep the unconditional refusal as a fail-closed invariant and carry the
  proof as a comment. Codex confirmed the refutation independently.
  Tests: `sess_clear_recovery_test.go` (expiry at both windows, the non-sentinel
  post-claim failure, unconditional refusal with `recordEpoch` unchanged, the
  production-shaped re-entry, the declining shapes, the landed-response-lost expiry,
  and two fence tests) and `sess_clear_recovery_e2e_test.go` under
  `mqttport_fulltests` (the clean close with its Will over real connections, the
  rejected clean start, the v5 expiry-override discard reporting Session Present
  false, and the owed-now Will characterization). Mutations run and KILLED: MU-A,
  MU-A2, MU-A3, MU-B, MU-B2, MU-B3, MU-C, MU-D (against both the expiry and the close
  test), MU-E, MU-F, MU-G, MU-G2, MU-H, MU-I, MU-I2, MU-I2a. Two were run and
  SURVIVED, reported rather than dressed up as kills: MU-J (the helper reads the
  parked action without `sess.mu`) — every writer of that field is reachable only
  gate-held or as a singleton check-and-write inside one `sess.mu` section, so no
  execution overlaps one with the helper's read and `-race` has nothing to observe;
  and MU-I2b (leaving the `sess.seq` sample outside the fence) — the pre-fence sample
  normally equals the later value, and `tryBeginRecordTx` reports only acquisition
  failure: never transaction identity, and (since waiter priority landed) not even that
  a transaction is open at all. Both remain as discipline carried
  on the argument, and the `sess.seq` read still moved inside the fence because the
  window it closes is real. MU-A3 needed a test the plan did not have, and it is killed
  as a DEFENSIVE-branch mutation rather than a production-shaped one: with the session
  RESOLVED the helper's own uncertainty gate already refuses, so only a post-claim failure
  leaving the session UNRESOLVED separates the caller's sentinel check from that gate —
  and that state is NOT production-reachable, because `clear()` holds its record
  transaction from before the in-fence re-check through the claim and the delete, so no
  writer can arm uncertainty inside it. T3b constructs the state directly and says so;
  what it pins is that the two checks are distinct facts, not a runtime interleaving.
  Two mutation-harness faults were found and fixed
  mid-run — a shell field-split that ran an empty `-run` regex and reported "no tests
  to run" as a survivor, and an MU-I2a definition that re-acquired the fence and so
  never modelled deciding outside it.
  Residuals in `docs/CONFORMANCE.md`: the obligation is memory-only, so a crash or a
  remote takeover drops it, and a non-discard-only parked action makes the helper
  DECLINE — a third loss path needing neither, whose reachability is unknown; for the
  v5 expiry-override close all three leave a record a non-clean reconnect resumes (its
  own §0 bullet below); the clean close's owed-now WILL is published after the
  recovery, so the same crash loses it, which is pre-existing and not closable without
  a durable Will intent; a session whose ambiguity never resolves is held resident
  rather than reaped by the sweep's stamp-a-new-deadline fallback, with the full
  retained footprint and the once-per-sweep re-entry stated; and the protection is per
  owning broker, complete fleet-wide only once every broker serving the account runs
  this code.

## F-019

**Title:** Settle a definite record-CAS rejection by RE-ATTEMPTING the write, for the
UNBOUND…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 19

### Original finding record

- [x] Settle a definite record-CAS rejection by RE-ATTEMPTING the write, for the UNBOUND
  never-expiring leak this bullet described. The bullet's own candidate — an
  authoritative readback under the ownership gate — was DISPROVEN by the plan review
  with upstream source: JetStream returns 10164 while a write for the subject is in
  process (`jetstream_batching.go`), so a load finding the record at our base sequence
  is compatible with the contender committing a moment later, and clearing the marker
  there would delete the only reason anything ever looks again. What ships instead is a
  PROBE: a memory-only marker armed centrally at the store boundary (so an unenumerated
  rejection site is covered — the enumeration was itself incomplete twice), validity as
  a predicate (`recordRejectedSeq == sess.seq && !seqUncertain`, the last conjunct
  load-bearing because an unresolved save is refused before the store and would classify
  on a zero-valued attempt), and a settlement that re-saves CURRENT memory so a fresh
  CAS arbitrates ownership. The readback then distinguishes only contention (record at
  the probe's own base — never ownership) from a foreign owner (gone, or at another
  sequence); there is no digest arm, because a rejected write did not land. It runs from
  the periodic pass, renamed `reconcileResidentRecordState`, under a new
  identity-current nobody-claim gate that is the sole unboundness check on that path — a
  plain claim would let a stale object read a same-broker replacement's own record as
  foreign. A companion fix ships with it, because the delayed-Will record fence reads a
  newer record with `DisconnectedAt == 0` as a live newer connection and cancels an owed
  Will, and a never-expiring session makes no disconnect save at all: its close now
  stamps `disconnectedAt` before the detach, and therefore before the shared purge's own
  write. The stamp needs the record FENCE rather than `sess.mu`, because an off-readLoop
  writer takes the fence without the ownership gate and a body it has already marshalled
  would otherwise land after the Will's origin capture. The acquisition is NON-BLOCKING:
  a refused acquisition stamps anyway, unordered, since the probe is a writer this change
  ADDS and would otherwise persist `DisconnectedAt = 0` at a newer sequence, and the
  residual is the one body the stamp could not order against — on the waiter-priority arm
  added later, one the prioritized writer marshals just after the refusal rather than one
  already marshalled, at the same cost.
  Waiting for the fence instead bought two defects for one fix (a stale entry uncertainty
  check, and a park timestamped a JetStream timeout late that re-opens an elapsed Will
  Delay), which is why it does not. The hazard itself is pre-existing — the void-verdict
  release handoff already makes such a write — but was not shippable alongside a recurring
  second writer. The reproduction the bullet demanded is a CAUSAL PAIR on an
  instance-scoped, client-hash-keyed persist-drop seam: notification delivered → evicted
  by `processSessionPersist` alone; identical takeover with it dropped → resident until
  the settlement classifies it. 8 plan rounds + 9 holistic rounds. **The BOUND half was
  cut** at round 9 after four consecutive regressions in it (next bullet); the unbound
  design was untouched by all of them. Residuals in `docs/CONFORMANCE.md`: convergence is
  bounded by the next PASS that reaches the session — a serial unbudgeted walk, so the
  honest bound carries its backlog term, not "one interval"; a BOUND session was not
  settled until it unbinds (closed by the next bullet);
  sustained contention holds the object at one probe per pass; the marker is
  memory-only; the record is not a liveness lease; protection is per owning broker and
  behaviourally asymmetric during a rolling upgrade although wire-symmetric; and the
  deferred close's "loses restart-survival of the deadline" residual is NARROWED — a
  landed probe repairs it, a contended one does not.

## F-020

**Title:** Converge a BOUND session's record-CAS rejection: close the split-brain
[MQTT-3.1.4-3] requires,…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 20

### Original finding record

- [x] Converge a BOUND session's record-CAS rejection: close the split-brain
  [MQTT-3.1.4-3] requires, without the four defects nine review rounds found in
  successive attempts. **Done.** The settlement is made by the displaced connection's
  OWN readLoop (`mqttGateDispatchOnRejection`, `sess_rejected_write.go`), from
  `mqttParse`'s per-packet loop, BEFORE the dispatch switch: it claims the ownership gate
  as the bound client — the claim a SUBSCRIBE makes, so it never fails one — takes the
  record fence in its blocking form, and probes. Only a LANDED CAS lets the packet
  through; the three non-serving outcomes are distinct. **Foreign**: discard the Will as a
  local takeover does, dispose the session state under the gate, then attempt the 0x8E
  through the shared tail and close SYNCHRONOUSLY on the readLoop before returning nil (the
  graceful-DISCONNECT branch's shape; the attempt is suppressed for a pre-CONNACK
  connection, or one whose DISCONNECT another path already claimed). **Unsettled**
  (contended, unreadable, ambiguous, or suspended by an unresolved
  write): fail closed with `errMQTTSessionOwnershipUnsettled` → **0x89**, never 0x8E,
  through the ordinary parse-error path so the real close bookkeeping runs. **Rebound**:
  close in SILENCE — the takeover makes its own 0x8E attempt, and speaking here would race
  it for the reason the client is left with — with `mqttDiscardWillOnTakeover`
  applied here, because `mqttHandleClosedClient` owes a Will BEFORE it contends for the
  gate and an early close would otherwise publish a Will the takeover was about to
  suppress. A refused ownership claim is CLASSIFIED rather than read as a verdict, through
  `lockSessionForTeardown`: transient contention is waited out and probed, a rebind takes
  the silent arm, and a gate still held when the bounded spin gives up is unsettled. The
  four recorded defects
  are answered structurally: placement is per-packet and pre-dispatch (a save the previous
  packet in the same buffer triggered arms the marker between the two); the serve gate
  (`recordRejectionGatesServingLocked`) is a SEPARATE predicate from the probe's licence,
  and deliberately omits `!seqUncertain` so a suspended probe keeps gating; the stop is a
  parse error, so the teardown, the keepalive deadline and DISCONNECT semantics are the
  existing ones; and CONNECT/DISCONNECT are exempt with the reasoning stated, while
  PINGREQ is not — which is what bounds convergence at ~1.5 keepalive intervals. At most
  ONE probe per arming, so the gate hold is the fence wait plus a store and a readback
  (~3 JetStream timeouts, assuming the fence is not barged — see the fence-fairness bullet
  below) and contains no socket I/O. 6 plan rounds plus the holistic review;
  the disposal-write mechanism was DELETED at plan round 4 after three rounds cut a new
  seam in it each (see the courtesy-write bullet below). Residuals in
  `docs/CONFORMANCE.md`, fourteen of them, the load-bearing ones being: outbound deliveries
  continue until the next inbound packet; a keepalive-0 silent client converges only at
  unbind; the packet racing the marker's arming is served; a no-verdict close may drop a
  connection that was still ours; the courtesy 0x89/0x8E writes are unbounded; and the
  resolver's clear-at-base seam can produce a FALSE foreign verdict, which now closes a
  live connection (next bullet).

## F-021

**Title:** The session-record fence is now BARGE-PROOF against non-blocking
acquisitions, which is what…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 21

### Original finding record

- [x] The session-record fence is now BARGE-PROOF against non-blocking acquisitions, which
  is what this bullet named ("count them and have `tryBeginRecordTx` refuse while one is
  queued"). `beginRecordTx` ANNOUNCES itself in an atomic waiter count incremented BEFORE
  it contends on `sess.mu` — the announce is the linearization point, and a count taken under
  the mutex would have left a pre-advertisement window resting on `sync.Mutex` fairness the
  Go API does not document — and both non-blocking acquisition sites refuse while it is
  nonzero: `tryBeginRecordTx` and the void-verdict release handoff's inline acquisition
  (`voidVerdictHandoffPendingLocked`, one conjunct serving its three uses). The refusal in
  `tryBeginRecordTx` is DOUBLE-CHECKED, and the pre-lock half is a liveness guarantee rather
  than an optimization: without it a try-storm keeps contending for the very mutex the
  announced waiter needs, so the claim would have rested on mutex fairness again. A caller
  refused pre-lock never touches `sess.mu`; one already past it contends once and refuses
  at the authoritative under-mutex reload, which is where acquisition linearizes.
  Two corrections to this bullet's own text. Its "three blocking callers" undercounts: the
  affected set is the CLASS of blocking fence acquisitions made while holding the session
  OWNERSHIP GATE — `mqttProcessSubs`, `mqttProcessUnsubs`, the bound rejection settlement,
  the CONNECT accept transaction, session expiry, the ordinary disconnect close on both
  branches, and the deferred disconnect discharge — explicitly non-exhaustive. And the
  CONNECT spin is 10 failed checks with nine 100 ms sleeps (~900 ms), not 10 × 100 ms.
  RETAINED RESIDUAL, not closed: there is no FIFO among BLOCKING waiters, so a newly
  arriving `beginRecordTx` can win the post-broadcast mutex race against an older one and
  identity churn can starve a waiter at a small constant count; `docs/CONFORMANCE.md`
  residual 13 states that with the reciprocal cost (refused non-blocking writers retry on
  events, not on a clock; the never-expire close stamp, which does not retry, takes its
  unordered arm on more occasions) and the per-broker rollout scope. Tests:
  `TestRecordTxTryBeginDefersToQueuedWaiter` (six parts — the direct refusal at counts 1 and
  2, the barge-free storm window, exactly-once withdrawal, and the three orderings the double
  check consists of, driven through three new production seams: a wait-loop seam under
  `sess.mu`, a pre-lock seam outside it, and a withdrawal-guard seam on the abnormal exit
  path),
  `TestRecordTxBlockingAcquisitionBoundedUnderTryStorm` (this bullet's stress test),
  `TestRecordTxWaitHookGoexitWithdrawsAnnounce` (the withdrawal guard),
  `TestRecordTxAbnormalWithdrawalPrecedesTheUnlock` (that guard withdraws INSIDE the
  critical section, so no reader of the count ever sees an announce for a waiter that no
  longer exists), `TestRecordTxWithdrawGuardHookGoexitStillCleansUp` (that guard's own
  cleanup is exception-safe, so a seam dying mid-cleanup strands neither the announce nor
  the session lock) and
  `TestMQTTVoidVerdictHandoffDefersToQueuedWaiter` (predicate, release deferral to the
  waiter's own release, and the periodic backstop's re-arm). Mutations run and KILLED:
  M1-M11, M12a/M12b, M13 and M14a/M14b (both waiter checks, the handoff conjunct, the
  announce and its placement, the early withdraw, the withdrawal guard and its flag, the
  withdrawal moved after the unlock, each half of the guard cleanup's exception safety,
  the backstop's `owed` predicate, each half of the double check, the
  reload hoisted out from under the mutex, and each check weakened from `> 0` to `== 1`).
  One earlier shape of the withdrawal-ordering test — an observer parked on `sess.mu`
  reading the count as the dying waiter released it — let M13 SURVIVE 200 iterations and
  was replaced by the guard seam's lock-held assertion; that is what the third seam is
  for. Both regression tests' failure paths were
  checked to REPORT rather than hang: the handoff test's cleanup releases the fence holder
  before it unparks and joins the waiter, and with that release removed an intentionally
  failed pre-release assertion hangs to the suite timeout instead of failing (verified in
  both directions), which is how an announcement regression would have disappeared into a
  timeout. One MUTATION-HARNESS fault was found and fixed mid-run, by review: M6 was defined
  as deleting both calls to the guard closure, which leaves it declared and unused, so the
  package did not build and the harness read the nonzero exit as a kill — a build failure
  is not a kill. M6 is now a compiling behavioural mutation (the guard fires but withdraws
  nothing), the harness refuses any run whose paired test did not actually execute, and
  every mutation above was re-run on the final tree under that rule. 21 plan-review
  rounds. Found by holistic round 5 of the bound settlement.

## F-022

**Title:** "Displace this connection" now has one owner per leg, and a connection gets
AT MOST ONE…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 22

### Original finding record

- [x] "Displace this connection" now has one owner per leg, and a connection gets AT MOST
  ONE server-initiated DISCONNECT ATTEMPT. Four paths displace a bound connection — a local
  CONNECT takeover (`mqttProcessConnect`), `processSessionPersist`, a resolver's foreign
  disposal, and the bound rejection settlement — and each used to notify the client itself,
  putting a second DISCONNECT on one socket (0x89 "try again later" after 0x8E "session
  taken over", or two 0x8Es), which [MQTT-3.14.4-1] forbids the server and which gives the
  client the opposite advice to the one that is true. Of the three legs the original bullet
  asked to make atomic, two already had exactly-once owners — `closeConnection` is
  idempotent (`c.closed.Swap`), and `willDisposition` is consumed once by
  `mqttTakeWillLocked` — so the fix added the missing third rather than a fourth mechanism:
  `mqtt.disconnectSent`, consumed under `c.mu` through the single-homed
  `mqttClaimDisconnectLocked`. Every server-DISCONNECT writer claims: the four displacement
  paths (through one shared tail, `mqttNotifyTakenOver`), the parse-error footer, the
  packet-too-large 0x95 and the shutdown drain's 0x8B. The enqueue-path writers claim in the
  same critical section as the write it authorizes; the shutdown drain, which bypasses the
  enqueue path, RESERVES the claim under the lock and writes through `writeDirect` after
  releasing it, so it can spend the claim and write nothing — which is one reason the
  guarantee is an attempt and not a packet. It builds first, so a nil build spends nothing.
  A latent second defect in the same class was fixed with it:
  the CONNECT-takeover and persist notifications were not CONNACK-gated, so a connection
  displaced before its own CONNACK — the state `mqttAbortAcceptedConnect` leaves — got a
  0x8E ahead of it, against [MQTT-3.2.0-1]; that gate now has one home in the shared tail
  and reads `connAckWritten` under `c.mu`, which is why `mqttEnqueueConnAck` stores the flag
  inside its own write's critical section (storing it after the unlock let a notifier read
  false for an already-acknowledged client and drop a DISCONNECT it was owed). Residuals,
  all stated in `docs/CONFORMANCE.md` item 12: the guarantee is at-most-one ATTEMPT, not one
  delivered packet (a best-effort write can lose to a concurrent close, the shutdown drain's
  reservation can be spent without a write, and the displaced arm's synchronous close can
  beat the displacer's notify, leaving zero); a notifier that takes `c.mu` before the CONNACK
  writer still reads the gate false, so an already-connecting client can get its success
  CONNACK and then a silent close, with the displaced CONNECT's own remaining work
  arbitrated by the record CAS as before; which reason wins
  is race-determined, so a delivered 0x89 can be stale; non-DISCONNECT traffic can still
  trail the winner until the close lands, which needs a delivery-side gate; the notification
  write is still unbounded (bullet below); and the `processSessionPersist` overlap with a
  gate-held probe was closed later by the classified-eviction item — its worker now defers
  to a held gate instead of barging. The claim is an in-process bool, so protection is PER
  OWNING
  BROKER and the rollout is asymmetric: nothing wire-, stream- or record-visible changes,
  and during a mixed-version window a session resident on an OLDER broker keeps the full
  duplicate-DISCONNECT behaviour on its own sockets until that broker upgrades — a partial
  deployment is not cluster-wide protection. The Will-fate routes stay with the
  Will-suppression bullet below,
  which defers itself to the Will-outbox analysis. Found by holistic rounds 1 and 2 of the
  bound settlement.

## F-023

**Title:** The uncertainty resolver cleared an ambiguity on a PLAIN LOAD at the base
sequence, so a…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 23

### Original finding record

- [x] The uncertainty resolver cleared an ambiguity on a PLAIN LOAD at the base sequence,
  so a late-committing proposal was later read as another owner's — a FALSE FOREIGN
  verdict. Fixed by changing the settlement contract: `resolveUncertainWriteInTx`'s
  at-base arm now RE-ATTEMPTS the attempt's exact framed bytes at its own CAS base
  (`storeFramedSessionMsg`, shared with the contention retry), so a CAS arbitrates
  instead of a read guessing. A success proves the content durable at the sequence it
  reports and kills the original if it is still in process; a rejection is classified by
  one further read through `classifyAttemptReadback` — the single helper both reads use,
  which is where the `Sequence > baseSeq` conjunct and the below-base fail-safe live;
  an ambiguous re-attempt keeps the identity untouched for the next pass. "It did not
  land" is no longer a reachable verdict, so `onNotLanded`, `runUncertainReverts` and the
  void-verdict re-save continuation are DELETED and the 22 production registrations became
  `retainOnLanded` or nothing; the three landed wrappers that captured a packet token now
  take the resolver's — including the cleanup `commitUnsub` reaches, which was a nested
  closure capturing the enclosing token and kept using it through a signature change on
  its caller (holistic round 6). It is now the method `removeUnsubConsumer`, so the
  packet's transaction is not in scope to be captured at all; the packet's own token is
  additionally named `packetTok` to keep the two visually distinct. KNOWN TEST GAP:
  passing the wrong token explicitly is still possible and no test covers it — the
  end-to-end unsubscribe test releases a consumer with no pending registers, so the
  token-gated record fallback is never reached. A discriminating fixture must force the
  cancellation tombstone to fall back to a record save and observe the consumer delete
  ordered against that verdict's durability, which needs a seam the harness lacks.
  The one ACKNOWLEDGED caller, the v5 UNSUBSCRIBE, gets a truthful
  UNSUBACK: its true per-filter reasons and reason strings are snapshotted before the 0x83
  overwrite and restored iff the flag set by THAT attempt's landed continuation is
  observed, with the acknowledgment skipped for a connection the settlement displaced.
  An ABSENT record is arbitrated the same way rather than disposed on the read: a
  clustered apply never re-checks the per-subject CAS (`canConsistencyCheck :=
  !isClustered`, nats-server v2.14.4 `server/stream.go:6247`), so a proposal already
  accepted into the log lands even after an exact-sequence delete — 10164 on the
  re-attempt therefore defers, and only 10071 classifies foreign.
  Explicit non-changes and residuals: the unconditional create (CAS base 0) is never
  re-attempted — it carries no header, so it is never classified at-base — and keeps its
  existing classification and residual; the below-base arm stays fail-safe rather than
  proven; and gone-plus-10071 stays fail-safe too, because a leader transition clears the
  in-process map while a replicated proposal can survive it. So `docs/CONFORMANCE.md`
  residual 7 is closed for the AT-BASE arm at a non-zero base, with the base-0 and
  gone-plus-10071 residuals retained and the forward-resolution posture stated. Found by
  plan round 1 of the bound settlement.

## F-024

**Title:** MEASURED AND CLOSED: an ambiguous shared-QoS 2 AFFINE provisioning save did
leave a durable…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 24

### Original finding record

- [x] MEASURED AND CLOSED: an ambiguous shared-QoS 2 AFFINE provisioning save did leave a
  durable affine path with no membership, and the settlement now reclaims it. The measured
  end state, from a fixture that confirms the AFFINE save is the one made ambiguous (an
  installed generation with no shared membership): the settled record carried
  `Q2AffGen`/`Q2Aff`, the `$MQTT_Q2AFF_*` durable survived on `$MQTT_out`, and `tmaxack`
  held the consumer's 1024-slot charge — with no membership anywhere and no claims, since
  the readiness gate never opened. BRANCH CORRECTION to the bullet's stated mechanism: the
  packet's final `sess.update` is not refused in the single-filter shape at all. A failed
  filter contributes nothing, so `needUpdate` is false and `update()` returns nil — the
  `else` branch RUNS and the unused-path unwind IS reached. Its own confirming save is
  what is refused at entry, which classifies DEFINITE, so `restoreQ2AffineUnwind` puts the
  path back. The multi-filter shape routes through `rollbackSubscribePacket` and its saves
  are refused too. Both shapes converge on the same durable end state, which is why the
  outcome question stood while the mechanism did not. FIX: the provisioning save's
  `saveMaybeLanded` arm registers a landed continuation
  (`unwindProvisionedQ2AffineOnLanded`) that runs the existing three-arm unwind under the
  resolver's token, guarded by `hasSharedQoS2Sub()`, and ONLY when that call MINTED the
  generation — a resume re-establishing a path the record already named reaches the same
  save and may owe assignments to a started delivery. The packet's own unused-path unwind
  is now gated on `!isSeqUncertain()`: while unresolved it could only discard the
  generation's assignments irreversibly and then restore a path it never removed.
  RESIDUAL, stated in `docs/CONFORMANCE.md`: the crash window between the ambiguous save
  and its settlement. Resume does NOT reclaim the path — it re-establishes it, without
  recharging, whenever the restored record carries `Q2AffGen` — so an unused durable
  survives an ordinary resume and is only made useful by a later shared QoS 2 SUBSCRIBE
  reusing it, or removed by session teardown deleting it; for a never-expiring session
  that is the broker's lifetime. Plus one more arbitrating CAS site on the same-base
  leader-transition exception's exposure surface.

## F-025

**Title:** A per-subject CAS is a linearization point only WITHIN a leader term, so a
surviving unapplied…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 25

### Original finding record

- [x] A per-subject CAS is a linearization point only WITHIN a leader term, so a
  surviving unapplied proposal can be overwritten across an election. DONE for `$MQTT_sess`
  as RECOGNITION, not arbitration, and not across every writer — read the residuals before
  treating this as closed. JetStream checks `Nats-Expected-Last-Subject-Sequence` BEFORE
  proposing and never again at apply (`canConsistencyCheck := !isClustered`, nats-server
  v2.14.4 `server/stream.go:6247`), and a leader transition clears
  `expectedPerSubjectInProcess` (`jetstream_cluster.go:4720`) while a replicated proposal
  can survive it, so a new leader can admit a second same-base write and apply BOTH in log
  order. PREVENTING that is out of reach here: production NATS is external
  (`internal/backend/backend.go`), and the only apply-time-rechecked precondition
  (`Nats-Expected-Last-Msg-Id`) compares a stream-wide value and cannot serialize one
  client ID's subject.

  What landed instead is the identity the bullet asked for — "a generation or owner token
  carried IN the record and checked by the writer, so a late apply is recognisable as
  stale" — as a PARENT POINTER (`WID`/`PWID`) on every `$MQTT_sess` record. Two writes
  carrying the same `PWID` are siblings of one base, so a survivor that is provably our
  sibling is no successor of ours, and the broker stops answering it with 0x8E "session
  taken over" — which tells a client to stop reconnecting — and answers 0x89 "server busy,
  try again later" instead. Consulted on the four paths that had already concluded foreign
  (definite rejection bound and unbound, the ambiguity resolver, the two CONNECT-time
  classified saves), plus the CONNECT rollback readback, whose sequence-only supersession
  cancelled the expiry timer on the same unsupported conclusion. `sess_fork_lineage.go`;
  `docs/ARCHITECTURE.md`, "Recognising a forked session record".

  NOT closed by it, and each filed below: the persist-observation route (the one that
  causes this most often) was closed by the item directly below, which taught the eviction
  to classify; no durable state is repaired; the other five arbitrated streams have no
  lineage; and the Raft window itself is still not reproduced by any test — what the tests
  stage is the durable outcome. Full list in `docs/CONFORMANCE.md`, "A forked session
  record is RECOGNISED, and nothing more". Found by holistic round 5 of the
  ambiguous-write settlement.

## F-026

**Title:** processSessionPersist evicted the rightful client on a remote PubAck it could
not classify —…

- Status: Resolved
- Classification: Resolved finding
- Release treatment: Completed
- Original roadmap order: 26

### Original finding record

- [x] `processSessionPersist` evicted the rightful client on a remote PubAck it could not
  classify — synchronously, with 0x8E, cancelling the displaced incarnation's Will outbox
  and the session-expiry timer on unclassifiable evidence. Closed by moving the resident
  branch's eviction onto a per-client classifying worker
  (`evictDisplacedOnRemotePersist`): one admitted worker per client (coalesced through the
  shared `reconcileWM` watermark plus an `evictRunning` flag), a latest-knowledge-wins
  loop bounded by strict trigger advance, ONE `as.mu` decision section that resolves the
  resident object fresh, OBSERVES (never claims) the ownership gate, takes the record
  fence non-blocking (which retires the round-2 ordering hazard: adoption happens inside
  the writer's open transaction, so fence-acquired proves adoption QUIESCENT — an
  ambiguous landed attempt still leaves a stale pair, which only ever narrows the
  verdict; see the unresolved-attempt residual in `docs/CONFORMANCE.md`),
  requires a strictly advanced record (`Sequence > sess.seq`; equality — the session's own
  record — no longer evicts), and classifies at decision time. A proven sibling answers
  0x89 with the ownership-unsettled Reason String and LEAVES the delayed-Will outbox
  records pending; everything unreadable/absent/below-trigger concedes 0x8E through the
  license rule (the observation licenses, the read only narrows). Release order is
  `as.mu` → `endRecordTx` → notify/close, with a new handoff-free `abandonRecordTx` for
  abnormal exits (the skipped void-verdict handoff belongs to the periodic backstop).
  Residuals — asynchronous and REFUSABLE eviction (held gate/fence defers; a quiet bound
  session then converges only on a local trigger, the §"resident session never
  re-validates" class below), the serving window, lost observations still unreplayed,
  clean-start/re-minted-gen forks still concede, and the rest — are stated in
  `docs/CONFORMANCE.md`, "A forked session record is RECOGNISED, and nothing more".
  `internal/mqtt/mqtt.go`, `sess_recordtx.go`, `sess_fork_lineage.go`;
  `sess_persist_evict_test.go`.

## F-056

**Title:** Remove inherited retained-message format and delete-protocol compatibility

- Status: Resolved
- Classification: Pre-release persistence compatibility cleanup
- Release treatment: Completed; MQTT wire-protocol retained behavior is unchanged
- Original dossier: `pre-release-migrations.md`

### Resolution

Three coordinated development-era mechanisms are deleted, and with them the only
reason the retained path had to stay silent about records it could not read.

**The `RD` delete-notification protocol** is gone entirely: the constant, the
account-wide `$MQTT.JSA.*.RD` subscription, `processRetainedMsgDel`,
`notifyRetainedMsgDeleted`, and the `mqttRetMsgDel` type. Stated precisely, `RD`
was not pure redundancy — it was an independent best-effort repair for one case,
a remote broker whose `AckNone` tombstone delivery was lost, where the origin's
sequence-carrying notification might still land. Removing it therefore WIDENS the
window of F-059's stale-tombstone arm — that arm now waits for the retained cache
copy to expire and a matching subscription to arrive, where the notification might
have retired the entry sooner. It does not make the arm unbounded: the next
matching subscription's `loadRetainedMessages` reads the tombstone and removes the
index entry. The widening is disclosed in F-059 and in `docs/CONFORMANCE.md`. Note
that F-059's sequence watermark does NOT by itself replace what `RD` was doing: a
watermark orders the writers that run, and a lost `AckNone` delivery runs no
writer at all. Covering the loss case needs a replayable consumer or a
reconciliation against stream progress — and specifically for the ABSENT-index
case, which no subscription reaches — which is why F-059 states the two
requirements separately. `RD` is
removed anyway because a lossy fire-and-forget second channel is dedup-grade
redundancy standing in for the sequence arbitration that is actually missing —
not because it was doing nothing.

**The legacy JSON reader** is gone: an absent `Nmqtt-RFlags` header now returns
`errMQTTRetainedMsgUnsupportedFormat`, a distinct sentinel from
`errMQTTInvalidRetainFlags`, because "this is not a format we write" and "these
flags are out of range" are different facts. `mqttRetainedMsg` lost its `json:`
tags with the fallback, which were its only remaining users.

**The old-server tombstone delete marker** is gone from both the encoder and the
decoder. An empty body is the tombstone on its own.

**The compaction delete beside the notification is also removed**, a deliberate
delta from the original dossier's "preserving current tombstone processing and
cleanup". `deleteRetainedMsg(rmSeq)` was provably inert when it did what it
intended and destructive when it did not. The mechanism claim it rested on — that
storing an empty body does not evict the prior record — is false for the pinned
backend: nats-server v2.14.4 `filestore.go storeRawMsg` enforces the per-subject
limit on every store (`for psmax && psmc > mmp { removeMsgViaLimits(fseq) }`)
with no empty-body or TTL exception, so the prior record is already gone and the
delete is a "no message found" no-op. Under the F-059 reordering, however,
`rmSeq` is the sequence of a NEWER live record and the call deletes the
authoritative current one — durable data loss. Removing it eliminates the single
durably destructive consequence of that pre-existing race; what remains of F-059
is volatile-index staleness only. The eviction premise is pinned empirically by an
assertion that a deleted subject holds exactly one stream record.

`docs/PORTING.md` item 8 is rewritten as a correction rather than a re-explanation.
Its original mechanism claim is not merely imprecise, it is self-defeating: if
storing the tombstone had not evicted the predecessor, deleting the tombstone would
leave the predecessor as the subject's last record — but under the pinned backend
the predecessor is already gone, so deleting the tombstone leaves NO record, which
reads as deleted just the same. Whatever the original resurrection really was
cannot be reconstructed from what that item wrote down, and no substitute story is
invented in its place. Keeping the tombstone remains worthwhile for a reason about
DELIVERY rather than storage — it is the record whose delivery makes other brokers
drop their index entry, and a deleted message is never delivered — which is stated
there in those terms.

That destructive window is no longer only an argument. A review round pointed out
that nothing exercised it — the happy path makes the delete inert, so restoring it
fails no test — and it is now reproduced deterministically by
`TestRetainedDelayedTombstoneKeepsNewerRecord`, using a new instance-scoped test
seam (`testRetainedApplyGate`, nil in production) that parks one tombstone's
consumer callback while the local seed of a newer publish runs ahead of it.
Restoring the delete destroys the newer record outright: the subject's stream
record is gone (`message not found`), which is the mutation the removal needed.

**Rejection is now fail-closed on every reading path.** `processRetainedMsg`'s
silent return WAS the old-server-ignore mechanism; with the marker gone, a decode
failure there is a real anomaly. It now warns with the subject and the error and
evicts the subject's index and cache entry. Log-and-return alone would have been
fail-open: a corrupt record REPLACING a valid one leaves nothing servable in the
stream, so a surviving index entry or cached copy would keep serving a value the
durable authority has lost. The eviction uses the logical retained key — the
stream subject with the `$MQTT.rmsgs.` prefix trimmed — because the index is keyed
without it; the untrimmed subject would match nothing and silently preserve the
fail-open behavior. This matches `loadRetainedMessages`, which already warned and
evicted, so the live and post-restart views of a corrupt record now agree. The
eviction is itself unconditional, so under F-059 a delayed corrupt-record callback
can evict a newer valid entry: the same volatile-staleness class F-059 owns, and
the F-059 fix must arbitrate this writer too.

The eviction also exposed a genuine pre-existing defect, which had to be fixed
before it could ship. `mqttSliceHeaders` compared the delimiter search's offset —
which is relative to the remaining slice — against the absolute scan position
(`del == i`), so a key whose length equalled that position ended the scan and
discarded that header and every one after it. The first header sits at
`len(hdrLine) == 10`; `Nmqtt-RExp` is exactly ten characters, and NATS emits
headers in map order, so roughly one retained message in seven carrying an MQTT
5.0 Message Expiry lost `Nmqtt-RFlags` along with the rest and became
undecodable. Before this change that was invisible and nearly harmless — the
publishing broker's local seed had already indexed the record, and the failed
callback returned silently — but every REMOTE broker, which has no seed, simply
never indexed those retained messages. With the eviction in place it became
active loss: the callback removed the correctly seeded entry. The guard is now
`del == 0`, which is what a zero-length key actually is and which the empty-key
check below it already covered independently. `docs/PORTING.md` item 10 records
it, and the "key length equals the scan position" row pins it (mutation M9).
This is why `make full-suite` failed once at the end of verification, on
`TestV5RetainedMessageExpiry`: the run-to-run header ordering decides whether the
alignment is hit. The same test passes 12/12 on the parent commit, which is what
identified the change rather than the environment as the trigger.

Making that eviction actually closed took a second change, which a review round
caught: `removeRetainedMsg` returned early — without touching `rmsCache` —
whenever the index held no entry for the subject. Cache-present with index-absent
is reachable, and is F-059's own first arm: an exact-filter subscription caches
unconditionally while only the callback and the local seed ever index. In that
state a corrupt replacement evicted nothing, and a later valid add re-indexed the
subject WITHOUT refreshing the cache, so the stale predecessor became servable
again. A `seq == 0` removal now deletes the cached copy whether or not the index
holds an entry, which is what "remove whatever is there" meant all along; a
`seq > 0` removal is unchanged, since it names one specific record.

Scope, stated precisely: a foreign record carrying a valid `Nmqtt-RFlags` header
is still accepted — cross-account sourcing is a supported topology — and malformed
OPTIONAL headers (properties, expiry) are still ignored rather than rejected.
Only headerless or otherwise undecodable records fail closed.

Compatibility: pre-release, no released persisted-state contract. A legacy JSON
record or a `-1` marker tombstone left in a development stream is warned about and
never served; the remedy is republishing the retained message or recreating the
`$MQTT_*` streams. Mixed old/new development fleets are not a supported topology,
and this is an intentional refusal rather than an oversight — new-broker writes
still decode on an old broker, and a delivered old-broker marker tombstone now
converges immediately, since warn-and-evict is exactly the state that tombstone
intended.

Tests: five native tests in `retained_convergence_test.go` run in the default
tree under `-race`, because no running suite covered multi-broker retained
convergence at all (`mqtt_cluster_ref_test.go` is build-tagged
`mqttport_clustertests` and never builds).
`TestRetainedCrossBrokerConvergence` proves a non-publishing broker learns both a
retained message and its deletion from the stream alone, and asserts the durable
residue: the last record for the subject is the empty-body tombstone and it is the
only record for that subject. `TestRetainedCorruptRecordFailsClosed` plants an
undecodable record BEFORE a valid one so the valid record's arrival in a restarted
broker's index is an ordering barrier proving the corrupt one was already
processed, then requires the rejection warning naming the subject and the
sentinel. `TestRetainedCorruptReplacementEvictsCachedPredecessor` warms both the
index and `rmsCache` through an exact-filter subscription before injecting the
corrupt replacement, and is the test a log-and-return implementation fails.
`TestRetainedEvictionClearsCacheWithoutIndexEntry` reproduces cache-present with
index-absent and proves the eviction reaches the cache there.
`TestRetainedDelayedTombstoneKeepsNewerRecord` is the compaction-delete
reproduction described above.

Nine mutations were run, each failing its paired test: restoring the JSON
fallback, treating a missing header as `flags=0`, skipping the tombstone removal,
re-adding the encoder's delete marker, reverting the decode-failure eviction to
log-and-return, restoring the decoder's marker strip, reverting the cache delete
to index-gated, restoring the compaction delete, and restoring the
`del == i` header-scan guard. The marker-strip mutation is
paired with a permanent decode row requiring `Nmqtt-RFlags:-1` to be rejected,
without which nothing pinned the decoder half — the encoder mutation alone leaves
it uncaught. One predicted signature was wrong and is recorded as such:
re-adding the encoder's delete marker does NOT fail
`TestRetainedCrossBrokerConvergence`, because the decode-failure eviction
converges a delivered marker tombstone to exactly the state that tombstone
intended; the empty-body round-trip decode row is its real pairing.

`internal/mqtt/mqtt.go`; `retained_convergence_test.go`, `mqtt_test.go`,
`mqtt_v5_test.go`.

### Original finding record

Preserved from `pre-release-migrations.md` as it existed on 2026-08-10.

**Title:** Remove inherited retained-message format and delete-protocol compatibility

- Status: Open
- Classification: Pre-release persistence compatibility cleanup
- Release treatment: Immediate cleanup before further MQTT-core implementation

#### Evidence

The retained-message path still carries three coordinated compatibility mechanisms:

- `mqttDecodeRetainedMessage` falls back to JSON when the current
  `Nmqtt-RFlags` header is absent. The production encoder always emits the
  header-based format; direct JSON fixtures exist only to exercise the inherited
  legacy reader.
- Empty retained-message tombstones prepend `mqttRetainedFlagDelMarker` so older
  servers fail to decode and ignore them. No older bridge release or supported
  mixed-version fleet exists.
- Every account manager subscribes to the internal `RD` retained-delete
  notification protocol. `processRetainedMsg` publishes that notification and
  `processRetainedMsgDel` consumes it, even though current brokers already observe
  the authoritative retained-stream tombstone. The source comments explicitly
  identify this as backward compatibility intended for later removal.

This is inherited compatibility with older NATS Server MQTT persistence, not MQTT
wire-protocol compatibility. Keeping it expands the accepted storage format and
adds an account-wide subscription, sender, receiver, message type, and tests to
every current broker.

#### Commit-sized resolution

Land one retained-message compatibility cleanup commit:

1. Remove the `RD` constant, subscription, message type, sender, receiver, and
   notification call while preserving current tombstone processing and cleanup.
2. Require the current header-based retained record, remove the JSON fallback and
   old-server delete-marker encoding, and keep validation of all current headers.
3. Replace legacy-format fixtures with current-format corruption and restart
   coverage where the underlying validation remains relevant.

Acceptance requires retained publish, replacement, deletion, restart recovery,
expiry, malformed-header, and multi-broker cache-convergence tests. A retained
record without the current required headers must be rejected with a clear log; it
must never be interpreted as another format.

## F-057

**Title:** Replace legacy reserved-subscription migration with current-schema refusal

- Status: Resolved
- Classification: Pre-release session-schema migration cleanup
- Release treatment: Completed; the live `$MQTT.>` SUBSCRIBE denial is unchanged
- Original dossier: `pre-release-migrations.md`

### Resolution

`purgeReservedSubs` and its `denied`-list plumbing are deleted. A session record
carrying a subscription key in the reserved `$MQTT.>` namespace is now REFUSED at the
CONNECT restore boundary by `mqttReservedMembershipInRecord`, which is read-only and
runs ahead of every other branch — the generation validation, the teardown adoption, the
overdue discard, and the resume construction — so no name is ever derived from such a
record and nothing is written to it. The diagnostic names the offending key and tells
the operator to stop the account's brokers and recreate the bridge-owned `$MQTT_*`
streams. No CONNACK is sent, matching the malformed-generation refusal below it.

Ordering ahead of the generation check is load-bearing, and it is a correction the
holistic review forced: the namespace deny (3610eb3, 2026-07-27) PREDATES
generation-scoped naming (`mqttSessGenFromRecord`, ab683d9, 2026-07-30), so a broker old
enough to have written a reserved membership wrote no `gen` either. With the checks in
the other order every record that can really exist got the generation refusal and the
key-naming diagnostic was unreachable. Both refusals are read-only and both prescribe
recreating the streams, so the order decides only which message the operator sees.
A reserved key alongside a *valid* generation is a shape no writer could produce; it is
refused all the same, because a fail-closed schema boundary is not owed a provenance
story.

The validator looks in `Subs`, `SharedSubs`, `Cons` and `Drains`, through two key
shapes: the `$MQTT.` prefix (plain filters and the synthetic level-up sid) and, for a
`$share/{group}/{subject}` key, the prefix test applied to the underlying subject.
`Cons` and `Drains` are checked because a record can carry a reserved consumer with no
membership left to name it — a shape the old membership-driven migration left behind
entirely.

Scope, stated precisely: the refusal guarantees such a record is never HONORED. It is
deliberately not a quarantine. The expiry sweep's `clear()`, teardown adoption by the
expiry and orphan sweeps, and the orphan-consumer sweep keep treating the record's
keys as ordinary subjects while DESTROYING state — but only while the record's
generation is valid. A record with no valid generation is reclaimed by none of them
(the expiry path refuses to act, teardown adoption validates first, and the
orphaned-consumer sweep counts the record's consumers as referenced), so it waits for
the operator. That is the pre-existing consequence of the generation refusal rather
than something this change introduced, and it is why the diagnostic offers no
work-around: an earlier draft suggested connecting under a different client ID, which
would have left the record and its consumers in place forever. A clean-start CONNECT is
refused too:
`createOrRestoreSession` runs before `processConnect`'s clean-session check, and the
discard it would take deletes the record's consumers — the same migration under
another name.

Residual: a refused restore returns while the client ID is still registered in
`sessLocked`, and `expireSessionFromRecord` yields to that registration without
re-arming the one-shot timer it consumed, so a due expiry can be deferred to a later
sweep pass. Pre-existing, shared with the malformed-generation refusal, and stated in
`docs/CONFORMANCE.md`.

Tests: `TestMQTTReservedRecordRefusesRestore` replaces the three migration tests with
eight record shapes (plain, shared membership, shared `Cons`-only, level-up
`Cons`-only, `Drains`-only, overdue deadline, generationless, claimed teardown), each
asserting a refused CONNECT with no CONNACK, a byte-identical record at the same stream
sequence, surviving JetStream durables, an identically refused clean-start retry, and
the complete formatted diagnostic. Six mutations were run against it: removing the
validator call (all eight rows fail), narrowing it to `Subs` (four fail), dropping the
shared-key arm (two fail), skipping `Cons` and `Drains` (three fail), moving the
validator below the overdue-discard branch (the overdue, generationless and
claimed-teardown rows fail — that move also puts it below the teardown branch and the
generation check), and moving it back below only the generation check (the
generationless row alone fails, which is why that row exists).

`internal/mqtt/mqtt.go`, `sess_resume_reconcile.go`; `reserved_namespace_test.go`.

### Original finding record

Preserved from `pre-release-migrations.md` as it existed on 2026-08-10.

**Title:** Replace legacy reserved-subscription migration with current-schema refusal

- Status: Open
- Classification: Pre-release session-schema migration cleanup
- Release treatment: First cleanup before further MQTT-core implementation;
  preserve the live `$MQTT.>` security boundary

#### Evidence

`purgeReservedSubs` is a full migration for a session record created before the
broker denied subscriptions to its internal `$MQTT.>` namespace. On CONNECT it:

- removes old plain and shared memberships through a session-record transaction;
- stages in-flight registration cancellations;
- determines whether other sessions retain shared durables;
- deletes plain, shared, and synthetic level-up consumers only after a confirmed
  save; and
- carries definite-failure rollback and ambiguous-write continuation behavior.

The accompanying integration tests construct old records and consumers solely to
prove that this migration succeeds and rolls back atomically. No released bridge
ever wrote those records. The machinery therefore contradicts the current policy
that development streams may be recreated instead of upgraded, and it adds a
large mutation path to the already sensitive CONNECT restore transaction.

The security behavior itself is not obsolete. A new SUBSCRIBE must continue to
reject every explicit `$MQTT.>` filter, and per-client NATS permissions remain
defense in depth.

#### Commit-sized resolution

Land one session-schema cleanup commit:

1. Validate restored `Subs`, `SharedSubs`, and their consumer references before
   publishing or resuming the session.
2. If a record contains a reserved membership, refuse restoration with an
   actionable instruction to stop the account's brokers and recreate the
   bridge-owned development state.
3. Delete `purgeReservedSubs`, its denied-list plumbing, migration fixtures, and
   transactional migration tests. Replace them with fail-closed restore tests for
   plain, shared, and level-up shapes.
4. Update changelog and resolved-finding language so it promises refusal, not an
   in-place upgrade, while retaining the documented namespace denial.

Validation must happen before a malformed record can derive a consumer name or
mutate broker-owned state. Ordinary current-format session resume and new
SUBSCRIBE rejection must remain unchanged.

## F-058

**Title:** Reject generationless delayed-Will outbox records

- Status: Resolved
- Classification: Pre-release outbox-format compatibility cleanup
- Release treatment: Completed; no MQTT-visible behavior changes
- Original dossier: `pre-release-migrations.md`

### Resolution

The empty-generation "legacy bucket" is gone. `validateWillOutboxBody` is the
format contract for a persisted `$MQTT_willq` record — a valid incarnation
generation (`mqttValidSessGen`), a nonzero `OriginSessionSeq`, a well-formed
`OriginSessEpoch`, a known `State`, and `WillID`/`ClientIDHash` that are each a
nonempty SINGLE subject token. `validateWillOutboxRecord` adds the binding
between the body and the subject it was stored on.

Every decode boundary validates: the point loader `loadWillOutbox` (which now takes
`domainTk`, so no unvalidated semantic loader remains in the package), the claim
scan, and the cancellation scan. A record that fails is REFUSED — never claimed,
published, cancelled or deleted — and reported once per subject with the operator
remedy. Refusal is read-only, so a refused record is reclaimed by NOTHING; the
diagnostic says so and prescribes deleting the message or recreating the stream.

Two checks are not the obvious ones and are load-bearing.

The subject binding is not implied by body validation. A well-formed record
carrying client B's identities, stored on client A's subject, would be fenced with
B's watermark and session state and then claimed and published on A's subject.
`finalizeWillPublish`'s `WillID` equality only detects a post-claim REPLACEMENT; it
never establishes that the claimed body matched its own subject.

The single-token identity check is not redundant with the binding, because NATS
parses the identities into subject tokens. A body with `WillID: "a.b"` on its
exactly-matching subject passes the binding and is visible to the account filter
(`<domainTk>>`), but is INVISIBLE to the reconnect-cancel filter
(`<domainTk><cidHash>.*`, exactly one trailing token) — so it could fire after a
reconnect that was entitled to suppress it. Both real mints are single tokens by
construction (`nuid.Next()` and `getHash` are base-62), so the check refuses only
foreign shapes.

Two deliberate deltas from the finding's decode-only wording:

- `enqueueDelayedWill` validates the record before persisting it (body only — it
  builds the subject from those same fields). On failure the Will takes the
  existing immediate-publish fail-safe. Persisting a record every reader refuses
  would convert a sacrificed Will Delay into permanent loss PLUS a permanent
  operator diagnostic. No production caller can reach the arm; a crash between the
  refusal and the publish loses the Will, which is inside the stated guarantee
  because durability begins at acknowledged outbox persistence and nothing was
  persisted.
- `willTakeoverFence`'s inline origin check is deleted rather than duplicated. Its
  `OriginSessionSeq != 0` / `mqttValidSessEpoch` test is now the validator's, and
  the fence documents the precondition that every production caller hands it a
  validated record. Without the writer-side check above this deletion would be
  unsound, which is why the two land together.

Erroring the cancel pass on a refused record was considered and rejected. The
reconnect caller deliberately ignores that error and proceeds — the watermark is
the authoritative suppression — so it buys no safety while logging an unresolved
cancellation on every reconnect of that client forever. It also misstates the
semantics: a refused record cannot fire either, so it is not an unresolved
*pending* record. `cancelWillOutboxScoped`'s contract comment now scopes "every
pending record was resolved" to the records the broker can interpret.

Deleting a refused record (the F-056 eviction pattern) was rejected too. Retained-
message eviction removes a cache entry the store re-derives; the outbox record is
the only durable copy of a Will-shaped fact, and deletion is a mutation of state
the broker has just declared it cannot interpret.

Rolling upgrade over a tree of valid records is behavior-identical: every record a
released writer could produce validates, and new enqueues use fresh per-WillID
subjects, so a refused record never contends for a live Will's subject slot. A
mixed-version fleet in which an old broker still claims records a new broker refuses
is unsupported outright, per the pre-release compatibility boundary.

Operational residual, stated in `docs/ARCHITECTURE.md` and in the diagnostic rather
than mitigated. Both scans walk their filter serially with one JetStream get per
matching record, and refused records are never deleted, so:

- `scanWillOutbox` restarts from sequence 1 each interval, so `N` refused records add
  `O(N)` requests to every scan on every broker, forever, and a large enough backlog
  can push a scan past `mqttWillClaimInterval` and delay valid due Wills.
- `cancelWillOutboxOnce` walks one client's filter and `mqttProcessConnect` waits for
  it **before the CONNACK**, so `N` refused records for one client add `O(N)` serial
  round trips to every reconnect of that client, forever. Returning nil on a refused
  record avoids the retry loop and the spurious error log, not the latency; a large
  enough per-client backlog is an availability problem for that client.

This is the `DenyDelete` accumulation hazard reached by the other route — there
deletion is denied, here the bridge declines to delete what it cannot read — so the
refusal is not a stable resting state and the log asks for prompt cleanup. A
quarantine stream, a bounded scan and a reaper were each rejected as a second
mechanism deciding the fate of records already declared uninterpretable.

Second residual, pre-existing and now written down rather than introduced: retry
exhaustion in `enqueueDelayedWill` means UNCONFIRMED, not proven absent. A create can
land while its ack is lost, and if every bounded re-read is also indeterminate the
loop reaches the immediate-publish fail-safe with a durable pending record in place —
so the Will publishes now and fires again later. An earlier draft of
`publishWillFailSafe`'s contract claimed no record can exist at that point, which was
false; the comment now separates the writer-refusal arm (provably no record) from the
exhaustion arm (the sanctioned at-least-once duplicate reached by a second route), and
`TestMQTTWillOutboxEnqueueExhaustionAfterLandedWriteDuplicates` pins it.

Tests: `will_outbox_refusal_test.go` — the claim scan, the expired-lease reclaim and
the terminal reaper each refuse eight unsupported shapes (missing generation,
out-of-alphabet generation, zero origin sequence, malformed origin epoch,
foreign-identity binding, dotted `WillID`, unknown state, undecodable body) while a
valid sibling in the same pass fires or is reaped; the cancel pass refuses three
shapes, cancels its valid sibling and still returns nil; the loader errors on a
decodable-but-unsupported record and still loads a valid control; and the writer
refuses an empty generation, persists nothing and publishes once, with a valid
control that persists and does NOT also publish. Every "untouched" assertion reads
the STORED BYTES, never `loadWillOutbox` — which now errors on exactly those
records, so a decoded assertion would hold vacuously. The two watermark tests that
deliberately persist an invalid origin epoch
(`TestMQTTWillOutboxWatermarkMalformedEpochCharsetRejected`,
`TestMQTTWillOutboxWatermarkInvalidOriginRefused`) were rewritten as raw-storage
refusal tests rather than given valid fixtures, which would have deleted the
coverage they exist for.

Seven mutations were run, each failing exactly its paired variant: dropping the
generation check (both generation rows), the claim scan's validation call (all three
scan tests), the cancel scan's (all three cancel rows), the loader's (both loader
rows), the writer's (the enqueue test), the subject binding (the foreign-identity
row) and the single-token check (the dotted-`WillID` row).

`internal/mqtt/will_outbox.go`, `mqtt.go`; `will_outbox_refusal_test.go`,
`will_cancel_test.go`, `will_outbox_test.go`, `will_outbox_cluster_test.go`,
`sess_persist_evict_test.go`, `sess_rejected_write_test.go`.

### Original finding record

Preserved from `pre-release-migrations.md` as it existed on 2026-08-10.

**Title:** Reject generationless delayed-Will outbox records

- Status: Open
- Classification: Pre-release outbox-format compatibility cleanup
- Release treatment: Immediate cleanup before further MQTT-core implementation

#### Evidence

Every current session has a validated NUID incarnation generation, and session
records without one already fail restoration with recreate guidance. Every current
delayed-Will enqueue therefore writes `mqttWillOutbox.Gen`.

The outbox still documents and accepts an empty generation as a legacy bucket.
Scoped displaced-connection cancellation may match that empty bucket, while scan
and load paths do not validate the field before a record can be cancelled,
claimed, or published. This is inconsistent with the same record's
`OriginSessionSeq` and `OriginSessEpoch`, which explicitly reject legacy or
migrated forms.

The empty-generation allowance is small, but it silently preserves a
development-era outbox format and weakens the invariant used to scope delayed-Will
cancellation to one session incarnation.

#### Commit-sized resolution

Land one delayed-Will format-hardening commit:

1. Centralize validation of every decoded outbox record, including a valid
   session generation, origin sequence, epoch, identity fields, and state.
2. Make scan, point-load, and cancellation paths treat a generationless or
   malformed record as unsupported/corrupt and never publish or mutate it as if it
   were current state.
3. Remove empty-generation legacy wording and tests; add fail-closed coverage for
   pending and publishing records plus actionable diagnostics.

The resolution must not change unscoped reconnect cancellation of valid records or
the generation-and-origin-sequence scope used by displaced-connection cleanup.

## F-051

**Title:** Bound a connection that never sends CONNECT

- Status: Resolved
- Classification: Confirmed connection-liveness defect
- Release treatment: Completed; one new observable behavior, one new option
- Original dossier: `dead-code.md`

### Resolution

`internal/mqtt/connect_deadline.go` bounds every accepted connection between
accept and CONNECT. MQTT 3.1.1 / 5.0 [3.1.4] says the Server SHOULD close a
Network Connection that does not deliver a CONNECT in reasonable time; the broker
did not, because the timer the ported code armed was unreachable. `createMQTTClient`
gated it on `s.info.AuthRequired || s.mqtt.authOverride`, and neither input has a
writer in this repository — the first has no writer at all, and the second's only
writer, `mqttConfigAuth`, has no caller. So `setAuthTimer` was never called and a
peer that opened a socket and sent nothing held that socket, its `s.clients`
registration, its `clientWG` token, a readLoop goroutine and a writeLoop goroutine
until it hung up. A peer that sent a partial CONNECT and stalled did the same:
`mqttParse` buffers the incomplete packet and returns, and the keep-alive read
deadline is armed from `cp.rd` only after CONNECT.

The bound is **unconditional**, which is deliberately wider than the finding's
"when authentication passthrough is configured" wording. The dossier's own
evidence states the defect unconditionally, `-auth-passthrough=false` is a
supported configuration under which the connection holds exactly the same
resources, and gating would have invented a `docs/CONFORMANCE.md` residual for
nothing. It also keeps the ported `TestMQTTAuthTimeout` honest: that test installs
no dialer, so under the gated reading it would have stayed vacuous forever.

Three parts are load-bearing.

**The arbitration is a state under `c.mu`, not `Timer.Stop()`.** Exactly one of
two racing facts wins: the CONNECT arrived (`mqttAdmitConnectLocked`) or the
deadline expired (`mqttExpireConnectLocked`). `Stop` reports that the timer had
already fired; it says nothing about whether the callback has already decided to
close, and it cannot order itself against a callback that is about to. The loser
does nothing — a late callback that finds the CONNECT admitted leaves the socket
alone.

**The refusal precedes `isClientAuthorized`.** A CONNECT that lost the race
returns `errMQTTConnectDeadlineExpired` before the per-client NATS dial, the
account session manager lookup and every session mutation, so there is nothing to
unwind. CONNECT authentication is otherwise untouched: `authViolation` remains the
authorization failure path, a different fact from the deadline.

**The callback closes `c.rawConn` only**, never `closeConnection`, so the ordered
teardown stays with `readLoop`'s `disconnect()` (which runs
`mqttHandleClosedClient`, nil-safe pre-CONNECT, first) and any TLS layer above the
accepted socket comes down with it. The timer is stopped in `closeConnection`, the
teardown every connection that can have an armed timer reaches; on a malformed or
non-CONNECT first packet the admission state stays `pending`, because neither
racer won.

A read deadline was rejected in favor of a timer: `mqttParse` re-arms one only
while `rd > 0`, so a client negotiating Keep Alive 0 would never clear a
previously-set absolute deadline and would be killed mid-session at the
pre-CONNECT window.

Renames and surfaces: `client.authTimer` became `connectTimer` (plus
`connectAdmission`), `setAuthTimer`/`clearAuthTimer` were deleted, and
`closeConnection`'s previously unlocked timer read moved under `c.mu`.
`internal/config`'s dead `AuthTimeout` became `ConnectTimeout` with a real
`-connect-timeout` / `MQTTNB_CONNECT_TIMEOUT` (default 5s), refused at `<= 0`
because the bound is fail-closed. No alias and no deprecated name were added:
neither name was reachable by an operator before this change, so there is no
deployed configuration to accommodate.

`TestMQTTAuthTimeout` was tightened rather than left alone. With the timer never
armed, its negative case read a success CONNACK, published, and passed; it now
requires that no CONNACK arrives.

## F-053

**Title:** Remove declarations unused by production and the full suite

- Status: Resolved
- Classification: Confirmed dead code
- Release treatment: Completed maintainability cleanup
- Original dossier: `dead-code.md`

### Resolution

The production and `mqttport_fulltests` reachability audit identified 15
declarations in `mqtt.go` with no caller. A final repository scan added the
orphaned `isValidName` and `containsAny` helpers from `compat_helpers.go`.

The independent slice removed `mqttWSPath`,
`errMQTTUnsupportedSubOption`, the unlocked `mqttTakeWill` wrapper,
`mqttConfigAuth`, `getMQTTClientID`, `loadNextMsgFor`,
`sharedConsumerHasOtherHolders`, and the two compatibility helpers. Live Will
consumers already hold `c.mu` and continue to use `mqttTakeWillLocked`; live
shared-holder scans already use the batched helper; and forward scans use
`loadNextMsgFromSeq`. Removing `mqttConfigAuth` also stranded `authOverride`.
That bit had no runtime writer because the function itself had no caller, and
standalone authentication remains at the per-client NATS dial boundary.

The integration slice removed three inherited mechanisms that cannot be selected
by the standalone broker:

- configuration-reload propagation through `mqttUpdateMaxAckPending`;
- embedded multi-account routing through `mqttGetJSAForAccount` and
  `mqttStoreQoSMsgForAccountOnNewSubject`;
- the embedded global-user retained-permission rescan through
  `mqttCheckPubRetainedPerms`, `mqttPerm`, `generatePubPerms`, `pubAllowed`, and
  its private asynchronous `storeMsgNoWait` wrapper.

The `Server.accounts` and `Server.users` shim fields and the `gsl` import went
with those mechanisms. The NATS `User`, `Permissions`, and `SubjectPermission`
aliases remain because supported upstream-reference tests still use them; the
JSA send queue remains because live publish and management paths use it.

No live call site changed. Default, full tagged, and unrestricted reachability
passes were run after each slice. The default race suite exposed unrelated
parallel timing failures on one loaded run; every named failure passed in an
immediate isolated race rerun, and an independent package race run passed.
