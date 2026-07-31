# Roadmap

This document is the authoritative checklist for the first public alpha.
Until every release blocker is complete, do not push release artifacts, publish
container images or Helm charts, or create an alpha tag.

The sections are ordered by dependency. Within a section, implementation may
proceed in parallel where the work does not overlap. Any new correctness,
security, interoperability, or data-loss finding becomes a release blocker and
is fixed before feature work continues.

## 0. Resolve known findings

- [x] Remove the accidental review sentence from the README attribution section.
- [x] Make MQTT TLS startup fail closed. An invalid or unreadable certificate,
  key, or CA must never result in a plaintext listener.
- [x] Reconcile the README TLS/mTLS feature claim with the missing
  certificate-rollover behavior. The README now documents working TLS and mTLS
  and states explicitly that live rollover is not implemented; rollover itself
  remains tracked under "TLS and certificate rollover" below.
- [x] Resolve the MQTT core TODO concerning JetStream domain assignment.
  `-js-domain` is now the single validated source of truth (the unreachable
  per-account and server-level branches are gone), and the broker no longer
  fabricates a `ClientInfo.Cluster` from its node name.
- [x] Resolve and document the policy for subscriptions to internal MQTT
  subjects. SUBSCRIBE now denies the whole reserved `$MQTT.>` namespace (not
  only the three delivery prefixes), legacy entries are purged from resumed
  session records, reserved Will topics are rejected at CONNECT, and the
  broker's own delivery subscriptions moved to the service connection so a
  full-namespace deny is deployable. Documented under "Reserved `$MQTT.>`
  namespace" in `docs/ARCHITECTURE.md`.
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
  digest is copied into `pendingWrite` when the failure is recorded, so later memory
  changes cannot alter what the readback compares against. The problem is purely
  that memory has to be reconciled differently in the two cases.) That is why
  keeping the mutation is only half the answer: `retainOnUncertain` registers how to
  undo it, and the readback runs those undos when it proves the record is still at
  the base sequence, or discards them when the stored bytes match. Without that,
  "keep it" silently means "assume it landed" — and for a drain claim, an
  unconfirmed removal that is treated as durable gets the message **acked away**
  (I1), or lets a forwarded PUBREL complete an exchange the record still owes (I10).
  A retained removal is therefore also marked `unclaimUnconfirmed`, which the two
  delivery callbacks check before doing anything irreversible — the QoS 1/2 callback
  before acking a redelivery, the PUBREL callback before forwarding — because each
  makes its own decision after the helper returns and would otherwise act on
  in-memory "not claimed".

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

  A caller that keeps a mutation also owes the readback its **landed** continuation
  wherever its success path does work after the save: UNSUBSCRIBE commits the staged
  drains, clears their inactivity timers, releases shared durables and drops the
  client-facing subscriptions, and skipping that for a write that did land would
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
  never gates readability — the unreadable path drops a message — so a malformed one
  falls back to the record's own store time. A same-broker discard additionally
  refuses to admit a release at all (`closeQoS2ReleaseAndDrain`, mirroring
  `closeQ2AffAndDrain`), closing the local case outright.

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
- [ ] Reap orphaned inbound QoS 2 hold records. Nothing reaps a `held` record whose
  session record was lost, so it wedges its packet identifier until a later PUBLISH
  on that identifier reaps it. Pre-existing: neither created nor worsened by the
  move protocol, which added the terminal tombstone but no new leak.
  Extend the same sweep to the two structures session-affine shared QoS 2 adds:
  an orphaned `$MQTT_Q2AFF_*` consumer whose session record is gone (an older
  broker in a mixed-version window ignores `Q2Aff` entirely), and an assignment
  claim whose winner generation no longer exists and which no contender happens
  to revisit. Neither loses a message — a stalled claim is honored, not
  released — but both accumulate.
  Extend it again to the inbound QoS 2 **release claims** the serialization above
  adds: a `discarded` marker, which no live path may ever reap (it is the only proof
  a late releaser must lose), and a `releasing` claim whose broker died between the
  claim and the commit, together with the hold record it protects. Age them by the
  record's own JetStream store time rather than by the claim's `At` field — a
  self-reported timestamp from a possibly skewed broker clock cannot be validated,
  while the store time is assigned by one stream leader and cannot be forged. `At`
  is required and range-checked, but it is forensic: if a sweep is ever made to rely
  on it, an implausible value must not be resolvable into "reap it now".
- [ ] Give outbound QoS 1/2 in-flight deliveries a durable identity, so a packet
  identifier survives a broker restart or failover rather than only a same-process
  reconnect. Today `pendingPublish`/`cpending` are memory-only, so after a restart
  a started delivery is re-offered with DUP set and a **freshly allocated**
  identifier, and a client is entitled to treat that as a new message. This is the
  single remaining cause of the HA exactly-once deviation, and it is not specific
  to shared subscriptions: it applies to every QoS 1/2 outbound path. A fix has to
  persist the identifier together with the source durable and sequence, the
  PUBLISH-versus-PUBREL phase, and a recovery ordering — which is why it is its
  own item rather than a detail of the shared-subscription work that surfaced it.
- [ ] Audit the remaining `$MQTT_*` streams for protocol prerequisites, deriving
  each list from what that stream's owner actually does rather than copying
  `validateQoS2InStream` wholesale — the required set genuinely differs per stream.
  `$MQTT_willcancel` does CAS writes and permanent retention but never deletes or
  purges by design, so `DenyDelete`/`DenyPurge` are *not* prerequisites there, while
  `NoAck`, `Sealed`, `SubjectTransform`, `MaxMsgSize` and the other write/CAS-affecting
  settings are; `validateWillCancelStream` currently checks state shape only.
  `$MQTT_willq` does CAS writes plus terminal deletion, so deletion prerequisites do
  apply. `$MQTT_sess` does CAS writes, deletion and cleanup. `$MQTT_willq` and
  `$MQTT_sess` have no validator at all today. Note also the new
  `$MQTT_qos2in` subject layout: hold subjects are now
  `$MQTT.qos2.in.<idHash>.<gen>.<PI>` (see "Session incarnations and teardown" in
  `docs/ARCHITECTURE.md`), so any prerequisite derived from the old
  `<idHash>.<PI>` shape must be re-derived. `$MQTT_msgs` gains one the drain
  depends on: a stream-level `ConsumerLimits.InactiveThreshold` would reap a
  drained durable while its session is offline and take its started deliveries
  with it, so draining must refuse rather than degrade where one is set.

## 1. Settle release decisions

These decisions affect binary behavior, package names, images, charts,
documentation, and release artifacts. Make them before creating those surfaces.

### Product name

- [ ] Decide the product name before renaming packages, images, charts,
  workflows, or release artifacts.

Current recommendation: `mqtt-nats-broker` describes the product more
accurately than `mqtt-nats-bridge`. The process terminates MQTT and owns broker
semantics; it does not merely relay between two independent brokers.
`nats-mqtt-broker` is also accurate but may imply that this is an official NATS
project. Retain `mqtt-nats-bridge` only if continuity outweighs that ambiguity.

### Memory-limit strategy

- [ ] Decide whether to use `github.com/KimMachineGun/automemlimit`.

Current recommendation: use the pinned library with a documented 0.9 ratio and
allow an explicit `GOMEMLIMIT` override. Kubernetes `resourceFieldRef` cannot
multiply the memory limit by 0.9, while implementing Kubernetes quantity
arithmetic in Helm is fragile. Acceptance requires cgroup v1/v2 tests and clear
behavior when no container limit is configured.

## 2. Make the MQTT core maintainable

- [ ] Split `internal/mqtt/mqtt.go` along stable responsibilities: protocol
  parsing, connection lifecycle, sessions, subscriptions, retained messages,
  QoS flows, MQTT 5 properties, JetStream persistence, and transports.
- [ ] Keep behavior unchanged during the split and run the upstream and native
  suites after each extraction.
- [ ] Preserve provenance for code derived from NATS Server through NOTICE and
  concise file or package documentation.
- [ ] Do not preserve file layout merely to keep a line-by-line upstream diff;
  maintainability of this broker is the controlling requirement.

### Performance and resource audits

- [ ] Audit every JetStream query and request pattern inherited from the early
  NATS Server MQTT implementation against current JetStream APIs and behavior.
  Look for avoidable request/reply round trips, repeated stream or consumer
  lookups, serial operations that can be batched or run concurrently, full
  scans where direct lookups are available, and polling that can be replaced by
  push or event-driven behavior.
- [ ] Benchmark the current and proposed JetStream paths under connect,
  subscribe, publish, reconnect, retained-message, and session-recovery loads.
  Record request counts, transferred bytes, latency, and NATS/JetStream load;
  change a query path only when measurements show an improvement without
  weakening MQTT semantics, authorization, consistency, or HA behavior.
- [ ] Perform a full memory and resource-lifetime scan of the broker. Cover
  clients, sessions, subscriptions, retained-message caches, QoS state,
  JetStream request state, timers, goroutines, channels, buffers, NATS
  connections/subscriptions, TLS reload state, and shutdown paths.
- [ ] Add churn and soak tests that repeatedly connect, subscribe, publish,
  disconnect, expire sessions, fail over, and reload TLS. Use heap and goroutine
  profiles plus leak detection to prove memory and resource counts return to a
  stable baseline after work drains; fix every confirmed leak before alpha.

## 3. Add MQTT transports

### TLS and certificate rollover

- [x] Support MQTT TLS and mutual TLS as tested production listener modes.
- [x] Expose TLS on a second listener, or document why a single configurable
  listener (`-port`, plaintext or TLS) is sufficient. Decided and documented in
  the README: one listener, `-port` selects it in either mode, no implicit port
  change when TLS flags are set, and TLS deployments use `-port 8883`. Serving
  1883 and 8883 at once would need multiple listeners and is not supported.
- [ ] Reload certificate, private-key, and client-CA changes from projected
  Kubernetes Secrets without restarting the process or pod.
- [ ] Do not require Stakater Reloader or another rollout controller.
- [ ] Replace the active TLS configuration atomically only after the complete
  replacement configuration validates.
- [ ] Keep the last valid configuration active when a projected update is
  incomplete, malformed, or contains a mismatched key.
- [ ] Prove that new handshakes use the replacement certificate while existing
  MQTT connections remain connected.
- [ ] Cover expiry, malformed files, mismatched keys, CA rollover, concurrent
  handshakes, and failed reload recovery in native tests.

### WebSocket

- [ ] Implement MQTT over WebSocket and secure WebSocket.
- [ ] Define listener, path, origin, proxy-header, timeout, and TLS settings.
- [ ] Add native integration tests for MQTT 3.1.1 and 5.0 over WebSocket.
- [ ] Replace or retire the parked upstream WebSocket reference tests once
  equivalent behavior is covered.

## 4. Build the runtime image

- [ ] Build the runtime image `FROM scratch`.
- [ ] Copy the system CA bundle into the runtime image.
- [ ] Run as a numeric non-root user with a read-only root filesystem.
- [ ] Verify outbound TLS connections to NATS, mounted MQTT TLS Secrets,
  health probes, graceful shutdown, and signal handling.
- [ ] Build and smoke-test linux/amd64 and linux/arm64 images in CI.

## 5. Package for Kubernetes

### Kubernetes manifest

- [ ] Replace JSON-like flow mappings and sequences in the example manifest
  with readable block-style YAML.
- [ ] Set the effective Go memory limit to 90 percent of the container memory
  limit.
- [ ] Include secure defaults, probes, resource requests and limits, graceful
  termination, and optional TLS Secret mounts.

### Helm chart

- [ ] Add a production Helm chart for the broker.
- [ ] Include Deployment, Service, TLS mounts, probes, resources, security
  context, autoscaling options, PodDisruptionBudget, and graceful termination.
- [ ] Support an external NATS/JetStream cluster.
- [ ] Add the official NATS chart as an optional dependency.
- [ ] Expose the dependency configuration under `nats:` so users can place
  normal NATS chart values there without translating every field.
- [ ] Add Helm lint, template, install, upgrade, rollback, and optional-NATS
  tests to CI.
- [ ] Package and publish the chart as an OCI artifact in GitHub Container
  Registry only as part of an alpha release.

## 6. Document migration and security

- [ ] Write a migration guide from NATS Server's embedded MQTT listener to this
  standalone broker.
- [ ] Cover listener cutover, DNS or load-balancer changes, expected downtime,
  rollback, and verification.
- [ ] State explicitly whether sessions, retained messages, QoS state, and
  delayed Wills can be migrated. Do not imply persisted-state compatibility
  where none exists.
- [ ] Explain service credentials, anonymous-client defaults, and per-client
  authentication passthrough separately.
- [ ] Provide NATS user/password and JWT/account examples for MQTT
  authentication and publish/subscribe authorization.
- [x] Document MQTT-topic to NATS-subject mapping and denial of the internal
  `$MQTT.>` namespace. See "Reserved `$MQTT.>` namespace" in
  `docs/ARCHITECTURE.md`; the migration guide should link it rather than restate
  it.
- [ ] Include Kubernetes Secret and live TLS rollover examples.

## 7. Finish provenance and release documentation

### Remove porting-era terminology

- [ ] Rename the `mqttport_fulltests`, `mqttport_clustertests`,
  `mqttport_wstests`, and `mqttport_tlstests` build tags and CI references.
- [ ] Preferred replacements are `mqtt_upstream_suite`,
  `mqtt_cluster_reference`, `mqtt_websocket_reference`, and
  `mqtt_tls_reference`.
- [ ] Remove `port`, `ported`, and `porting` as project-identity language from
  the README, CI, lint configuration, comments, and documentation.
- [ ] Keep technically necessary networking terms such as listener port,
  `containerPort`, and port numbers.
- [ ] Replace `docs/PORTING.md` before alpha with concise provenance
  documentation covering the MQTT 3.1.1 source, upstream revision, license,
  modifications, and intentional divergence.
- [ ] Keep attribution in NOTICE and the README after removing the porting
  narrative.

### Release documentation

- [ ] Remove the pre-release changelog before the first alpha.
- [ ] Start a changelog with the first published alpha, or generate release
  notes from Git history and pull requests.
- [x] Replace the README's duplicate roadmap with a short link to this file.
- [ ] Make version-controlled files under `docs/` the source of truth for a
  GitHub Wiki.
- [ ] Publish the wiki from CI so it cannot drift from repository docs.
- [ ] Include installation, configuration, architecture, TLS, Helm/Kubernetes,
  authentication and authorization, migration, operations, troubleshooting,
  conformance, and provenance pages.

## Alpha release gate

The first alpha may be tagged only when:

- [ ] All findings and roadmap items above are complete.
- [ ] The full test, race, lint, conformance, Paho, container, and deployment
  gates pass with no unexplained failures or skips.
- [ ] CI is green from a clean checkout on every supported architecture.
- [ ] MQTT 3.1.1 and 5.0 conformance baselines have no unexplained regression.
- [ ] The scratch image and Helm chart install successfully in a clean cluster.
- [ ] TLS rollover, WebSocket, authentication, authorization, migration, backup,
  restore, upgrade, rollback, and shutdown procedures are documented and tested.
- [ ] Release artifacts use the final product name consistently.
