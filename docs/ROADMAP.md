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
  claimed, the ordinary-close in-memory/delete crash window is unchanged, and the
  deferred close still does not invoke this purge. Three follow-ups it deliberately
  leaves are the next three bullets.

- [ ] Call `purgeSharedPending` from the deferred-disconnect close path — as ONE
  classified operation with that path's own record write, not as a bare call.
  `applyDeferredDisconnect` (`sess_uncertain.go`, the session-survives branch)
  records the disconnect time, saves and arms the expiry, and never purges, so
  the uncertain-write close leaks exactly what `mqttHandleClosedClient` frees:
  the session's pending shared-group deliveries keep their packet identifiers and
  Receive Maximum slots, and a resume can NAK-steal group messages AckWait would
  have redelivered. Under a Receive Maximum of 1 a single stranded entry stops
  the session delivering at all. Both callers reach the function only for an
  UNBOUND session holding the ownership gate, so the purge itself would be safe
  there.
  **The bare call is not, and a review round proved it** — recorded here so the
  next attempt does not repeat it. `purgeSharedPending` can itself write the
  record: when both register-stream rungs fail, `deleteInflightRegisters` falls
  back to `persistVoidedRegs`, whose `saveClassified` can land ambiguously and
  set `seqUncertain`. `saveWithDetail` then REFUSES every later write (`mqtt.go`,
  "an earlier record write is unresolved"), so the branch's own save is refused
  after `takeDeferredDisconnect` has already consumed the parked action and
  nothing will retry it: `disconnectedAt` survives in memory only, while the
  landed purge record still carries `DisconnectedAt=0`, and the expiry then sees
  a session that looks connected by the record and superseded in memory. So the
  fix must open the record transaction first, stop the purge from initiating an
  independent save, persist the fallback nonces and the original disconnect time
  together, and keep the retry/resolve continuations when that save is ambiguous.
  Test it with a failed register delete AND tombstone plus a landed-but-lost
  response, then assert the persisted deadline survives resolution and restart —
  a test that only covers successful register cleanup proves nothing here.
  Spec5 [4.8.2], [4.9].

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

- [ ] Investigate: a reconnect racing its own DISCONNECT(expiry 0) close can
  resume the session it ended. Observed three times under heavy concurrent
  load in `TestMQTTv5SessionExpiry/disconnect_overrides_expiry_to_zero`: the
  reconnect arrives while the old connection is still bound ("Replacing old
  client" takeover), the old connection's close then finds `sess.c != c` and
  returns without running the expiry-0 discard, and the CONNACK reports
  Session Present=true for a session the DISCONNECT ended at close. Spec5
  [3.1.2.11]: the session must end when the network connection closes.
  Evidence so far: 3 failures, all on a loaded machine (one in a full-suite
  run concurrent with a `-race` suite, two in a probe window confounded by
  compilation); 0 in 120 further targeted runs on the same tree and 0 in 100
  runs at `ac49002` under controlled identical load, so neither "introduced
  by the register-claims change" (whose code provably does not execute in
  this shape) nor "pre-existing" is EVIDENCED yet — the mechanism analysis
  above says pre-existing, the counts say only "rare and load-sensitive".
  Needs a deterministic reproduction (hold the old connection's close between
  the rebind check and the teardown, then CONNECT) before any fix.

- [ ] Investigate: the Phase-2 shutdown scan-stop can miss its between-records
  window under load. Observed once in a full (sequential) `mqttport_fulltests`
  run: `TestMQTTWillOutboxWatermarkPhase2ShutdownStopsScan` counted 2 marker
  loads where the loop-top `willClaimStopCh` check should have stopped the scan
  after the first record ("scan performed 2 marker loads"). 30/30 standalone
  passes on the same tree, nine prior full-suite passes, and the Will-cancel
  machinery is untouched by the register-claims change (last touched b70e9d9 /
  3a25be2), so this is either a scheduling-sensitive test assumption or a real
  ordering gap between `closeWillDispatch` and the scan loop's stop check.
  Needs the same treatment as the expiry-race bullet above: a deterministic
  reproduction before any fix.

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

- [ ] Work the follow-ups the in-flight-register stabilization pass deliberately
  deferred. The pass ran under a stopping rule — release-block only on identifier
  reuse, identifier loss, QoS 2 duplication, phantom Receive Maximum consumption, or
  crash loss of a committed teardown verdict; record everything else here instead of
  reopening the design — and these are what it recorded:
  - Rename the verdict surfaces to match what they became: the persisted field is
    still `CancelledRegs` and the session set `cancelledRegNonces`, kept that pass to
    minimize churn, but they now hold void-register verdicts for TRANSMITTED
    deliveries too, not only cancelled allocations. Schema renames are still free
    pre-release (see the compatibility boundary).
  - Retire the crash-window verdict residual properly: a verdict whose register was
    confirmed-deleted just before a crash is kept forever, because retiring it on
    scan-time absence needs a fence proving a superseded broker's late create-only
    CAS can no longer land, and a time-based fence would rest a correctness invariant
    on a timing assumption. Design a real settlement fence (for example, condemned
    verdicts retired only by a later incarnation's scan once cross-broker writer
    liveness is provable) instead of a window.
  - Reap parked retirements: a retired QoS 2 register whose consumer is gone is
    deliberately left in place (delete-or-leave — a tombstone would destroy the
    suppression), which can park a retirement and its register forever. It reserves
    no identifier and no Receive Maximum slot, so it is stale metadata, but the
    orphan sweep could learn to reconcile it against source absence.
  - Verdicts for registers whose every store attempt definitively failed linger
    until a scan happens to see a register that will never exist; the store loop
    could prove "no attempt was ambiguous" and retire its own verdict at abandonment,
    the same proof `releaseCancelledReservation` already uses for the no-CAS case.
  - Drive the UNSUBSCRIBE ambiguous-save continuations (`undoUnsub`/`commitUnsub`
    reopening the delivery barrier and withdrawing transmitted verdicts) through a
    packet-level readback test; the behavior is pinned at the unit level today, and
    the wiring only by the definite-failure path.

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

### Durable enforcement of the session/marker coupling

- [ ] Decide whether the `$MQTT_sess` + `$MQTT_willcancel` "one logical unit"
  constraint should be durably enforced at startup rather than documented.
  Raised by the stream-prerequisite audit's review (round 7): deleting only
  `$MQTT_sess` mints a new incarnation epoch while permanent watermarks keep
  the old one; the fence then classifies them stale and falls back to the
  session-record rule, whose comment already names "unsupported partial
  recreation" as the carrier of this documented residual
  (`will_outbox.go`, `sessionRecordFence`). The reviewer proposed echoing the
  authoritative epoch into `$MQTT_willcancel` metadata and refusing a
  mismatch before publishing the manager. Deciding needs its own design pass:
  the echo is new arbitration-bearing durable state (who writes it, create
  races between initializing brokers, bootstrap when the marker stream is
  created first, self-heal on the SUPPORTED coupled recreation), and refusal
  on mismatch inverts `TestSessionManagerEpochRecreationYieldsDifferentEpoch`,
  which has pinned recreate-then-start since the watermark item landed. Not a
  release blocker: the exposure requires an administrative action the
  operational constraints in `docs/ARCHITECTURE.md` already forbid.

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
