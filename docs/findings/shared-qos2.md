# Shared QoS 2 and affine restoration findings

These findings cover transactional affine cleanup, malformed-claim recovery, and
truthful restoration of shared-subscription sessions.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10.

## F-047

**Title:** Keep affine assignments when a SUBSCRIBE packet fails but the session
survives

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 47

### Original finding record

- [ ] The packet's two affine unwinds treat a FAILED PACKET as an ENDED SESSION. They mark
  the generation's assignments `Discarded`, which this design defines as "the owning
  session ended" (`docs/ARCHITECTURE.md`, claim state) and which [MQTT-4.8.2-5] permits on
  that basis — but the session survives a failed SUBSCRIBE, and [MQTT-4.8.2-5] requires a
  live session to complete a delivery already started to it. Reachable when a packet
  carries a SECOND shared QoS 2 filter: it re-enters `ensureQ2AffineConsumerSubscription`
  with the consumer present, skips the save and opens the readiness gate while the session
  is still unresolved, so a move can claim and copy under the generation before the
  membership rollback. The settlement reclaim was guarded when this was found — it fences,
  drains, and then RETAINS the path whenever `q2AffineGenerationOwnsClaims` says the
  generation still owes one, leaving teardown as the event that ends it — and the same
  guard is what the two packet sites need. Pinned for the reclaim by
  `TestQ2AffineReclaimKeepsAPathThatStillOwesADelivery`, and its predicate is `terminal()`
  rather than `Discarded` alone so a permanently-retained `Lost` claim cannot pin the path
  for the life of the session (`TestQ2AffineReclaimIsNotBlockedByATerminalClaim`). A
  second, smaller part of the same cluster is untouched: `discardQ2AffClaimsFor` skips a
  claim that is already `Discarded` but will still stamp `Discarded` onto an `AffineDone`
  or `Lost` one, which is imprecise rather than harmful — neither becomes reapable as a
  result — and belongs with the per-site fix because the helper is shared with `clear()`
  and teardown. The gate opening on an unresolved session is itself worth closing
  separately.

## F-048

**Title:** Arbitrate affine cleanup before discarding assignments

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 48

### Original finding record

- [ ] The packet's two affine unwinds discard this generation's assignments BEFORE the CAS
  that arbitrates whether they still own them, so a cleanup CAS lost to a remote winner
  that PRESERVED the generation leaves that winner's started QoS 2 delivery discarded —
  terminated rather than redistributed, which is the [MQTT-4.8.2-5] outcome for the wrong
  session, and unrecoverable because the discard is monotonic. The two sites are
  `prepareQ2AffineUnwind(c, true)` from the packet's unused-path unwind (`mqtt.go:6755`)
  and from `rollbackSubscribePacket` (`mqtt.go:7262`). The settlement reclaim
  `unwindProvisionedQ2AffineOnLanded` was converted when this was found and passes
  `disposeNow` false, disposing only once its cleanup record is confirmed; the remaining
  two were left behaviorally unchanged deliberately, because each sits inside a packet
  rollback with its own failure semantics — converting them is a transactional-rollback
  change needing its own plan and race fixtures, not a parameter flip. Session TEARDOWN is
  the model to follow: `claimTeardown` (`sess_teardown.go:151`) durably CASes an ownership
  marker and resolves every rejection by re-read, and only a WON claim reaches
  `deleteTeardownTargets`. Disposing LATE is recoverable rather than stalling: a claim
  whose record no longer names its generation is detected by `materializeQ2AffOnBehalf`
  (`qos2_shared_affine.go:1800-1816`), which discards it, acks the source and reaps it on
  the next source redelivery. Both of those were verified in the code after a first
  attempt to refute the finding asserted the opposite of each. Reaching the race also
  requires the remote to hold a shared QoS 2 membership and to start a delivery under the
  same generation inside the claim scan. Stated in `docs/CONFORMANCE.md`; the converted
  site's race is pinned by
  `TestQ2AffineReclaimRejectedByAGenerationPreservingWinnerKeepsItsClaim`.

## F-049

**Title:** Provide an operator recovery path for unreadable affine claims

- Status: Open
- Classification: Operational corruption hardening
- Release treatment: Post-alpha candidate if normal broker operation cannot create the
  malformed state
- Original roadmap order: 49

### Original finding record

- [ ] One unreadable or unattributable assignment claim wedges affine reclamation
  ACCOUNT-WIDE, with no way to clear it. The fail-closed behavior is correct and
  deliberate — `scanQ2AffClaims` aborts rather than conclude anything about a record it
  cannot decode, and the orphan sweep skips affine reconciliation entirely on an
  incomplete inventory (`orphan_qos2_sweep.go:191-195`), which is what protects every
  affine consumer from deletion — but nothing repairs or quarantines the offending record,
  so every affine consumer delete and every sweep-driven reap stay blocked for as long as
  it exists. Note the discard itself is STREAMING and therefore partial, not atomic:
  `discardQ2AffClaimsFor` mutates each matching claim inside the scan callback, so the
  claims reached before the malformed record are discarded (and a both-sides-proven one
  may be reaped) while the rest are not — a validate-first two-pass discard is the
  alternative if that partial progress is ever judged unacceptable. Reached by `clear()`,
  by session teardown, and by the settlement reclaim (which releases its local
  subscription and MaxAckPending charge anyway and leaves the rest pending, see
  `docs/CONFORMANCE.md`). What is missing is an
  operator-visible path: surface the offending subject, and either quarantine it out of
  the inventory once it is provably undecodable or provide a supported way to remove it.
  Pinned as behavior by `TestQ2AffineUnreadableClaimFailsScanClosed` and
  `TestQ2AffineReclaimDiscardFailureStillReleasesTheLocalHalf`; raised by the holistic
  review of the ambiguous affine provisioning reclaim.

## F-050

**Title:** Refuse or retry a CONNECT with incomplete shared-subscription restoration

- Status: **Closed 2026-08-23 by refusing, not retrying.**
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 50
- **Filed twice.** `docs/ROADMAP.md` carried this defect as both the §0.3 F-050 entry and
  an unnamed §0.2 bullet added by F-064's holistic review ("a saved shared filter that
  fails restoration at CONNECT is silently dropped"). Same mechanism, same consequence,
  one dossier. Both boxes are ticked by the same change; the §0.2 filing is kept because
  it is the one that recorded the F-064 review's constraint on any fix.

### What closed it

**The success CONNACK follows a complete restoration, or the CONNECT is refused.**
Subscription restoration moved to before the CONNACK, so a filter it cannot install
refuses the CONNECT through the pre-CONNACK abort discipline that already discards this
connection's Will and leaves the requested lifetime ungranted. That is what answers the
F-064 review's objection: the problem was never "refuse" — it was refusing *after*
acknowledging, which publishes the Will and lets a requested expiry of 0 destroy a
persistent session.

**Five reachable exits, each with its own fixture**, because a remedy demonstrated on one
says nothing about the others: `ensurePubRelConsumerSubscription`,
`ensureQ2AffineConsumerSubscription`, the raw subject `processSub`, the plain-or-group
JetStream consumer, and the level-up companion of the last two. The verdict is
`mqttFirstUnrestoredFilter`, which reads a value `processSubs` was always required to set
(on a SUBSCRIBE those bytes become the SUBACK) rather than adding bookkeeping. It is named
**local failure aggregation**: it does not arbitrate ownership, does not prove this broker
still owns the record, does not prove a group durable exists, and does not prove an
installed subscription yields end-to-end delivery.

**The reorder needed exactly one new fact, and it is connection-scoped.** Three plan
revisions were spent establishing that it cannot be per-subscription, for two structural
reasons: a per-subscription flag cannot be armed before the subscription exists — a bare
`c.processSub` makes the NATS handler live *before* it returns, so a stored PUBREL can
enter its callback with no metadata attached at all — and a released gate is
indistinguishable from one that never existed, so a refusal cannot express "woken and
still not allowed out". So transmission suppression is one atomically-published verdict on
the session, consulted by all four delivery callbacks before they touch `sub.mqtt`, armed
unconditionally on every accepted CONNECT and deliberately left installed after a refusal.
`mqttSub` gains no field, `processSub` is unchanged, and the reconciliation phase's
adoption and sweep are untouched. Both terminal transitions are CAS-guarded: they are
exclusive today, and a double channel close panics the process.

Two orderings are load-bearing. The grant point moves after restoration, per its own rule
that the requested lifetime applies only once everything that can refuse has passed. The
3.1.1 forced leave stays *after* the CONNACK, so the pre-CONNACK restore excludes shared
filters for a non-v5 connection **explicitly**, where the old code relied on ordering
alone.

**A restore-path failure also stops being destructive.** It no longer deletes a plain
durable the record still names (reachable when a multi-level filter's level-up companion
fails after the main half succeeded), and it refunds `tmaxack` per filter from the amount
each group join **reported** — not from the durable, because the config comes back whether
a call charged it or reused a resident entry, and refunding by durable erases an earlier
connection's charge and leaves the accounting permanently short.

### Designs refuted in review, so none returns

- **Failing the CONNECT after the CONNACK.** The F-064 objection: the connection is
  `connectReceived`, so a bare error return sends a v5 DISCONNECT as the first packet
  ([MQTT-3.2.0-1]) and close bookkeeping publishes the Will; the lifetime is already
  granted, so a requested expiry of 0 destroys a persistent session.
- **Retrying in place.** A retry that also fails lands in the same state, so the
  disposition question is only deferred; the two JetStream exits already retry internally.
- **Removing the failed filter from the record.** Silently deleting durable client state on
  a transient failure is the shape this finding condemns.
- **Duplicating the provisioning into the reconciliation phase.** Two mechanisms stating
  one fact, and it leaves the raw-subscription exits after the CONNACK.
- **Installing the raw subscription ungated in a final pre-CONNACK sub-pass.** Tried to
  avoid the shared QoS 0 swallow; `mqttDeliverMsgCbQoS0` honours neither `closed` nor any
  gate and writes the packet, so it makes a QoS 0 PUBLISH the client's **first packet**.
  There is no safe version of "ungated raw".
- **Proving the group durable by re-issuing its recorded config.** Its premise is false —
  shared consumer configurations are deliberately not persisted — and
  `ActionCreateOrUpdate` with the stale in-memory one rewrites a live group durable's
  configuration for every other member. Filed as its own roadmap item.
- **Moving the 3.1.1 forced leave before the CONNACK.** Converts an unacknowledged CONNECT
  attempt into a permanent destructive mutation of session state. Filed separately.
- **Carrying the restore state on each subscription.** Killed twice: `phaseOwned` is
  load-bearing both for `sweepUnadoptedPhaseSubs` and for the withheld-first-transmission
  bookkeeping, so no split of it works; and a third per-subscription fact still cannot be
  armed before the subscription exists.

### Residuals

Stated in `docs/CONFORMANCE.md`: a permanently unrestorable filter refuses every resume
(clean start is the recovery, and a v5 client is told the reason); a shared QoS 0 message
can be swallowed during the restore window; **a withheld QoS 1/2 delivery can lose its
never-transmitted marker to a rebind racing the refusal, after which it waits out AckWait
instead of being NAKed forward and its first client-visible PUBLISH can carry DUP — the
lost prompt NAK is this finding's own residual, the DUP derivation is F-072's**; the same
marker is lost for a shared durable even without that race, because the ordinary close
runs `purgeSharedPending`; a timed-out affine gate wait falls into F-072's population; and
the displaced-broker window has no upper bound — the record-state backstop interval is its
first opportunity, not a limit.

Two remedies for the rebind race were built and withdrawn across holistic rounds 1-3, and
are recorded so neither returns: **draining** the woken callbacks before the refusal
returns (a counter cannot be an admission barrier — a callback can be descheduled between
loading the verdict and registering, or arrive after the drain observed zero, and delivery
interest stays live until teardown), and deciding the recording on a **durable-scoped**
predicate rather than the connection (removes the race but not the lateness, and the set of
places a durable can live is neither complete — a record-sourced shared membership's
durable exists only through `SharedSubs` and an in-flight config — nor sequence-precise for
a drain). Closing it needs a real callback-admission boundary, which this finding does not
justify; HEAD already treats the same boundary as best-effort.

Holistic round 4 added one defect to the inventory, and it is F-050's own shape in
miniature: a **malformed persisted shared key** was silently skipped when the filter list
was built, so it produced no failed filter, the aggregation saw nothing wrong, and the
client was told Session Present for a membership nothing restored. It now refuses. The
mechanism is worth recording: a pre-decided failure must be carried in the filter's
`reason`, not only its `qos`, because `processSubs`' first loop clamps any `qos` above 2
down to 2 before it tests the failure sentinel.

### Original finding record

- [ ] A CONNECT restore ACCEPTS a session whose shared filter failed per-filter, leaving a
  persisted membership with no live queue subscription and no group consumer. The
  restoration loop marks the filter failed in `mqttProcessSubs`' first loop and its second
  loop skips it, but `update()` skips failed filters too — so `sess.sharedSubs` keeps the
  restored entry, the CONNACK reports Session Present, and a client that is not required
  to re-subscribe waits forever for shared messages that are never delivered. Reachable
  from ANY per-filter restore failure (a failed group-consumer create, a failed
  subscription, an ambiguous affine provisioning save), not from one path. The fix is a
  choice the CONNECT path has to make explicitly — retry the failed restoration for the
  current binding, or refuse the CONNECT so a retry performs a complete restore — and it
  changes CONNECT acceptance semantics, so it is its own item. Raised by the holistic
  review of the ambiguous affine provisioning reclaim, whose retention test now proves
  delivery after a REAL re-subscribe (`TestQ2AffineAmbiguousProvisioningKeepsAPathAMembershipOwns`)
  and deliberately does not claim delivery without one.

### Downstream

F-027's forked-record repair (closed 2026-08-12) excludes shared-subscription membership
for exactly this ordering: the success CONNACK precedes shared restoration, so a
re-asserted group membership would be durable while a per-filter failure leaves no join
and no consumer — the accepted-but-dead state above. **F-027's repair set widens to shared
membership when this closes**, and its snapshot constructor is where that exclusion lives
(`sess_fork_repair.go`).
