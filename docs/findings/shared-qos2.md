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
  session ended" (`docs/ARCHITECTURE.md`, claim state) and which [MQTT-4.8.2-6] permits on
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
  terminated rather than redistributed, which is the [MQTT-4.8.2-6] outcome for the wrong
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
  (`qos2_shared_affine.go:1630-1642`), which discards it, acks the source and reaps it on
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

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 50

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
