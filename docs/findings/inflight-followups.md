# In-flight-register stabilization follow-ups

This record intentionally contains follow-ups of different sizes and release treatments.
Split its sub-items before scheduling implementation.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10.

## F-046

**Title:** Complete the deferred in-flight-register stabilization follow-ups

- Status: Open
- Classification: Mixed stabilization follow-ups
- Release treatment: Split before scheduling; most work is a post-alpha candidate
- Original roadmap order: 46

### Original finding record

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
    **Resolved 2026-10-17** (ROADMAP §1, "In-flight verdict schema names"): renamed to
    `VoidedRegs` (JSON `voided_regs`) and `voidedRegNonces`, with `forgetVoidedReg` and
    `stageVoidVerdictLocked`; no migration.
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
  - ~~Drive the UNSUBSCRIBE ambiguous-save continuations through a packet-level
    readback test~~ — superseded: `undoUnsub` is now the definite-failure path only,
    and `commitUnsub` is driven at packet level by the UNSUBACK-truthfulness tests
    (`unsub_ack_truthful_test.go`) plus `TestMQTTUnsubAmbiguousLandedSaveStillCommits`.
