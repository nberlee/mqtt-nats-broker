# Reclamation of the per-CONNECT permanent records: retain under the current protocol

- Status: Decided 2026-10-23 (Codex consult). All three classes are retained; the residual
  in `docs/CONFORMANCE.md`, "Per-CONNECT permanent records accumulate for the recovery
  epoch, and a reconnect loop can exhaust storage", remains.
- Release treatment: Not an alpha precondition (`docs/ROADMAP.md` §2, "Performance and
  resource audits").
- No finding ID. No implementation item follows; reopening needs the conditions below.

This dossier records the design audit the roadmap asked for: what a reclaimer of these
records would have to prove, and why nothing in the current protocol can prove it. It does
not claim reclamation is impossible under every protocol. It claims that the visibility a
sound reclaimer needs does not exist today, per class, and names what would have to exist.
Source locations are under `internal/mqtt/` and were read at `29c00b4`.

## The records

Every successful CONNECT leaves at least four records that nothing deletes within the
account's recovery epoch:

| Class | Stream | Subject | Per CONNECT |
|---|---|---|---|
| Acceptance fact | `$MQTT_sess` | `$MQTT.accept.<domainTk><cidHash>.<epoch>.<acceptID>` | one |
| Handoff receipt | `$MQTT_sess` | `$MQTT.handoff.<domainTk><cidHash>.<intentID>` | one |
| F-105 transition outcome | `$MQTT_inflight` | `$MQTT.inflight.f105.txn.<idHash>.<txn>` | at least two |

Each record has its own subject, so `MaxMsgsPer: 1` does not bound them, and neither
stream can carry global limits without evicting live records. The teardown receipt
(`$MQTT.trcpt.>`) is a neighbour on `$MQTT_sess` keyed per discarded generation, not per
CONNECT; it is out of this audit's scope and covered by the same residual.

The audit covers the three classes together because reclaiming one leaves the per-CONNECT
growth in place.

## Acceptance facts

**Writer.** Only `saveAcceptWithOwnership` (`sess_persist.go`), as member 1 of the
three-member accept batch (fact, receipt, session record; `handoff_metadata.go`). Each
attempt mints a fresh `acceptID`; a retry re-sends the same frozen bytes.

**Readers.**

- `readAcceptFactSeq` (`sess_accept_fact.go`), used only by `mintAcceptanceTenure`
  (`shared_lifecycle_ops.go`). It reads the current CONNECT's own fact by exact subject
  and digest and refuses the CONNECT unless it reads Landed. The delay is bounded by the
  CONNECT.
- The delayed-Will scans `scanAcceptFacts` and `loadNextAcceptFact`
  (`sess_accept_fact.go`), from `cancelIfSuperseded` and `tryFireWill` (`will_outbox.go`).
  They read by subject filter, never by `acceptID`: from `OriginSessionSeq + 1` within the
  origin's epoch, from 1 across epochs, either start raised by a persisted scan floor. A
  no-message result or a fact beyond the cutoff yields `mqttFactNone`. In the cancellation
  scans that means no act; on the firing path it licenses the PUBLISH, which is persisted
  as the decision. There is no count or other completeness check: completeness rests on
  facts never being removed, so a fact deleted before or during a scan reads as absent and
  can license a publication.

The uncertain-acceptance resolver once read an attempt's own fact as well. That readback
was deleted with the superseded mechanisms on 2026-10-19 (`docs/findings/resolved.md`), so
`mintAcceptanceTenure` is the only reader of an acceptance's own fact.

**No durable anchor for a held delayed Will.** `enqueueDelayedWill` (`will_outbox.go`)
returns `mqttWillEnqueueUnavailable` when the service connection's account is unavailable
or does not match the Will's authorized account, before the outbox record is built or
stored. Its callers retain the Will in the in-memory queue only on that outcome
(`mqttHandleWill` in `will_disposition.go`; `dischargeDeferredWill` in
`sess_uncertain.go`, also reached on takeover). The persisted session Will
(`mqttPersistedWill`, `sess_state.go`) is vestigial: CONNECT sets it to nil and nothing
reads it back, and the session record carries no Will identity, origin or FireAt. So
nothing durable names a held Will. Under the current destination and protocol it cannot be
registered at hold time either, because the hold exists precisely because that destination
is unavailable from that broker.

**Passing FireAt releases nothing.** A retained entry is re-enqueued with its original
FireAt and origin (`dischargeOwedDelayedWills`, `will_delayed_retry.go`), the enqueue does
not compare FireAt with the current time, and the record is then arbitrated by `At <
FireAt` and the origin rule. The queue has no age bound: an entry leaves on a
non-Unavailable outcome or at shutdown, and the capacity bound rejects new retention
without evicting existing entries.

**Counterexample.** The owed-Will interleaving in the `docs/CONFORMANCE.md` residual, "Why
the evidence is kept", point 2: a Will held in broker 1's queue with origin `r0` and
deadline `T`, two reconnects through broker 2 leaving `r0 < f1 < r1 < f2 < r2`, a reaper
that sees no outbox record taking `f1`, and the Will published although the client
reconnected within its delay.

## Handoff receipts

**Writers.** COMMITTED as member 2 of the accept batch; ABORTED by `abortMetadataAttempt`
(`handoff_metadata.go`) from `settleProposal` and `settleFailedMetadataCommit`. Every one
of them is create-only (expected last subject sequence 0).

**Reader.** `resolveMetadataAttempt` (`handoff_metadata.go`); any load error reads
Unresolved. Its callers are `settleProposal`, `resolveOrRetryMetadataCommit`,
`settleFailedMetadataCommit`, `loadConnectOwnershipInput` (which reads the drive's own
slot whatever the authority now carries), the `abandonOwnProposal` worker launched after a
refused CONNECT, and `checkMetadataVerdict` inside `commitAuthorityTransition`.

**Durable references.** The intent ID lives in the `$MQTT_inflight` authority body
(`Creation.ID`, `Intent.ID`) and, inertly, in transition outcomes' `Intent`. A crash-left
proposal is settled at an unbounded delay by whoever next runs `settleStandingProposal`:
the next CONNECT, a discard at clear, or teardown adoption, expiry and sweep.

**Reference removal is not permanent.** A live accept drive reuses its intent ID across
every step it takes, so after the authority dropped the ID the same drive can re-reserve
over a Closed authority or prepare a handoff under it. The latch that stops re-proposal
(`ownProposalAborted`, `outbound_connect_drive.go`) is set only once the drive has
observed its own abort, and a deleted ABORTED slot reads Unresolved, so it is never set.

**Counterexamples: a late writer re-creates a deleted slot, in both directions.**

1. *Deleted ABORTED, then a late COMMITTED.* A frozen batch resend
   (`resolveOrRetryMetadataCommit`, `outbound_connect_accept.go`) whose provenance was
   checked before it was suspended commits if its remaining expectations still hold: the
   fact is create-only and the record is conditioned on its base sequence. The provenance
   read is on `$MQTT_inflight` and the batch on `$MQTT_sess`, so the two are not atomic.
   Deleting a COMMITTED receipt alone does not let its own batch replay: that batch's fact
   still exists and the record has advanced.
2. *Deleted COMMITTED, then a late ABORTED, then a serving generation closed.* A delayed
   `settleFailedMetadataCommit` or `settleProposal` reads the deleted slot as Unresolved
   and writes ABORTED create-only. The drive takes SelfAborted and runs its cleanup
   `opCloseGeneration(gen, tenure, txn)`. Finalization kept the reservation's `OwnerTxn`
   and installed the same generation and tenure (`opFinalizeClaim`, `outbound_ops.go`),
   and `checkMetadataVerdict` does not examine a predecessor that is not Reserving. So the
   close matches all three identities of the authority that is now SERVING, and closes it.

**Unreadable authority.** Absent, Corrupt and Indeterminate authority reads are not
proofs. The authority is never deleted in normal operation, so Absent after it existed
means a wiped stream or the upstream `MaxMsgsPer` revival defect, either of which can hide
or resurrect a body carrying the ID.

A tombstone in place of a receipt would keep every create-only writer failing, but it
keeps the subject, so it is not reclamation.

## F-105 transition outcomes

**Writers.** `commitAuthorityTransition` (`outbound_transition.go`), whose outcome member
is create-only, and `abortTransition`, which competes for the same subject create-only.

**Reader.** Only `loadTransitionOutcome`, through `classifyAgainstOutcome` and
`resolveTransitionHandle`, reached only from an in-memory `mqttTransitionHandle`
(`outbound_authority.go`) that is never serialized. An absent outcome reads Unresolved. No
code loads an outcome from a durable transaction field: the `Txn`, `OwnerTxn` and
`PrevTxn` fields are used for CAS and ownership comparisons only.

**Long-lived holders.** Handles are held by the disposal instances
(`outbound_disposal.go`, per generation, re-armed on a timer), the custody ledger
(`outbound_custody_ledger.go`, per generation), admission continuity
(`outbound_admit_continuity.go`, which re-resolves already-landed handles on each
absent-exchange classification), source retirement (`outbound_source_retire.go`, while the
object is resident) and retained abandonment (`outbound_retained_abandon.go`, for the
process lifetime). Re-reading one handle after an unbounded delay within one process is
legitimate in four of them.

**Crash and liveness.** Each holder's durable obligation is recovered from durable state
(registers, exchanges, the authority, resource records, tenure settlement), and old
attempts are fenced by the authority CAS. That is recovery of the obligation, not
reconstruction of every local decision. A crashed process leaves no handle, but that does
not fence the requests it already issued. And nothing durable records when a live
process's holder has exited, so no reaper can tell that an outcome is no longer
referenced.

**Counterexample: a holder's own abort turns a deleted commit into "never landed".**
Transition `T` commits outcome `O(T)` = COMMITTED. A reaper deletes `O(T)`. The custody
holder's settle step (`settleCustodyHandle`, `outbound_custody_session.go`) resolves and
reads Absent, so Unresolved; it aborts create-only, which now succeeds and writes `O(T)` =
ABORTED; it resolves again, and `classifyAgainstOutcome` returns NeverLanded on the
aborted outcome before consulting the authority. The attempt is dropped although the
authority carries `T`'s effect. Source retirement differs in shape but not in result: a
successful abort drops the claim directly, so there too a deleted COMMITTED outcome is
taken as not landed.

A registry of live handles would itself be records, needing registration before exposure,
transfer, release, crash retirement and its own reclamation, and crash retirement cannot
rest on a timeout, which does not tell a crashed broker from a paused or partitioned one
that resumes.

## Why each candidate mechanism falls short

- **A watermark over facts** (`L`, with or without keeping the current fact): refuted by
  the owed-Will interleaving; the unamended rule also takes the fact
  `mintAcceptanceTenure` reads.
- **A per-client authority join for receipts** ("delete a receipt no authority
  references"): a live drive reintroduces the ID, a captured reader reads its own slot
  regardless of the authority, and a late create-only writer re-creates the slot.
- **A tombstone for receipts:** keeps the subject, so reclaims nothing.
- **A registry of outcome readers:** needs a full lifecycle with timeout-free crash
  retirement, and would still leave the writers unfenced.
- **An admission bound** (rate or ceiling): a rate bounds the slope over an unbounded
  epoch, and a ceiling without reclamation eventually refuses legitimate CONNECTs; both
  need an availability policy nobody has decided (the 2026-10-12 decision).

## Failure direction of a wrong reaper

- Facts: a deleted fact reads as absent, and the firing path publishes a Will a timely
  reconnect should have suppressed: a false affirmative built on an absence. Deleting an
  acceptance's own fact refuses its tenure, and so the CONNECT.
- Receipts: a late writer re-creates the slot with the opposite verdict, up to closing a
  serving generation (counterexample 2).
- Outcomes: a holder's own abort turns a committed transition into NeverLanded, and the
  CONNECT ownership step refuses a CONNECT whose step reads Unresolved.

## Reopening conditions

A future proposal must meet these obligations or replace each with an equivalent fence or
proof. They are sufficient conditions drawn from the counterexamples above, not the only
possible implementations.

1. **Facts.** Every delayed Will that a fact could still suppress is durably discoverable,
   for example through an anchor written before any outage (at CONNECT or close) that
   stays discoverable across session replacement until the Will is discharged or fenced.
   The current acceptance's own fact is kept for `mintAcceptanceTenure`. Deletion is
   coordinated with scans in progress, since a scan reads a deleted fact as absent.
2. **Receipts.** Before a delete, a Valid authority read shows the intent ID gone past a
   terminal transition for it, and every live drive and every create-only writer of the
   slot, including requests already issued, is fenced. Non-Valid authority reads abstain.
3. **Outcomes.** Every reader and every writer of an outcome has a durable lifecycle, with
   crash retirement not based on a timeout and already-issued requests fenced.
4. **Together.** The proposal covers all three classes, since reclaiming one leaves the
   per-CONNECT growth in place.
