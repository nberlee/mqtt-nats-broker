# Roadmap

This document is the authoritative checklist for the first public alpha. Technical
evidence, rejected approaches, test references, and residual risks live in the
[findings dossiers](findings/README.md); roadmap entries keep only the information
needed to order and gate work.

Until every item classified as an alpha blocker is complete, do not push release
artifacts, publish container images or Helm charts, or create an alpha tag.
Conditional blockers become release blockers when their corresponding feature is
included in the supported alpha scope. Investigations block only when they confirm
a defect or leave a release-gate failure unexplained.

The sections are ordered by dependency. Within a section, implementation may
proceed in parallel where the work does not overlap. Any new correctness,
security, interoperability, or data-loss finding must receive a stable finding ID,
a release classification, and a canonical dossier before feature work continues.

## 0. Resolve known findings

- **F-105 row 5, unit C: a checkpoint within the amended recovery boundary, conditional on
  F-105's integrated gate.**
  Unit A's retained-capacity wake remains preserved; unit B remains stopped.
  Durable abandonment releases charge while retaining identifier custody and
  identity-bound MQTT ACK routing. Session-owned disposal and terminal cleanup
  retain source suppression, validated reply metadata and retry ownership through
  source settlement, including ACK-first terminal/rejected outcomes without an
  abandonment fact. Synchronous recovery installs successor ownership before
  releasing restrictions and persists the drain floor before activation.
  Delayed ACK completions preserve genuinely allocated successors; generation
  retirement discharges old work without changing new allocations. A busy
  disposal cannot be replaced by ordinary recovery: its single-flight lease
  blocks that handoff until the old attempt returns.
  P-12 f6 remains an accepted residual: displaced incomplete recovery may require
  a later successful resume; returning storage alone does not restore progress.
  The original no-reconnect requirement remains unmet. Unit C did not close F-105 or
  F-106 alone; both are ticked in the W3 candidate and NOT accepted as closed until the
  integrated branch review passes, and this entry stays conditional on that gate.

The remaining unit C follow-ups are **NONBLOCKING**:

- C76-WAKE-STRESS: combine timer, callback and shutdown final-pass contention
  beyond the single-flight and retained-owner checks.
- C76-SCOPE-SAMPLING: sample additional QoS, cold-recovery, expiry, oversize,
  affine and indeterminate entrances sharing the abandonment-exclusion boundary;
  the full cross-product is not claimed.
- T-C34 equivalent snapshots: add equivalent crash snapshots beyond the distinct
  intent, materialization, live-token, source-ACK and unreadable-outcome cuts.

- [x] [F-111: Give every recovered reservation a lifecycle
  owner](findings/recovery-reservation-lifecycle.md). At 7de0a61, raw and
  marker-opaque PUBLISH reservations and recovered PUBREL placeholders could
  outlive their durable evidence on a reused session; terminal and cancellation
  successors could leave a scheduling owner behind; live PUBREL bookkeeping could
  bypass unresolved recovery ownership. C5 closed the cases it creates; the
  lifecycle change extends the same machinery to the pre-existing ones: a complete
  successful resume stages absence releases, commits them on the same CONNECT
  under generation, epoch, walk, instance, sequence and send fences, and installs
  identifier exclusion before removal. The N-7.2 marker replacement licence is
  kept nonce-only; known recovered PUBREL placeholders reconcile without blocking
  delivery.

  **F-105 dependence (audited 2026-10-05; restated 2026-10-06):** this closure's machinery
  is F-105 row 5 unit C code, part of the F-105 integration ticked in the W3 candidate
  (`TestUnitCRecoveryOpaqueOrdinaryTwoPass`, `TestUnitCRecoveryHandoff`), so it is
  conditional on F-105's integrated gate; see F-105's "Checked bullets resting on F-105".

**Shared-subscription QoS 2 scope decision — TAKEN 2026-09-02.** The first alpha supports
shared subscriptions at QoS 0 and QoS 1. A shared-filter SUBSCRIBE requesting QoS 2 is
accepted at **granted QoS 1** (`SUBACK 0x01`) by default; ordinary, non-shared QoS 2 stays
supported. The existing session-affine shared-QoS-2 implementation is deliberately retained
behind an explicit experimental or non-production gate, but it is no longer an alpha
commitment. Its persistent state has no upgrade or downgrade contract: disabling the gate
with experimental state present, or carrying that state across an incompatible build, is
unsupported. Operators must reset that state rather than expect migration, reinterpretation
or cleanup support from a build in which the experiment is disabled.

This decision retires the shared-QoS-2-only release gates without declaring their defects
fixed. F-047, F-048, F-049, F-064, F-070 and F-071, plus any affine-only branch of a wider
finding, are gathered under [the dedicated post-alpha chapter](#p-shared-subscription-qos-2-post-alpha--maybe-someday).
Other roadmap work is proved against the supported QoS-0/1 shared path and the ordinary
QoS-2 path. It may preserve the experimental path when cheap, but it must not grow scope,
reject a sound general fix, or fail an alpha gate merely to keep that path working. An
experimental regression is recorded in that chapter for the later investment decision.

- [x] **Enforce the alpha shared-subscription QoS ceiling.** The
  production default must grant at most QoS 1 for a shared filter, persist that granted QoS,
  and avoid provisioning the affine consumer, assignment, copy and correlation machinery.
  MQTT 5 continues to advertise Shared Subscription Available; do not lower the server's
  global Maximum QoS. MQTT 5 answers a shared QoS 2 request with granted QoS 1; MQTT 3.1.1
  continues to treat `$share/...` as an ordinary literal topic filter because shared
  subscriptions are not part of that protocol version.
  Preserve the current shared-QoS-2 implementation and its tests behind an explicitly
  experimental gate; the current gate is a programmatic test option that the production
  command does not expose. The affine suite additionally requires the
  `mqttport_experimental_shared_qos2` build tag and runs only in a non-gating CI job.
  Pre-existing experimental shared-QoS-2 state must be reset before
  running with the gate disabled; detection, automatic downgrade and migration are not part
  of the supported contract. Enabling, disabling, upgrade, rollback, backup and restore of
  experimental state are unsupported.

The 35 closed findings are indexed in the [finding dossiers](findings/README.md).
Of those, 31 are retained in the
[resolved-findings archive](findings/resolved.md); F-027, F-029, F-030 and F-031 remain
in the mixed-status [HA arbitration dossier](findings/ha-arbitration.md) beside their
open follow-ups. The entries below are the active release view; their linked dossiers
are canonical for mechanism, evidence, rejected designs, tests, and residuals.

### 0.1 Remove pre-release persistence compatibility

Do these before further MQTT-core implementation. The broker has no released
persisted-state contract, so carrying these paths through subsequent session,
retained-message, and Will changes would make every change maintain and test
formats that no user release depends on. Each checkbox is one commit.

- [x] [F-056: Remove inherited retained-message
compatibility](findings/resolved.md#f-056).
  The legacy JSON reader, the old-server tombstone marker and the `RD`
  delete-notification protocol are gone, and so is the compaction delete beside
  the notification: the empty-body tombstone alone carries deletion, and the
  store evicts the record it replaces. An undecodable record is now rejected with
  a log and evicted rather than silently ignored. Filed while doing it: F-059 and
  F-060 below.

- [x] [F-058: Reject generationless delayed-Will records](findings/resolved.md#f-058).
  The empty-generation legacy bucket is gone. Every decoded `$MQTT_willq` record is
  validated — generation, origin sequence and epoch, state, and identity fields that
  must each be a single subject token — and bound to the subject it was stored on,
  at all three decode boundaries. A record that fails is refused: never claimed,
  published, cancelled or deleted, and reported once per subject. Two deltas from
  the finding's wording: the enqueue validates before persisting and falls to the
  existing immediate-publish fail-safe, and the takeover fence's now-duplicate
  inline origin check is deleted rather than kept beside the validator. Residual,
  stated: a refused record is reclaimed by NOTHING — no sweep, fence or reaper
  touches it — so it persists until an operator deletes the message, which is what the
  diagnostic prescribes. It used to offer recreating the stream as an alternative; F-066
  withdrew that, since the ten broker-owned streams are one recreation unit and
  recreating `$MQTT_willq` alone is refused at the next startup — the only other route
  is a whole-bundle reset. Permanence has a running cost, and
  the diagnostic says so, on two paths. The claim scan re-reads every extant record
  each interval, so a refused backlog adds `O(N)` JetStream gets per scan per broker
  and a large enough one can push a scan past its interval and delay valid due Wills.
  The reconnect half of that cost is since GONE with the reconnect-time cancel walk
  itself (F-031), so a refused backlog no longer adds round trips to a client's
  CONNECT. A quarantine stream, a bounded scan and a reaper were all rejected —
  each is a second mechanism deciding the fate of records the bridge has said it
  cannot interpret.

These cleanups do not remove MQTT wire compatibility or the operator migration
guide from NATS Server's embedded MQTT listener.

### 0.2 The supported HA scope: multi-frontend

Decided 2026-08-10: the first alpha supports multiple MQTT frontends against a
replicated JetStream cluster, keeping the README's horizontally scalable,
cross-instance HA promise. Every item in this subsection that is reachable in the
supported feature set is therefore an unconditional alpha blocker. The 2026-09-02
shared-QoS-2 scope decision is orthogonal: affine-only findings and branches are parked in
chapter P, while shared QoS 0/1 and ordinary QoS 2 remain subject to this HA bar. The
lineage work was designed against an observed
pre-2.14.4 window in which stale stream-leader work could propose after a fast
election. **That premise is now RETIRED rather than assumed** (F-030, closed 2026-08-16):
`test/depproof/` enumerates every route by which a write conditioned on a superseded
base could reach a proposal across a leader transition, fixtures the two live ones
against isolated nats-server checkouts, refutes seven other shapes, scopes post-start
mirror/source drift outside F-030, and pairs mutations with each live mechanism. Both
live routes are closed by the combined evidence: the route
inventory and mutations, the pinned source mechanisms, and the NATS maintainer's
explicit confirmation that leader-local in-flight ownership plus applied-before-serving
enforce the header across elections
([`nats-server#8469`](https://github.com/nats-io/nats-server/issues/8469#issuecomment-5304075553)).
No further lineage is to be added on the strength of the old premise without checking it
against that inventory.
The per-stream findings inherit the server contract and the canary, but each needs its own
consequence audit, and **all three have now run.** F-032's ran on 2026-08-17: no lineage
was needed, and it produced F-062, which is now closed too. F-033's ran on 2026-08-18, with
the same result on lineage and four new findings: F-063, F-064, F-065 and F-066. F-034's ran
on 2026-08-22, again with no lineage owed — but unlike the other two it found a reachable
defect, and its remedy is identity conditioning and bounded destruction rather than lineage.
It also produced F-067 and F-068, plus F-069 through F-072 from its verification; three
further findings it surfaced are filed separately, for the reason its own bullet gives.

- [x] Decide and document the first-alpha HA support level. Decided: multi-frontend
  HA stays in alpha scope. The README already promises it, the architecture is
  built for it (stateless frontends, all durable state in JetStream,
  cross-instance takeover), and a single-frontend alpha would defer the epic
  this project exists to deliver. The items below stop being conditional, and
  the dossier release-treatment lines record the same resolution.

- [x] [F-027: Repair a recognised session-record fork](findings/ha-arbitration.md#f-027).
  **Slice C of the arbitration design.** A fork's durable loss is exactly the record
  DELTA, and because `classifyRecordLineage` matches on a shared parent that parent is a
  single-hop common ancestor — so the repair is a three-way merge against it, not a union.
  A union was the trap: it resurrects a subscription the client was UNSUBACKed for,
  because "absent from the survivor" is ambiguous between a lost addition and a
  survivor-side removal, and only the parent's content disambiguates it. Two
  record-sourced grant snapshots rotate at every landed adoption; a forked verdict at any
  of seven classification points mints an in-memory capsule of the delta, inside the
  transaction that classified; and the client's own reconnect re-asserts it through the
  ORDINARY accept-save CAS, behind the record fence. No repair write, no worker, no
  durable state, so the four recorded rejections are answered rather than waived, and the
  repair inherits F-029's conditional write, rejection classification and T0/T1a/T1b/T2
  re-drive. The whole coupled per-sid state is modelled — membership, the v5 options, both
  identities of a multi-level filter, drains, cancelled-register verdicts and the PUBREL
  consumer — as ten action rows over nine state pairs, with the conflict predicate drawn
  at drain PRESENCE so a reconciliation-recreated durable is not read as a subscription
  conflict. The three settle-items are settled: the reclaim sits behind the accept
  transaction and therefore strictly after the CONNECT's uncertainty resolution on every
  path; all four rejection verdicts are covered by one in-fence mint at
  `probeRecordRejectionInTx`, which is also what covers `settleRecordRejection` being
  unbound-only while a displaced connection is still attached. It is bounded repair, not
  general repair: not a record this broker did not author, not across a restart, not a
  filter the survivor decided about, and **not shared-subscription membership, which stays
  excluded** because a per-filter shared restore can fail while the membership stays durable.
  (The original reason given here — that the success CONNACK precedes shared restoration —
  stopped being true when F-050 moved restoration before the CONNACK; the exclusion stands
  on
  the failure asymmetry, not on the ordering.)
  `docs/CONFORMANCE.md` states every bound and the cap-forfeiture residual.

- [x] [F-029: Arbitrate simultaneous takeovers from one
base](findings/ha-arbitration.md#f-029).
  **Slice B of the arbitration design.** Every session-record write is now conditional,
  base 0 included, which closes the defect found while designing it: a clean-start
  CONNECT whose teardown claim was SUPERSEDED still proceeded to an accept save at base
  0, framed with no CAS header, so it landed unconditionally and erased the winner's
  record within a single leader term. The claim's outcome is typed
  (`errMQTTClearSuperseded`) — four callers terminal, two re-driving — and
  `finishAdoptedTeardown`'s superseded release is surfaced rather than swallowed, which
  was the one production path that wrote on a base it had been told was stale. The
  re-drive is governed by three rules (any stale-base outcome re-enters the
  authoritative restore; base and parent come from one read; bounded, and no exit
  leaves a `seq == 0` resident) rather than by an enumeration of `processConnect`'s
  branches, and by four binding transitions, because the discard runs pre-bind and the
  accept save post-bind. The lineage chain crosses the teardown re-mint via
  `reMintedFrom`, and the generation short-circuit is gone from `classifyRecordLineage`
  — a clean-start sibling always carries a different incarnation, so that short-circuit
  hid the one fork the rule exists to catch.
  **What it does NOT close** was filed as two blockers below rather than recorded as
  residuals; the claim-won deletion window is now closed by the retirement watermark,
  and cross-term create × create recognition is closed by F-029b below.

- [x] **The claim-won deletion window** (F-029a) **and the PUBREL orphan class.** The
  window was real: a teardown that wins its claim deletes the retiring generation's
  resources before its release can say whether it still owns the claim, and a cross-term
  resume derived from the same base evicts the marker off the record subject
  (`MaxMsgsPer: 1`), leaving a record that names exactly what was deleted with nothing
  for `finishAdoptedTeardown` or the sweep to adopt. Closed by the durable teardown
  intent this bullet asked for, kept OFF the replaceable subject: a permanent per-client
  **generation-retirement watermark** on the new `$MQTT_teardown` stream, written after
  the claim is won and before the first deletion, always confirmed, and read by three
  readers — the winner's own release-superseded arm (condemn a same-generation survivor
  with a fresh marker, MARK ONLY), the CONNECT restore (discard a retired record through
  the existing overdue shape rather than resume it hollow), and the orphan sweep (condemn
  one with no CONNECT at all, its finish deferred to a later pass). Replacement is
  ordered by the claim marker's `$MQTT_sess` sequence alone; the generation plays no part
  in that decision. Nothing ever reaps a watermark, so no GC choice widens the residual,
  and the stream is epoch-COUPLED to `$MQTT_sess` — a partial recreation of the unit is
  refused at startup rather than classified per read. Every reader validates the record
  with `mqttValidateTeardownTargets` first, because a marker licenses its adopters to
  delete every name the record carries. The companion half landed with it: the sweep now
  has a PUBREL orphan class (parse, config validation at both the discovery and
  pre-delete sites, verbatim-plus-derived reference classes, final record re-read then
  instance re-lookup).
  **Residuals** (`docs/CONFORMANCE.md`): the fence names only the LATEST retirement and
  the single slot can itself be regressed by a cross-term apply on `$MQTT_teardown`, both
  stacked instances of the documented apply-time CAS hole; the PUBREL reaper's two
  post-check windows, which used to be stated as the same bound the affine reaper has —
  that reaper is gone (F-036 course E), so they stand on their own; a bundle reset loses every
  open fence (`$MQTT_teardown` recreated ALONE used to do the same and was the sharper
  case — F-066's enforcement refuses it as of 2026-09-01). A finish that outruns
  the survivor broker's own
  observation is a new route into F-036 and is listed under that blocker, not here.

- [x] **Sweep the rollout-qualified residuals out of the docs.** DONE. Every residual
  qualified by "an older broker", "mid-rollout" or "once every broker upgrades" was
  re-read against what its feature does for a broker running this tree and rewritten:
  **9 documentation sites** (five in `docs/CONFORMANCE.md`, **three**
  `docs/ARCHITECTURE.md` sections against the bullet's "two", plus this entry), **30
  inventoried Go comment blocks** and **one runtime diagnostic string**, plus **three
  more the implementation's own broad audit surfaced**: `sess_fork_lineage.go`'s
  "validates only Gen", a third instance of the wrong generationless-record claim in
  `clear()`, and `pubrel_correlation_test.go`'s body, which still called the plain form
  "legacy" after its doc comment stopped doing so. The comments were in scope because
  they restate the same claims beside the mechanism; leaving them would have put code
  and docs in direct contradiction. Most sites became **Scope** on the F-029
  template — the limit is real, but it is the reach of the brokers that read a stream or
  record field, with no runtime capability gate — and one became a restart /
  post-initialization-mutation split, whose recovery was also corrected to stop-all-and-
  recreate (the errors offer no in-place fix, and every broker-owned stream is coupled to
  every other — at the time this said `$MQTT_sess` and `$MQTT_teardown` plus
  `$MQTT_origin`, which the write-lineage audit deleted; F-066 has since made all ten
  one recreation unit).

  Three sites were not merely stale. Two had framing that **hid a live producer**, so the
  rewrites gain accuracy: the affine oversize backstop is reached because a QoS 1/2
  **Will bypasses the PUBLISH admission preflight**, and an uncorrelated PUBREL comes
  from a **retained or otherwise unbacked QoS 2 send**, not from a broker that predates
  correlation. Two comments were **factually wrong** — both claimed a generationless
  record falls back to legacy hash-only names, which `mqttSessGenFromRecord` refuses.

  The third was not wording at all. `unwindProvisionedQ2AffineOnLanded`'s
  `hasSharedQoS2Sub()` guard named a **legacy persisted shape** — a record with a qos-2
  `SharedSubs` entry and an empty `Q2AffGen` — that no writer on this tree can produce,
  which under the boundary is refused rather than accommodated. So the item also
  **closed a legacy-state accommodation its title does not promise**: the pairing is now
  refused at CONNECT, in the startup expiry sweep, at a fired expiry timer, and in
  `mqttValidateRecordAffinePair`; and `materializeQ2AffOnBehalf` — which read
  `Q2AffGen != claim.Gen` as proof its winner was gone — now validates the record first,
  since an empty generation mismatches every claim and would discard an assignment owed
  to a started delivery. The guard itself is KEPT as a fail-closed backstop over the
  in-memory state, and its comment now says so instead of claiming a live population.
  Residual in `docs/CONFORMANCE.md`: a refused record is reclaimed by nothing until the
  streams are recreated. The enforcement points call the sweep's validator **whole**,
  which also makes its three pre-existing malformed pairings refuse a CONNECT and an
  expiry rather than only the sweep — deliberate, and stated in
  `docs/ARCHITECTURE.md`: splitting that predicate is the failure its own comment
  records. The holistic review's substantive finding is folded in: because the
  validator is shared with `buildSessionRefs`, a refused record initially suspended
  the orphan QoS 2 sweep account-wide **and permanently**, since nothing rewrites
  such a record and expiry now refuses it too. That was over-broad, and the same
  function already said so about its affine claim inventory — "deliberately not an
  early return ... or one persistent corrupt claim would disable them account-wide,
  forever, on evidence about a different resource class". The reference walk now
  reports the affine half indeterminate **separately from a fatal failure**: an
  affine-pair failure suppresses `reconcileOrphanQ2AffClaims` (and, until F-036 course E
  removed it, affine consumer deletion)
  while inbound holds, in-flight registers and PUBREL consumers keep being reclaimed
  (that record's identity and generation are sound, so their references are
  complete); only a record whose identity cannot be established still aborts the
  pass. This also fixes the same over-broad abort for the three pre-existing
  affine-pair failures. Both are warnings now rather than debug, and the
  recreate-the-streams guidance is attached only to record-shaped failures so a scan
  timeout cannot prescribe a destructive recovery. Two tests pin the split, each
  asserting both halves, with mutations M-H (fatal again) and M-I (skip dropped)
  breaking opposite halves.

  A third round then found a **bypass of the new rule itself**, and it was real: the
  predicate matched the literal QoS 2, but `processSubs` clamps a filter above QoS 2
  down to 2 while restoring and the resume feeds `SharedSubs` values in verbatim, so
  a record carrying 3 became a live shared QoS 2 membership that the rule never saw.
  A persisted shared QoS byte outside 0..2 is now refused in its own right
  (mutation M-J), with QoS 3 and QoS 255 rows in the shared acceptance matrix.

  A fourth round found the last one: the winner-record validation in
  `materializeQ2AffOnBehalf` sat *inside* the generation-mismatch arm, so a record
  that MATCHED a claim's generation and was still malformed went on to materialize a
  copy and **acknowledge the group source** — irreversible, for a delivery owed to a
  session the same refusal makes unresumable. Validation moved ahead of the
  comparison, since the record is used in both directions. Its first fixture passed
  for the wrong reason (the arm was unreachable without the winner's affine consumer)
  and mutation M-K exposed that before it could be believed.

  One correction to the plan's own bookkeeping: `inflight_register_test.go:4717` was
  listed as a survivor of the closing regression grep and is not one — its phrase
  splits across two comment lines, so the single-line pattern never matched it. It
  remains a correct exclusion ("the old broker" there is the *displaced* participant,
  two concurrent brokers of this tree); it was simply never a gate hit.

  Deliberately not swept: operator-facing "not a rolling upgrade" error strings and the
  assertion pinning one, `docs/findings/` and `CHANGELOG.md` (historical record),
  `CONTRIBUTING.md` / `docs/PORTING.md` / `CLAUDE.md` (correct statements of the
  boundary), upstream nats-server's embedded MQTT listener (a real external population),
  and the reversed "a broker old enough to have written X" phrasings, which describe
  fail-closed refusal rather than rollout support.

- [x] **Cross-term create × create recognition** (F-029b). **SUPERSEDED 2026-08-16 by
  the write-lineage audit in §0.2, which deleted every source below**: a create is
  parentless again and this pair concedes, stated as residual R-create in
  `docs/CONFORMANCE.md`. The entry is kept for the mechanism it describes and the
  refutations it records. A create USED TO descend from
  the record whose deletion emptied the subject, which is one rule with three sources
  rather than a new mechanism: the record `clear()`'s own claim held, the record an
  ADOPTED marker names in its `PWID`, and — the new one — the record the
  generation-retirement watermark named in a required `rwid` field — a field since removed
  outright, see the vanished-record bullet in §0.2 — for the interval
  whose marker is already gone. That makes two creates in one interval siblings of each
  other and a create in a later interval a descendant, so ordering survives deletions
  with no new comparison; inheriting a fresh interval nonce instead would have missed
  the MIXED pair, where one broker tore the record down locally and the other only ever
  saw an empty subject. The watermark gains a fourth reader, whose one deliberate
  asymmetry is that a CORRUPT watermark refuses the CONNECT — admitting a session over
  one this broker cannot read leaves its own later teardown unable to record a
  retirement — while an indeterminate read concedes rather than failing a fresh CONNECT.
  The invariant that makes the token exact is that every record deletion advances the
  interval identity, so the **CONNECT rollback now ARBITRATES before it fences**: its
  bare sequence-targeted delete is replaced by the real teardown (`clear()`'s seventh
  call site), whose claim is a CAS at the candidate's own sequence, and it branches on
  the session's EFFECTIVE persisted lifetime — which also fixes a live defect, since the
  bare form destroyed a persistent session's record on a rejected resume that merely
  REQUESTED expiry 0. A refused teardown parks the discard on a still-resident session
  rather than abandoning it, and the classification reads the lineage pair captured at
  entry, because `clear()` zeroes it. `reMintedFrom` became `retiredBaseWID`: one field,
  one fact, three sources. **SUPERSEDED**: the write-lineage audit deleted
  `retiredBaseWID` and all four create-parent sources; the rollback's real teardown, its
  effective-lifetime branch and the parked discard are unaffected and remain in force.
  **What it does NOT close**: a record-absent create with no usable watermark still
  concedes, filed as the bullet below with its refuted cheap fix, and four routes on
  which a stale, regressed, delayed or out-of-band identity manufactures a FALSE FORK —
  the dangerous direction, and the one they are named for. Three of them also concede: a
  stale read across a remote lifecycle and a regressed watermark slot give the two
  creates of one interval different parents, and an out-of-band deletion left a
  watermark naming the last RETIRED record rather than the deleted one, so a create
  following it matched neither the record that was deleted nor its parent. (All in the
  past tense: this describes the create inheritance the write-lineage audit DELETED, and
  the watermark field it read has since been removed outright.) All four are
  stated as residuals R1-R4 in `docs/CONFORMANCE.md`. Recognition succeeds exactly where
  a valid retirement history supplies the identity; nothing here claims more. One piece
  of NEW exposure comes with the rollback rework and is stated as such: the teardown
  claim it now takes can itself be regressed by a cross-term apply, which the bare
  delete could not reach — the same claim-apply window every other `clear()` caller
  already carries, narrowable only by durable evidence of the interval a claim
  overwrote. Slice C's repair is untouched and deliberately so: a create has a parent
  POINTER and no parent CONTENT, so a create-parented fork is recognised and repaired by
  nothing, merging against an empty base being the union Slice C refuted.

- [x] **A durable identity for an interval with no retirement history** (F-029b's R0).
  **SUPERSEDED AND DELETED by the write-lineage audit** — the register, its stream, its
  validator and every residual below it are gone, and `$MQTT_origin` is now refused at
  startup. The entry is kept for the refutations it records, which still bind anything
  proposing a durable interval identity again. Its closure was real and so was its cost:
  R0c, the replayed-or-regressed identity, was a FALSE fork, which is the direction the
  audit deleted the whole inheritance to remove.
  The **session-interval register** (`internal/mqtt/sess_origin_interval.go`): a new
  broker-owned stream `$MQTT_origin`, one single-slot register per client at
  `$MQTT.origin.<domainTk><hash>`. The
  identity is `<origin-epoch>.<sequence of the standing register message>`, and every
  teardown ROTATES it from inside `recordGenRetirement` — the single site, so a
  retirement cannot be recorded without rotating the identity and both callers inherit
  it. A record-absent create whose watermark read is Absent reads the register (or
  establishes it with a create-only CAS), and stamps that identity as its first write's
  parent. Closes the genuine-first-ever and history-lost subcases; the INDETERMINATE
  read concedes by construction, without touching `$MQTT_origin` at all.
  **The bullet's own sketch was refuted along the way** and is recorded so it is not
  re-proposed: a retain-all stream with minimum-sequence election needs a bounded
  corrupt-prefix walk and a durable record of the first interval's END, and none of that
  is required once the register is a single slot every retirement replaces — with an
  ADVANCING identity there is no interval boundary left to mark. The claim that closing
  history-lost needs two permanent entries per session lifecycle was disproven the same
  way. `DenyDelete`/`DenyPurge` on `$MQTT_teardown` were NOT adopted, and they are not
  required on `$MQTT_origin` either: a delete cannot rewind a stream's sequence counter,
  so an erasure loses an identity rather than replaying one. Both are audited
  non-prerequisites on all three streams, and administrative pruning is permitted with
  its cost stated (R0f).
  **The rotation carries no idempotency guard, deliberately.** Three were designed and
  each refuted — `MarkerSeq` (one retirement occupies several `$MQTT_sess` sequences),
  the claim nonce (it proves duplicate claim BYTES, not a duplicate BOUNDARY), and the
  retired record's `WID` (commutative within one retirement, but it does not ORDER
  different ones, so a paused finisher REGRESSES the register). Any guard that can
  decline a write can leave an earlier interval's identity standing, which is the
  false-fork direction; an unguarded rotation can only mint a fresh sequence. The
  resulting late/duplicate-finisher divergence is R0a in `docs/CONFORMANCE.md`, with a
  worked interleaving and a both-directions test, and any future change reintroducing an
  equality guard must answer those three refutations first.
  **Costs**: one confirmed publish per teardown inside `clear()`'s record fence, and one
  register read per record-absent CONNECT whose watermark is Absent (plus one write and
  one re-read only when no register exists yet). Zero on every other CONNECT.
  **Residuals** R0/R0a-R0f in `docs/CONFORMANCE.md`: the transient read; the
  late-or-duplicate-finisher divergence; a cross-term double-apply on `$MQTT_origin`'s
  own Raft group; a restore or LIVE recreation replaying or regressing its sequences —
  the only false-fork direction, bounded by the mandatory-restart rule; two client IDs
  sharing an eight-character hash denying each other recognition; administrative
  pruning of the register losing recognition for the pruned client; and mixed-arm
  divergence. R1 and R4 are WIDENED to the no-watermark population, stated rather than
  claimed away.

- [x] [F-030: Add write lineage to the Will outbox](findings/ha-arbitration.md#f-030).
  **CLOSED 2026-08-16 by disproving the failure premise; no lineage was added.** Every
  `$MQTT_willq` write is already conditional on the sequence its writer read, so the
  finding reduces to a property of the server. `test/depproof/` (`make depproof`, plus a
  CI job on any change to
  `go.mod`, `go.sum` or the harness) carries an exhaustive route inventory for a write
  conditioned on a superseded base reaching a proposal across a leader transition: two
  live routes with fixtures, seven other shapes refuted, post-start mirror/source drift
  scoped outside F-030, and mutations per live mechanism run inside isolated nats-server
  checkouts.
  **R-old is closed**: an operation whose captured proposing term outlives that term is
  refused by Raft's comparison of the captured term against the current one (2.14.4,
  `nats-io/nats-server#8370`), proved by two discriminating mutations that do not depend
  on the other route's machinery. **R-new is closed by independent, converging
  evidence**: the readiness envelope and reconstructed per-subject expectation refuse it
  in every measured run and under discriminating mutations; the pinned source identifies
  those enforcement boundaries; and a NATS maintainer explicitly confirmed that the
  leader's in-flight ownership and refusal to accept messages before pending entries are
  applied guarantee correct enforcement across elections
  ([`nats-server#8469`](https://github.com/nats-io/nats-server/issues/8469#issuecomment-5304075553)).
  The four earlier oracle defects remain recorded because they prevent treating the
  harness alone as proof; they no longer keep the finding open because it is now a
  regression canary behind an upstream-owned contract rather than the sole basis for
  that contract. The plan's control prediction was FALSIFIED and is recorded as such: on
  v2.14.3 the older-term write IS admitted at the proposal boundary — the difference the
  floor buys — but never becomes the surviving record, because apply-time log order and
  the store's refusal of any sequence that is not the next one both discard it; neither
  backstop is mutation-proved, so neither is part of any claim.
  `TestMQTTWillOutboxReplicatedTerminalDoneStands` is the bridge sentinel over a real R3
  cluster, with an executable unconditional-write mutation that must republish an
  already-published Will. The two synthetic pins are retained, renamed as negative
  dependency tests, and are never cited as evidence about the server. The corresponding
  same-base conditional terminal-regression and claimant-divergence residuals are
  retired for a conforming `$MQTT_willq`. Unconditional foreign writes introduced by
  post-start stream-configuration drift remain outside F-030. The per-stream findings
  inherit the server contract and the canary, not a checkbox — and all three audits have
  now run. F-032's produced F-062, closed 2026-08-18; F-033's produced F-063, F-064, F-065
  and F-066; F-034's closed on 2026-08-22, producing F-067 and F-068 and routing its
  live-owner residue to F-036.

- [x] **Audit the session write-lineage machinery for deletion, obligation by
  obligation.** Prescribed by F-030's evidence and deliberately NOT folded into it.
  **DELETED: the whole CREATE side.** All four sources a create drew a parent from — the
  record-absent watermark inheritance, the adopted marker's `PWID`, `clear()`'s stamp,
  and `parentForNextWriteLocked`'s fallback — plus `sess.retiredBaseWID` and the
  session-interval register with its stream, validator and epoch (`sess_origin_interval.go`,
  495 lines). The finding that licensed it: **every route by which the inheritance
  produced a verdict was documented in `docs/CONFORMANCE.md` as a FALSE fork** (R0c, R1,
  R2, R3, R4), each needing an inherited identity that outlived the interval it named,
  and there was no route on which it was reachable only by a true one. A create is now
  parentless, which kills all five and concedes the cross-term create pair — the stated
  residual R-create, pinned in both orientations and end to end on the wire.
  **KEPT, each with its live obligation:** `classifyRecordLineage`, its five consult
  sites, the disposal vocabulary and `sess_fork_repair.go`, because the AUTHORED-RECORD
  fork's retirement is unproved on the writer every accepted CONNECT uses (the next
  bullet); `WID`/`PWID` stamping at all three writers; the create arm's
  corrupt-watermark REFUSAL, which is a separate obligation from the inheritance beside
  it; and the independent sequence fail-safes. **`$MQTT_origin` is REFUSED at startup**,
  not tolerated — a lookup-only preflight before the provisioning loop, since
  `ensureBrokerStream` creates an absent stream before validating it. Nine provisioned
  streams, down from ten. Six tests and five mutations (M1, M2, M5, M7, M9); no
  obligation was deleted on the strength of the term proof alone.

- [x] **Prove or retire the accepted-CONNECT atomic-batch write path.** F-030's
  dependency inventory (`test/depproof`) fixtures `$MQTT_willq`'s SINGLE-MESSAGE
  conditional write. Every accepted CONNECT writes something else: `storeAcceptBatch`
  commits the record and its permanent acceptance fact in ONE `AllowAtomicPublish` batch
  (`sess_accept_fact.go`), and the inventory's R-batch row is a **written refutation
  only** — `$MQTT_willq` never uses batches, so nothing exercises that shape.
  **The finding, established from the pinned v2.14.5 source, is that the two writers are
  genuinely not equivalent**: `ProposeMulti` pushes each entry separately
  (`raft.go:964`), each push releasing the queue mutex and signalling independently;
  `runAsLeader` pops whatever prefix currently exists and regroups by byte and entry
  thresholds; each resulting group independently takes the lock and independently
  rechecks leadership; and `storeToWAL` advances one Raft index per encoded append
  entry. **No Raft-level field preserves "these entries came from one `ProposeMulti`"**,
  so a two-entry batch can be divided across groups, WAL indexes and a leadership
  transition in ways a single `Propose` cannot — a FIFO prefix split, so
  fact-without-record is producible and record-without-fact is not. F-030's inventory
  therefore does not reach this writer, and the authored-record fork machinery is
  retained on exactly that gap.
  **What this item must NOT become.** Answering it needs fixtures on the queued, popped
  and WAL routes, which is where the multi-entry difference lives — and that is an
  independent verification campaign for consensus fundamentals the maintainer has
  already confirmed. A scoping attempt was trimmed to a single substitute row
  (R-atomic-apply) and the substitute was refuted: the natural split is FIFO, so it
  yields a fact-prefix and never the lone commit that would exercise the count guard.
  Two rounds of that review also **failed on the provider side**, which refuses content
  designing edits that disable consensus safety checks. So the honest options are a
  narrower question that source inspection shows is specific to `ProposeMulti`, or a
  written refutation carrying the four routes above. Do not open it by re-proving Raft.
  **PROVED, 2026-08-17 — the four Raft-layer facts hold and the consequence drawn from
  them is FALSE.** Batch identity is not preserved at Raft and does not need to be: it
  is carried in the stream ENTRY ENCODING and enforced at APPLY. A committed non-commit
  member is counted and its entry buffered in memory, never stored; only a gap-free
  `batchCommitMsgOp` replays the members into the store, under one `mset.mu` hold. **A
  FIFO prefix split therefore applies NOTHING** — fact-without-record is not producible
  after all. Measured, not argued: `TestJetStreamAcceptBatchPrefixStoresNothing` (a thin
  delta over upstream's own R3 `ProposeMulti` prefix + `StepDown` shape, asserting on
  every replica what upstream never looks at — the target subject), mutation
  `m-batch-apply-eager`, the unmutated `^TestJetStreamAtomicBatchPublish` pin, and the
  bridge sentinel `TestMQTTAcceptBatchReplicatedIndivisible` with `m-accept-split-write`
  over a real R3 `$MQTT_sess`. Two limits are stated rather than glossed: the guarantee
  is a HISTORICAL COMMITMENT at the moment of the write (the record subject is a
  `MaxMsgsPer: 1` register, so a later legitimate save replaces what the batch
  committed), and a hard kill between two members' store writes CAN leave partial
  durable residue that recovery converges — never "no residue exists". No Raft edit and
  no consensus-safety check disabled; the term guard is inherited by citation, since
  `ProposeMulti`'s is verbatim `proposeLocked`'s.
  **CONSEQUENCE AUDITED 2026-08-17 — the machinery is RETAINED, on a route the closed gap
  never covered.** The audit asked whether any supported writer, ambiguity path, deletion,
  recreation, restore, replay or administrative input can make a consult site observe the
  COMPLETE predicate. It can. The predicate `found.PWID == ackPWID && found.WID != ackWID`
  establishes two record BODIES CLAIMING one parent, never that two CAS applications off
  one base both landed — and an **unconditional** write produces the first without the
  second. A headerless direct publish to `$MQTT.sess.<domainTk><cidHash>` is not a write
  the F-030 closure speaks about at all: that proof is scoped to writes CARRYING a
  per-subject expectation. Nothing validates provenance at restore
  (`createOrRestoreSession` checks JSON, client ID, reserved membership, generation, the
  affine pair, teardown/retirement and overdue state, then adopts `WID`/`PWID` as given),
  and the reserved-namespace guards deny `$MQTT.>` to MQTT clients rather than to
  permitted NATS publication. `TestMQTTForkedRecordE2EAnswers0x89AndPublishesTheWill`
  already proves the whole route end to end — an unconditional store, the live connection
  reaching the classifier, 0x89, and the Will PUBLISHED — and it runs in `make full-suite`,
  which applies no `-run` filter.
  **Deleting it would not have been a cleanup.** The forked verdict is not a reason code:
  reporting this route as a takeover would skip the repair capsule
  — and, as this bullet read when it was written, would also have cancelled a delayed Will
  that was still owed, the bound-rejection foreign arm having discarded where the forked arm
  did not. **F-084 removed that half**: both arms consume the Will now, so what deletion
  would still cost is the capsule
  (`TestMQTTForkedRecordE2EConnectRebuildsFromRecord` pins the capsule restoring real
  delivery). One surviving route invalidates deletion however much other evidence
  converges, so the exit was fail-closed.
  **It also corrects the falsified PREMISE where that premise was doing live work.**
  Part A closed the cross-term same-base route ONLY for bridge-authored conditional writes
  on `$MQTT_sess`, and a substantial number of code comments, test commentary and doc
  blocks still explained live machinery by it. The authoritative statement now lives ONCE,
  in `docs/CONFORMANCE.md`'s three-part same-base boundary (closed / still open /
  untouched), with `docs/ARCHITECTURE.md` summarising and referencing it; code comments
  stay locally useful rather than becoming "see docs" pointers, and historical text
  (`CHANGELOG.md`, completed `[x]` items) is preserved verbatim.
  **What is NOT claimed.** This is not a proof that every semantically equivalent
  restatement in the repository has been found. No such claim is mechanically checkable
  over natural language, and asserting one was the error that cost this item three review
  rounds. A lexical signature sweep (130 candidates) and a concept-rooted structural pass
  were SEARCH AIDS that made the work finite; they are not a completeness certificate. The
  defensible claim is narrower and is the one made here: the audit reached a justified
  result — RETAIN, on a live route with an enforced end-to-end oracle — and every concrete
  false statement the reviews and sweeps surfaced was corrected. A stale comment found
  later is an ordinary defect, not a reopening of this item.

  - [x] [F-032: Add write lineage to inbound QoS 2
  state](findings/ha-arbitration.md#f-032). **Audited 2026-08-17. No lineage was needed,
  and the audit produced a new alpha blocker, F-062 below.** Those are two separate
  results; the first does not imply "nothing actionable remains", which is the
  conflation the first five drafts of this item made. All four bridge-authored writes to
  `$MQTT_qos2in` carry `JSExpectedLastSubjSeq` — three create-only, one at the writer's
  own loaded sequence under a generation comparison — so clauses (a) and (b) answer NO
  over an inventory derived from two mechanical sweeps (complete for bridge-authored
  data mutations with the test seams nil, not "by construction"). The upstream contract
  is **inherited by citation** from F-030 rather than re-proved, and no `test/depproof/`
  row was added; `docs/ARCHITECTURE.md`, "Trusting the backend" now states why
  re-proving or reimplementing an unprovable upstream guarantee is the wrong instinct,
  and where the line between a dependency gap and our own gap falls. Lineage was refused
  on the merits: three writes are create-only so there is no base to descend from, and
  the fourth is guarded by an immutable exchange generation. **What the audit found
  instead** is the one route `$MQTT_willq`'s inventory never needed: `purgeQoS2Hold`
  names a SUBJECT, not a sequence, so JetStream has no condition to enforce because the
  bridge never supplied one. Its teardown caller is F-062, since closed by giving the
  purge a proven cutoff; its orphan-sweep caller and the post-enumeration gap went to
  F-036. Also corrected: the statements that had made the defect unrecognisable,
  including the headline "no silent loss on any PUBREL path", "exactly one create can
  ever succeed", a `discarded` marker concerning "only ever" its own exchange, and "a
  late purge is harmless by naming". Those sentences are why it survived review, so
  correcting them is part of filing it.

  - [x] [F-062: Bound the inbound QoS 2 hold purge
  and the discard marker it leaves](findings/ha-arbitration.md#f-062). **Alpha blocker,
  found by F-032's audit. DONE 2026-08-18.** *The dossier is authoritative for the
  mechanisms, their arms and the remedy; this bullet is a derived summary.* Two
  mechanisms shared one precondition: a teardown that WINS its claim against a peer
  still serving the same incarnation (a resume preserves the generation), while that
  peer's client reuses a packet identifier. **Mechanism 1** — the hold removal was a
  SUBJECT purge, so a hold accepted and PUBREC-acknowledged on an already-enumerated
  subject, while that subject's purge was still pending, was destroyed. **Mechanism 2**
  — the `discarded` marker the teardown left was reaped by no teardown or releaser path
  (it is the only durable proof a late releaser of the retired incarnation must lose),
  so a LATER exchange on that identifier read it and was answered 0x92 with its hold
  released. Both lost an **acknowledged** PUBLISH. **What closed it: two remedies
  sharing one durable field, plus a third change to the awaited arm.** `streamSubjects`
  returns `State.LastSeq` with the enumeration, and `purgeQoS2Hold(subject, upTo)` emits
  `Sequence: upTo + 1` with **no unbounded form**, so the teardown passes its walk
  watermark and the sweep its exact re-confirmed record sequence. `mqttQoS2RelClaim`
  gains a `Fence`, required for `discarded` and absent for `releasing`, so the arm that
  adopts a pre-existing marker purges to **that marker's fence alone** — not `min(fence,
  upTo)`, which would spare a hold a concurrent teardown's decision covered — and a
  releaser whose hold sits above the fence reaps the marker at its exact sequence and
  retries. That reap rests on strictly increasing occupant sequences within one stream
  incarnation, which is now stated once beside the compatibility boundary. Two cutoffs
  are refused rather than sent: `0`, which purges nothing while reporting success, and
  `MaxUint64`, which wraps to the unbounded request. An enumeration that cannot bound a
  purge is re-sampled, never acted on, and exhausting that budget errors and fails the
  clear. **The third change, found by the holistic review after approval and
  load-bearing on its own:** the awaited arm no longer purges at all. It had read the
  claim subject going empty as proof its enumerated exchange had terminated, which is
  false — a releaser also deletes a FOREIGN generation's claim before retrying its own
  create — so a vacancy now licenses only RE-COMPETITION, and any destruction happens
  under a marker the walk actually won. Without it an acknowledged exchange could be
  destroyed and its releaser then answered 0x00, reporting success for a message never
  delivered. Do not fold this into the two cutoff/fence remedies; it is not incidental
  behavior, and removing it restores the loss. **Residuals, none of them loss except one
  requiring record corruption**: a hold enumerated but above the cutoff survives to the
  sweep; a tombstone from an exchange completing between enumeration and purge is
  likewise spared; a corrupt readable fence can over-purge to its own value; and a
  refused watermark leaves a marked session that refuses the client ID's next CONNECT
  until an adoption succeeds. All in `docs/CONFORMANCE.md`. **A per-write `0 < Fence <
  markerSeq` coherence check was designed and deleted** — it caught numeric inversion
  but not stream recreation, and did not prevent the loss even in the case it caught. Do
  not re-propose it; the dossier records why. **It also closed a claim elsewhere**: the
  inbound QoS 2 hold walk is now bounded per walk, which `sess_teardown.go` had called
  out as unbounded. Scoped to that walk, and "per walk" is not "frozen at the first
  walk's watermark" — the dossier's mapping is authoritative. The unbounded
  `$MQTT_inflight` purge in that teardown was untouched then and owed F-034; F-034
  replaced it on 2026-08-22 with an enumerate-load-classify pass bounded by its own
  enumeration watermark.

  - [x] [F-033: Add write lineage to shared QoS 2
  assignments](findings/ha-arbitration.md#f-033). **Closed 2026-08-18 as an audit: no
  lineage was needed, and it produced three findings.** Clauses (a) and (b) answer NO —
  every claim write goes through one CAS helper that always emits
  `JSExpectedLastSubjSeq`, W1 is create-only, W2 CASes at a base read in the same
  attempt, and D1 deletes an exact sequence. Seven of the ten transitions additionally
  assert the winner's identity; three record owner-independent facts, of which the two
  `SourceAcked` writes rest on the CAS alone while `AffineDone` is additionally
  incarnation- and copy-fenced (F-065). All ten transition and twelve reap call sites
  are dispositioned individually in the dossier. Clause (c) is inherited by citation
  from F-030 with no new depproof row; **R-purge is not applicable and that is positive
  evidence** — `purgeStream` has exactly three appearances in the tree and none on this
  stream. **The yield was elsewhere, as in F-032**: the unconditioned operation is
  `CONSUMER.DELETE` on the interest-retained `$MQTT_out`, which has no precondition
  field at all. Eleven worked interleavings sorted onto three falsified premises of ours
  — **F-063** (nothing binds evidence that a copy was RETAINED to the irreversible
  source ack that follows it), **F-064** (a live generation reference leaves every claim
  of that generation to an owner resume that may never come) and **F-065** (a claim
  reaped on a copy-absence reading taken before its own `SourceAcked` transition). Three
  more sort onto **F-036**: the contender's discard arm, and — corrected during review —
  the sweep's by-name affine consumer delete, whose window a resuming session's ADOPTION
  reached only when that session was resident while its record was absent. **That delete
  was REMOVED by F-036 course E (2026-09-01), so its manifestation is dissolved rather
  than open.** The
  different-generation arm is **settled**, and a lost sweep-delete response was a bounded
  delay that converges. The four in-code "F-033, unaudited" markers across three sites
  now state the answer, and three further sites that asserted something the audit
  falsified are corrected too. Holistic review then found one more, filed as **F-066**:
  the claim key identifies one source message only within one incarnation of
  `$MQTT_msgs`, and recreating that stream WAS a procedure a startup refusal instructed.
  F-066 closed that: no startup refusal names a single stream any more, and recreating
  one is refused.

  - [x] [F-063: Prove a copy was retained before acknowledging its
  source](findings/ha-arbitration.md#f-063). **Alpha blocker — done.** `$MQTT_out` is
  interest-retained, so a copy stored onto a subject no consumer filters **succeeds and
  keeps nothing**, and the only re-read in `materializeQ2AffCopy` was on the
  CAS-REJECTION path: a store that SUCCEEDED was never verified, the claim advanced to
  `materialized`, and the group source was then acknowledged irreversibly. All three
  routes now converge on one PRESENCE CHECK — a successful store is verified exactly as
  a rejected CAS already was, an already-`materialized` claim is re-verified and never
  re-stored, and the release is authorized by `sourceAckAuthorized`: `copyPresent ∨
  AffineDone ∨ Discarded ∨ Lost ∨ SourceAcked`, refused otherwise. Both callers were
  restructured so a failed materialization re-reads the record instead of returning, or
  every terminal fact would have become unreachable for exactly the claims that need it.
  No new durable field and no new claim state. **The stated guarantee was re-scoped by
  an explicit decision**, from "bound/atomic" to "conditioned on a fresh but non-atomic
  observation": the claim and the copy live on different streams and `CONSUMER.DELETE`
  is unconditioned, so no finite check binds copy existence to the ack. The residual — a
  consumer deleted between the final proof and the ack — is in `docs/CONFORMANCE.md`
  with a worked interleaving. Two designs were refuted in review and are recorded in the
  dossier so neither returns: an attempt-scoped `Nats-Msg-Id`, and an owner-independent
  `Lost` writer (the claim CAS and the copy CAS are on different streams, so it can
  condemn a claim a concurrent materializer has just given a copy, permanently).
  **F-064, F-065 and F-036 are untouched**: an owner-independent reconciler cannot make
  consumer existence atomic with the store and the ack, which is what establishes the
  premises are distinct, and what this item's refusal strands is F-064's to resolve.

  - [x] [F-064: Reconcile assignments independently of owner
  resume](findings/ha-arbitration.md#f-064). **PARKED, NOT FIXED: removed from the alpha
  gate by the 2026-09-02 shared-QoS-2 scope decision and gathered in chapter P.** The
  technical finding remains open, and the partly landed implementation is retained.
  `buildSessionRefs` keys `refs.affine` on `ps.Q2AffGen`
  alone, so a live matching record makes the sweep skip **every** claim of that
  generation, and `recoverQ2AffClaims` — the only writer of `Lost` — runs solely on the
  owner's own resume. **What landed:** on-behalf restoration of a destroyed affine
  consumer. A contender taking a group redelivery rebuilds the winner's durable from its
  record and materializes onto it, so the stall is closed **without the owner for
  assignments that never reached `materialized`**. Neither gate below is addressed — the
  remedy bypasses both, because a redelivery carries the payload and the sweep does not.
  **What did not, and why the technical finding remains open:** - *The stall for a
  `materialized`
  assignment whose copy is gone* — residual 8. That population takes a different
  dispatcher arm and still waits for its owner. Re-storing there was designed and
  **refuted twice**: the copy CAS and the claim CAS are on different streams, so no
  claim-side fence orders a contender's store against the winner's authorization of the
  original PUBLISH, and the replacement copy can become a second PUBLISH for an exchange
  already past PUBREC. The route back to this is the PUBREL-carried claim reference in
  the `Lost`-truthfulness item below, and **that reference now EXISTS** (done
  2026-08-30): the stored PUBREL carries a bound `(claim subject, copy subject)` pair
  naming the assignment it completes. It supplies only the direction that item needed,
  PUBREL → claim. A contender needs claim → PUBREL, a reverse lookup over an
  account-wide subject space, and that is still uncosted — so residual 8 is unchanged.
  **A 2026-08-31 attempt to recost that lookup as cheap was WRONG and is withdrawn; what
  it established is the real obstacle, so it is recorded rather than deleted.** The
  attempt claimed the claim's `Winner` and `Gen` are exactly `mqttPubRelNames`' inputs.
  They are not, and the two generations are **different identities**: the claim's `Gen`
  is the AFFINE generation — `beginQ2AffOp` returns `sess.q2AffGen` and that is the value
  stored (`qos2_shared_affine.go`) — while `mqttPubRelNames` namespaces the PUBREL
  subject, delivery subject and durable by the SESSION incarnation generation `sess.gen`
  (`sess_incarnation.go`) — the same different-keys fact F-036's bullet records for
  `refs.hasInbound` versus `refs.hasAffine`. **The mapping is the winner's own record,
  which carries both `Gen` and `Q2AffGen`, and the costing therefore SPLITS BY RECORD
  STATE rather than being uniform — a second correction, because a first pass over-read
  this as "a contender has no derivation" and that is false.** A contender already loads
  that record: `materializeQ2AffOnBehalf` calls `loadFenceSession(claim.Winner)` and
  compares `ps.Q2AffGen` against `claim.Gen` (`qos2_shared_affine.go`). So for the
  **matching-record** population — the one F-064's reference skip is about — `ps.Gen` IS
  the session generation `mqttPubRelNames` needs and the derivation already exists in
  the tree, needing no new field. What is NOT derivable is the **absent, indeterminate or
  mismatched** record, where there is no `Gen` to read; and whether those states belong to
  residual 8 at all is itself unproven and is the thing to establish before costing
  anything. Only if they do is the cost a durable recovery-safe mapping from
  `(Winner, Q2AffGen)` to the owning `Gen`, or an additional persisted reference — priced
  for those cases alone, not for the population as a whole. The
  `mqttQ2AffClaim.Gen` field comment reads "winning session's durable generation", which
  is what misled the attempt; §8's comment-corpus sweep gains it. Two facts the attempt
  did establish and that survive: a filtered walk of ONE PUBREL subject already exists
  (`unsub_drain.go`) and each PUBREL carries the bound pair (`pubrel_correlation.go`), so
  the cost is in FINDING the subject, not in scanning it; and the `RWID`/`RetiredSeq`
  refusal does **not** categorically forbid a new field — that was proportionality
  against a Reason String and a log line, so a field closing what was then an alpha blocker
  would have received its own cost judgement. Also unproven either way: whether "no
  matching PUBREL" settles anything, since a late or in-flight store is indistinguishable
  from an absent one. - *The
  accumulation half* — **PARTLY LANDED.** Under a live generation reference the sweep
  now reaps the claims that are already durably TERMINAL, and collects the retired
  affine `$MQTT_inflight` register class together with the copy and the claim each
  register names. Two windows of the three are therefore closed: a skipped reap after
  `AffineDone`, and a `SourceAcked` committed with its response lost before the terminal
  contender's reap. `Lost` is untouched and not even attempted, since `reapable` refuses
  it unconditionally. Both live-generation arms reap through a CHECKED reaper carrying a
  shape precheck, because between the inventory's validation and the reaper's own fresh
  load a same-copy unsupported rewrite can appear and the plain reaper would delete it
  or normalise it through its `SourceAcked` advance. The register class is collected by
  `settleRetiredAffineRegister`: source `Complete` resolves the triple, source `Live`
  with the pair proven by that source records `AffineDone` from the register's own
  PUBREC evidence, deletes the copy at its exact sequence with a CONFIRMED delete,
  reaps, and deletes the register last because it is the only proof of that licence.
  Source `Unknown` and `Lost` settle nothing. - *W-c, the NONTERMINAL population, STAYS
  OPEN, and this is the reason — do not re-derive the remedy the earlier draft of this
  bullet prescribed.* That draft said the sweep should settle a claim left `claimed`
  with `SourceAcked` false and no copy once its source is authoritatively gone, guarded
  by an in-`mutate` "still nonmaterialized" check. **That is unsound, and the guard
  cannot save it.** Worked: (1) the pass proves the source gone and the copy absent; (2)
  a materializer dispatched earlier, holding the payload, STORES the copy — the store
  precedes its own phase CAS, so the record is still `claimed`; (3) the pass's fresh
  read therefore sees `claimed`, passes any phase guard, and commits `Discarded ∧
  SourceAcked`; (4) the materializer's advance merges `Discarded` in (`mergeProven`);
  (5) `q2AffCopyIsDisowned` reads a terminal claim of this very incarnation and
  `withdrawQ2AffCopy` deletes the copy UNPUBLISHED. The session never ended, so that
  disposal falsifies the `[MQTT-4.8.2-5]` premise `q2AffCopyIsDisowned` itself states,
  and it is the write this finding's own dossier refutes as non-conformant under
  `[MQTT-4.8.2-4]`. No positive fact licenses it: source absence does not revoke an
  already-dispatched payload, copy absence is an instantaneous observation, a `claimed`
  phase does not prove no store is in flight, a live record supplies no quiescence, and
  there is neither a cross-stream transaction nor any bound on how long a materializer
  may be in flight. Nor is there a truthful terminal fact to write instead: `AffineDone`
  would assert a delivery that never happened, `Lost` is refuted for an actor with no
  payload and is never reapable anyway, and `SourceAcked` alone leaves the claim
  nonterminal so `reapable` still refuses it. **What did land for this population
  instead:** the rule is EXTRACTED as `settleSourcelessQ2AffClaim` with exactly one
  caller, `recoverQ2AffClaims` on the owner's own resume, and HARDENED there — source
  read first and copy read LAST, the phase condition moved from `mutate` (which cannot
  veto, because `mergeProven` runs after it) to a PRECHECK inside the CAS loop, and a
  shape precheck beside it. Its remaining window, the final copy read to the CAS, is
  bounded RETRIES rather than one round trip and is stated in `docs/CONFORMANCE.md`. -
  *Retired affine registers*, the class this half also had to collect: a live record
  protects them through `refs.hasInbound(idHash, ps.Gen)` rather than
  `refs.hasAffine(idHash, ps.Q2AffGen)` — a DIFFERENT key, so a remedy written for
  claims alone misses them. That arm landed; see above. **The premise was enforced at
  TWO gates** — the reference skip and `discardAbsentWinnerClaim`'s generation-match
  deferral — and mutation showed that removing either alone changes nothing, so any
  remedy that works THROUGH the sweep must address both. The on-behalf remedy does not
  go through the sweep at all. The accumulation remedy DOES go through the reference
  skip, and it addresses that gate by NARROWING it rather than removing it: the skip
  still protects every nonterminal claim of a live generation, and
  `discardAbsentWinnerClaim` is untouched, because the new arms never reason about the
  winner's disposition — they act only on facts the claim itself already carries. **What
  keeps the technical finding open:** residual 8, and W-c above. Both are argued rather
  than pending, and both have a named route: residual 8 through the `Lost`-truthfulness
  item's PUBREL-carried claim reference, W-c through nothing currently available.
  **Which bar forbids closing W-c by residual, settled 2026-08-31**, because two wrong
  citations were offered first: it is neither F-066's coverage ruling nor F-036's scope
  consult — the first is specific to F-066's interpretation surface and the second
  adjudicated F-036's own two widened manifestations. The authority is this bullet plus
  §0's own governance, which prohibits the alpha while any blocker is incomplete.
  F-036's ruling is strong PRECEDENT for why relabelling a defect as a residual is
  insufficient, and is cited as precedent only. That was the release bar before the
  2026-09-02 scope decision; chapter P now owns these affine-only remainders and explicitly
  removes them from the alpha gate.
  **This box is not an executable alpha item, and later alpha lanes must not be redirected
  into it.** Both remainders are argued: W-c's remedy is refuted with a worked
  counterexample, and residual 8's reverse lookup is derivable for a matching winner
  record but not for an absent or mismatched one, as corrected above. F-066, which this
  bullet used to name as the next executable item, **landed 2026-09-01**; the next
  candidate is therefore F-036 below, whose OPEN 1 F-066's ruling dissolves. The
  smallest useful work HERE is establishing which record states residual 8's population
  actually occupies, since the matching state needs nothing new and only the others
  could need a persisted reference. That research belongs to chapter P and starts only
  after an explicit decision to invest in shared QoS 2.

  - [x] **Report a delivered shared QoS 2 message truthfully instead of as `Lost`.**
  Split out of F-064; not a conformance defect, so never an alpha blocker. **Done
  2026-08-30**, after six plan-review rounds. *The finding:*
  `materialized ∧ ¬AffineDone ∧ copy absent` was reported `Lost` for a message the client
  answered with PUBREC, because `mqttProcessPublishReceived` stores the correlated PUBREL
  before it reaches `mqttFinishQ2AffineDelivery` — untruthful rather than deviant, since
  `Lost` releases the group delivery without re-offering the message and the handshake
  completes from that same PUBREL, but it cost an erroneous loss report and a claim that
  was never reaped.
  **What landed, and the ROOT CAUSE is not the one this bullet used to name.** The design
  the bullet prescribed — make the correlated PUBREL carry the `Q2AffClaim` subject — is
  necessary and not sufficient: `mergeProven` resolved the `AffineDone`/`Lost` exclusion
  by whichever landed first, so a `Lost` that won a cross-broker race could never be
  corrected however good the later evidence. The fix is therefore in two halves. (1) A
  new claim field `PubRecProven` is the ONLY fact that outranks `Lost`, in either arrival
  order, on the ground that `Lost` is an inference from an absent copy and this is a
  proof. (2) The stored PUBREL carries a BOUND `(claim subject, copy subject)` pair —
  never the claim subject alone, which would credit a successor incarnation with a
  delivery it never made — and `reconcileQ2AffPubRelProofs` resolves it on resume, before
  `recoverQ2AffClaims` and before affine consumer restoration, performing its own reap
  because the recovery-and-reap route is skipped when that restoration fails.
  **Generic `AffineDone` is deliberately NOT the dominating fact, and a review round
  refused the version that made it one.** It means terminal copy disposition and is
  written by seven routes, one of which is the delivery gate's own `AffineDone || Lost`
  arm — so letting it dominate would make "this claim is already `Lost`" the reason to
  clear `Lost`, confirm the copy ack and reap, which is circular and reachable.
  Also landed: recovery refuses to infer a loss when the PUBREL walk was incomplete (an
  inference from a search that failed is a guess, not a fail-closed reading); the loss
  diagnostic moved after its CAS; the parser is a tri-state, so a malformed carrier is
  REFUSED rather than read as absent; `PubRecProven ∧ Lost` and `PubRecProven ∧
  ¬AffineDone` are refused record shapes, checked before the already-proven short circuit
  on both proof routes; PUBCOMP resolves the obligation BEFORE it untracks, so a withheld
  PUBREL keeps its packet identifier and Receive Maximum slot instead of freeing them for
  a wrap-around re-mint that would let the duplicate branch acknowledge the original
  away; and both enlarged records are budgeted — the proven claim shape in
  `mqttQ2AffClaimRecordMax`, and the affine PUBREL as its own ALTERNATIVE candidate
  `mqttQ2AffPubRelFormMax` rather than added to the copy's bound, which would sum two
  forms that never coexist.
  **The five mechanisms this bullet had already refuted were not re-proposed**, and three
  more were refuted during the plan loop: global `AffineDone` dominance, an in-memory
  per-pair suppression set (it cannot arbitrate a cross-broker race, and once dominance
  is durable it restates what the record already says), and withholding the PUBCOMP ack
  after untracking the entry.
  **Residuals, in `docs/CONFORMANCE.md`:** a bundle reset destroys the proof with the
  copy identity it names, both living on `$MQTT_out` (F-066's accepted-data loss, which
  its enforcement never claimed to fix — recreating that stream alone is now
  refused); a teardown carrying
  a MALFORMED carrier still loses it, because such a reference identifies no assignment and
  failing closed there would wedge the teardown forever — the ordinary late-proof case is
  CLOSED, since teardown now reconciles every stored PUBREL's proof into its assignment
  before deleting the consumer that retains it, and fails closed otherwise — what remains
  there is the interval BETWEEN that scan and the delete, which a stale broker's PUBREC can
  still fall into, and which needs a durable mechanism rather than the store-then-verify
  watermark read that was designed and REFUTED on THREE independent counts, recorded so a
  revised version cannot repeat the other two: the watermark is one replaceable record per
  CLIENT and `recordGenRetirement` advances on `MarkerSeq` alone, so a teardown of one
  generation can run while the slot names another; the verifying read is not
  crash-persistent; and the arm it would take does not clear a `Lost` already on the claim.
  Three candidate closures are named in `docs/CONFORMANCE.md` — completing F-036's
  stale-serving predicate for the retiring session generation, a retention handoff that
  keeps proof records independent of the consumer, or a claim-side "PUBREL owed" intent
  with register-driven recovery;
  a permanently unreadable claim or a malformed
  carrier defers the verdict and holds one identifier and one Receive Maximum slot; and
  an authoritatively ABSENT claim is acknowledged rather than withheld, which is stated
  as an accepted destructive consequence rather than as fail-closed.
  **It does NOT close F-064.** It supplies the durable PUBREL → claim association
  residual 8 names; the owner-independent claim → PUBREL REVERSE lookup that residual 8
  actually needs is still uncosted, and W-c still routes through nothing available.

  - [x] **A saved shared filter that fails restoration at CONNECT is silently
  dropped.** **This was F-050 filed twice** — same mechanism, same consequence, same
  dossier — and closing it ticks both boxes; see §0.3's F-050 entry for what landed.
  Kept here rather than deleted because this filing is the one that carried the F-064
  review's constraint on any fix: failing the CONNECT *after* the CONNACK is ungraceful,
  so it publishes the Will and can delete a zero-expiry session. The remedy respects that
  by not acknowledging at all — restoration moved BEFORE the CONNACK, where the existing
  pre-CONNACK abort discipline already discards the Will and leaves the lifetime
  ungranted.

  - [x] [F-076: Prove a shared group durable exists before restoring its
  membership](findings/shared-qos2.md#f-076). **Alpha blocker — done 2026-08-23, with the
  guarantee SCOPED and the other half split out as F-077.** Surfaced by F-050's plan
  review. `processSharedJSConsumer`'s resume fall-through subscribed to the recorded
  deliver subject and stopped, so a group durable deleted out of band left a live
  subscription on a subject nothing publishes to, with no error anywhere — and because
  shared configs are never persisted, `exists == true` means a RESIDENT session, which
  makes the defective path the **ordinary same-broker reconnect**. **What landed:** two
  batched classifications around the restore (a pre-pass before `processSubs`, acting
  before the subscribe; a post-pass after it repeating the FULL classification and feeding
  F-050's existing pre-CONNACK aggregation), four outcomes with absence and indeterminacy
  kept distinct, one canonical constructor shared by the join path and the validator, and
  whole-struct validation closed by a reflection bijection over every `ConsumerConfig`
  field — three bounded member-variable, two compared against an EXPECTED normalized value,
  one server-owned and constrained to the three named server metadata keys, with the
  normalized set MEASURED against a real create rather than listed. A `Cons` entry carrying
  a `DeliverGroup` was refused at all five ingresses **that existed then**; the count
  later rose to six, the sixth being the shared last-holder scan's own refusal, and fell
  back to five when F-105's shared-group lifecycle transfer (`e078092`) deleted that scan.
  It also fixed a latent defect in a shared helper: `lookupConsumersMulti` encoded an
  authoritative not-found and a successful nil `ConsumerInfo` identically, so the
  in-flight source cache was concluding a terminal `Lost` from a reply that proved
  nothing; it now resolves `Unknown`. **What it does NOT establish, and the tick rests on
  this:** configuration evidence, never ownership evidence. **Both of its stated residuals
  were since taken up by F-077**, which is where the current behaviour is described: the
  "no self-repair" residual is closed for the authoritatively-absent verdict (that one is
  now recreated) and retained for the other two, and ownership is proven on the CONNECT
  path by a classified record write — while the live SUBSCRIBE path was left unarbitrated
  as F-078, which has since landed. Read this bullet as the account of what F-076 itself
  established, not as the current contract. **Refuted and recorded:** re-issuing the
  remembered config, blind canonical `CreateOrUpdate`, any-error-means-absent, a short
  field allowlist, copying the normalized fields, and dropping the local entry so the next
  CONNECT recreates. Residuals in `docs/CONFORMANCE.md`: the delivery gap is not repaired,
  and the proof is fresh but not atomic.

  **F-105 note (audited 2026-10-05; corrected 2026-10-06):** the closure stands; its
  phrase about the shared holder scan now records the scan's deletion; see F-105's
  "Checked bullets resting on F-105".

  - [x] [F-077: Arbitrate
  ownership after any shared group durable create](findings/shared-qos2.md#f-077).
  **Alpha blocker — DONE.** The CONNECT half landed in `5cc8081`; the box stayed open on
  F-078, which has now landed and closes it. Split out of
  F-076 by a scope consult, and **a defect on `main` rather than one F-076 introduced**: no
  route that restores shared group interest revalidated that this broker still owns the
  membership it was keyed on, so a superseded broker can serve — and ACKNOWLEDGE — group
  deliveries under a membership the record no longer carries, destroying those messages for
  the members that legitimately hold it. **What landed:** one classified record write,
  `proveSharedMembershipOwnership`, CAS'd at `sess.seq` inside the accept transaction and
  run once per restore that installed, repaired or adopted shared interest. It is a WRITE
  because a read cannot arbitrate — 10164 comes back while a write for the subject is in
  process, so "the record did not move" is unprovable from a load — and it is a SEMANTIC
  no-op rather than a byte-level one, since every write mints a fresh `WID`. **The trigger
  is restored interest, not a create**, which was the first review round's finding: a resume
  whose durables all verify present-and-canonical creates nothing and still serves. It is
  a stack-local fact committed per filter only once the whole filter (its level-up companion
  included) succeeds, and kept apart from `created` and `charged`, each of which it had
  previously been inferred from wrongly. **10071 keeps its own lineage classification**
  through `disposalForDirectRejection`, which is the only place a sibling record is
  recognised on that arm and which mints the fork grant; collapsing it would assert 0x8E
  where the record supports only 0x89. **The absent-durable repair landed too**, closing
  F-076's "no self-repair" residual for that one verdict: the pre-pass now treats
  `errSharedGroupAbsent` as REPAIRABLE and every other error as terminal, stated as a class
  rather than an `if absent` at each site because F-076's batching seam closed only when the
  special cases were deleted. Repairability moves THREE decisions together — the batch
  termination, the pre-pass filter failure, and the resume arm's own return — and the
  POST-pass keeps absence terminal, because a durable absent again after the repair is one
  something is deleting underneath this CONNECT. **A durable created and then lost to a
  foreign verdict is LEFT IN PLACE**, and `docs/CONFORMANCE.md` states that it is not inert.
  Every deletion was refuted: the holder scan (since deleted; the group record's
  arbitrated holder count now decides) skipped same-client-ID records, so after a
  takeover a blind last-holder delete destroyed the NEW owner's durable, and
  `releaseSharedConsumer` refunds a historic charge through an already-rejected transaction.
  A refused resume sends no DISCONNECT — that notification is gated on the CONNACK having
  been written — but it is no longer SILENT: the pre-CONNACK refusal signal now carries a
  v5
  failure CONNACK here, within its scoped window. `make cluster-failover-tests`' regex is
  extended here, so F-078's
  replicated case is visible when it lands. **What kept the box open was F-078**, the live
  SUBSCRIBE path, which installed and revived the same interest with no ownership write at
  all — and which has since landed, closing this box with it. The CONNECT path needs no
  transmission gate because the restore gate already withholds everything until the
  CONNACK; the live path has no equivalent and that is the
  whole reason it is a separate item. Note also that the repair BROADENS the population
  reaching the live path, which F-078 now arbitrates.

  **F-105 note (audited 2026-10-05; corrected 2026-10-06):** the closure stands; its
  phrase about the shared holder scan is now past tense and names the arbitration that
  replaced it; see F-105's "Checked bullets resting on F-105".

  - [x] [F-078: Arbitrate a live SUBSCRIBE's shared interest before it
  serves](findings/shared-qos2.md#f-078). **Alpha blocker — DONE.** A live SUBSCRIBE now
  proves this broker still owns the membership before anything it touched may serve. What
  is withheld until that proof lands is stated exactly, because one class is deliberately
  exempt: the connection's QoS 1/2, PUBREL and affine deliveries WAIT, its **shared**
  QoS 0 ones are DROPPED, and its **plain** QoS 0 ones keep flowing. A plain QoS 0
  delivery acknowledges nothing on the group's behalf, so gating it would lose messages
  while protecting nobody, and a mixed packet therefore authorises plain QoS 0 traffic
  before the proof lands. `TestLiveSubPlainQoS0IsNotGated` requires that exemption
  positively. **What landed:** the
  same classified record write F-077 uses, reused unchanged, placed as the packet's LAST
  record write; a packet-scoped transmission gate armed before the packet installs
  anything, scoped to the CONNECTION and a per-session GENERATION and resolved by exactly
  one CAS through a defer that survives `runtime.Goexit`; ONE combined wait over that gate
  and the restore gate under ONE absolute deadline, which replaced `awaitRestoreGate`
  entirely (deleted — two mechanisms stated one fact); and a disposition tail following the
  two existing precedents, Foreign disposing and closing while every other non-owning
  outcome disposes nothing. (It closed with `0x8E` when this landed; **F-084 made it
  `0x89`**, so the arms now differ in the DISPOSAL alone.) **The trigger is wider than F-077's,
  and that is the subtle half:** the gate opens without a proof only for a packet that
  committed no trigger AND none of whose record writes was rejected by its **per-subject
  CAS**, because FOUR record writes in a SUBSCRIBE lose that evidence before the caller
  sees it — the early membership save converts a definite rejection into a per-filter code
  and continues, and the affine provisioning save, the affine unwind and the void-register
  store each swallow their own, the last through two chains AND through the failed-filter
  cleanup. A packet-local fact carries them. **Only a CAS rejection counts:** a local or
  ambiguous failure is evidence about nothing (an ambiguity is our own bytes maybe-landing
  under our own CAS), and treating any failure as evidence turned an ordinary per-filter
  failure into a connection-level refusal and broke 1352 ported-suite tests — a contract
  the affine items own, not this one. **The Will is consumed under the held gate on every
  non-Foreign close**, because a record-observation eviction can detach the session first
  and the detached path publishes only an already-owed IMMEDIATE Will. **An ambiguous proof
  is resolved by the packet itself under the gate it already holds** — releasing first lets
  the periodic pass, which resolves BOUND uncertain sessions, apply its generic policy,
  which disposes on forked; a forked resolution therefore retires the uncertainty WITHOUT
  disposing, discarding the landed-only continuations and leaving the void revision
  uncredited and the sequence, lineage and body at the acknowledged base. **Residuals** in
  `docs/CONFORMANCE.md`: a delivery admitted before the arm can still escape the gate
  (bounded, with the direction stated — it can only be interest predating the packet); the
  gate's collateral cost to the connection's other traffic; and the window between a
  foreign verdict and the detach. **Follow-up coverage, accepted as non-blocking and NOT
  claimed as done:** the four ack arms at the ownership-CAS boundary with a second-member
  oracle; the resume fall-through chain end to end; a **shared** QoS 0 drop (only the plain
  control is asserted — the paired half needs a v5 fixture, since shared subscriptions are
  MQTT 5.0 and a 3.1.1 attempt failed its own positive control); PUBREL and affine inside
  the window; a mixed packet; the disposal-ordering assertion with an
  exactly-one-DISCONNECT oracle; forked-keeps-the-Will with the Will armed, in both
  forms; and **the eventual-Foreign arm**, which needs a SECOND BROKER — a local
  contender cannot interleave a takeover while the SUBSCRIBE holds the session's
  ownership gate — so the single-broker fixture reaches the unclassified arm only.
  What DID land is TWO end-to-end oracles plus the gate's unit invariants, and they cover
  different arms. The replicated production-order fixture drives a live SUBSCRIBE against
  a real second-broker takeover and requires A's FIRST packet to be a DISCONNECT, which
  pins the **verdict**: "not a SUBACK" was too weak, and a mutation collapsing Foreign into
  a non-disposing refusal passed the earlier form. **The oracle was re-based by F-084**: it
  required `0x8E`, reachable from nowhere else in the tail, and that code no longer reaches
  the wire from any record-derived route — Foreign and Forked now share both the code and
  the Reason String. The fixture asserts the DISPOSAL instead, which is the fact the code
  stood proxy for and the one thing Forked and Local do not do. What used to stay unproved
  there — a tail that reported correctly and disposed nothing — is therefore now proved
  there, and the owed disposal-ordering and forked-keeps-the-Will fixtures below still carry
  the rest. The single-broker
  fixture covers the **unclassified** arm, where the wire output cannot discriminate at
  all — the discriminating signal there is the store-attempt count on an unchanged
  re-SUBSCRIBE: exactly two, the proof and the tail's own byte-identical reattempt, so a
  third store means the generic resolver re-ran what the tail had settled. A THIRD fixture
  pins the quiet branch's ORDERING as correct: a review round read it as a defect, since
  a packet whose ancillary save went AMBIGUOUS opens the gate while the session carries an
  unresolved write. Withholding there was implemented, measured and **reverted** — it takes
  the ported suite from 0 failures to **1355**, since it turns an ambiguous affine
  provisioning failure into a connection-level refusal, the contract the affine items own.
  The consult REFUTED the finding rather than deferring it: a quiet packet installed
  nothing, so its open releases only traffic backed by interest predating the packet, the
  convergence window `docs/CONFORMANCE.md` documents and now names this case in. **Eleven**
  mutations kill their paired tests, one of them added because the later-generation unit test
  was flaky: it asserted that the generation blocks a gate published before the waiter's
  SEND, where the code closes one published before its final READ, so a correct waiter failed
  it 26 times in 200 runs. A pre-wait seam made the property measurable and narrowed the
  claim to the boundary the code actually holds.
  **Refuted and recorded so none returns:** a
  per-subscription flag (F-050's first refutation — there is no subscription to hang it on);
  copying the restore gate's store-then-CAS ordering; releasing the gate and re-acquiring
  it
  around resolution; and treating a fence-busy resolution as a terminal outcome.

  The **superseded** description of the open defect follows, kept because it carries the
  analysis the fix rests on. The live half of F-077, split
  out by a non-convergence escalation after three plan rounds each re-cut the same seam and
  every finding of the third faulted the mechanism the second had just introduced. *The
  finding:* a live SUBSCRIBE that installs or revives shared group delivery interest serves
  under a membership it never revalidated. **Two routes.** *S3* is the `!exists` create plus
  the post-subscribe verify `CreateOrUpdate`, which genuinely recreates a durable a
  concurrent leaver deleted. *R2* is the resume fall-through, which creates nothing and
  verifies nothing, and its reachability took two rounds to establish and must not be
  re-derived: a SUBSCRIBE adds a shared filter; its rollback save fails DEFINITELY, so
  `readd` restores `cons[sid]` while `sharedSubs` has already been reset to the pre-packet
  snapshot; the client reconnects; the CONNECT builds its filters from `sharedSubs`, which
  lacks the key, so the sid is never collected and the resident `cons` entry survives with
  no deliver subscription; the client then SUBSCRIBEs and the fall-through subscribes on a
  remembered config. `readd` ALONE is not sufficient — `reopenTeardownDelivery` clears
  `closed` and the subscription was never unsubscribed — so the disconnect/reconnect step
  is what makes it real. F-076's justification for passing nil verdicts here ("a live SUBSCRIBE
  cannot reach that arm") is therefore false. **A per-subscription gate CANNOT work, and
  this is settled**: a shared QoS 2 SUBSCRIBE provisions the PUBREL and affine
  subscriptions BEFORE `processSharedJSConsumer`, and the PUBREL handler goes live before
  `processSub` returns, so there is no subscription to hang a flag on — which is F-050's own
  first refutation. Do not re-propose an `mqttSub` field. The callback census is complete
  at four. **Four open defects in the session-scoped gate the third round found**, all
  recorded in the dossier: the arming point cannot be both late enough to be meaningful
  and early enough to cover PUBREL without an explicit no-effect terminal outcome; an abandoned
  verdict left installed blocks the connection and leaks into the next one, because unlike
  the restore gate nothing unconditionally re-arms it; the wait needs the restore gate's
  THREE-way verdict with a timeout NAK for PUBREL and affine, not a two-way "non-open"; and
  a waiter captured on one packet's gate escapes the next one's, because the gate load never
  re-validates that its pointer is current. The dossier also carries the foreign/forked
  disposition with its two precedents, the carried test cases and the five still missing,
  and the residual F-078 must widen from F-077's resume-scoped version to the whole
  operation class.

  - [x] **`sharedDurablesHeldByOthers` fails OPEN on an undecodable record.** Authorised
  as
  its own item by F-077's escalation. **Done.** The holder scan `continue`d past a
  record it
  could not decode, while `sweepOrphanedConsumers` — which makes the same kind of
  delete-by-absence-of-a-reference decision — **aborts its whole pass** on exactly that
  shape. An undecodable record may be the very thing holding the durable, so skipping it
  could delete a group consumer other members still hold, taking the interest-retained
  backlog of every member with it. **The finding was stated too narrowly and the fix is
  wider than the bullet's words**, per the plan review: an implementation that only replaced
  the `continue` satisfies this text and keeps the bug, because holders are read out of
  `SharedSubs` **alone**, so a record carrying shared group consumers in `Cons` — the shape
  `mqttSharedConsInRecord` exists for, which `buildPersistedSessionLocked` never writes and
  both sibling readers already refuse — decodes perfectly and hides exactly the holder whose
  durable is about to be deleted. Reachability is identical for the two shapes, so both
  landed. The restated property is that the scan must never report a durable **unheld** on
  the strength of a record it cannot establish shared-holder absence from; all three arms
  (indeterminate walk, undecodable, unsupported shape) now converge on one `holdAll`
  disposition. **The shape check sits ABOVE the own-record skip**, because that skip
  presumes an interpretable record and a same-ID one may be a takeover winner's; that
  ordering is pinned by its own test case and its own mutation. **Deliberately NOT widened**
  to the orphan pass's other aborts — an unattributable client ID and an unvalidatable
  generation — since the own-record skip is keyed on `ps.ID` and is the documented takeover
  residual F-077's deletion refutations rest on, so tightening attribution would change that
  residual rather than this defect. The four sibling `$MQTT_sess` walks were swept and
  **none changed**: `buildSessionRefs` and the orphan-consumer sweep already abort, while
  `sweepSessionExpiries` and `adoptAbandonedTeardowns` correctly *skip*, because they decide
  what to write about the record in front of them rather than what to delete by absence.
  `docs/ARCHITECTURE.md` states that discriminator once, and its "refused at every ingress"
  enumeration gained this sixth ingress. **Residuals, in `docs/CONFORMANCE.md`:** nothing
  reclaims a durable spared by a refusal, since the orphan sweep aborts on the same record;
  the retention is **unbounded in count**, not merely in kind; and the scan remains a
  non-snapshot read rather than an arbitration, so the leaver-versus-joiner
  `CONSUMER.DELETE` race is untouched.

  **F-105 dependence (audited 2026-10-05; corrected 2026-10-06 by WP-F W3):** the property
  this bullet closes is now enforced by F-105 code, so the closure is conditional on
  F-105's integrated gate. What replaced the mechanism its passages describe:
  `sharedDurablesHeldByOthers` and `holdAll` were deleted by the shared-group lifecycle
  transfer (`e078092`, 2026-09-22). A group consumer's fate is the group record's
  arbitrated holder count on `$MQTT_shared`, executed from release verdicts, and names are
  incarnation-qualified, so a late delete cannot reach a successor; the
  leaver-versus-joiner race is the transfer's to state (F-105's "The SHARED transfer
  LANDED" paragraph), not this scan's. The retention residual is narrower than "nothing
  reclaims a durable spared by a refusal": a durable is withheld where no readable, valid
  lifecycle authority licenses its deletion, while the orphan sweep
  (`shared_lifecycle_sweep.go`) finishes retiring incarnations and deletes the consumers
  the group record derives retired. The undecodable-record refusal stands at the five
  remaining `$MQTT_sess` ingresses. See F-105's "Checked bullets resting on F-105".

  - [x] [F-082: Release a failed shared filter's own state independently of the
  last-holder
  answer](findings/shared-qos2.md#f-082). **Alpha blocker — done.** Split out by the plan
  review of the item above as a caller-specific lifecycle defect, and given its finding ID
  and
  dossier by this run's first consult. **The bullet's own statement of it was the smaller
  half.** As filed: `releaseFailedSharedFilter` tested `held[dur]` **before** removing the
  resident `cons` entry and taking either of its two exits, so a refusal stranded the closed
  delivery state, the `cons` entry and the ack-budget accounting, not only the group durable,
  while the other four callers withhold the consumer delete alone. That is fixed. **What the
  research added, and what a future reader should not re-derive:**

    - *The release was keyed on `sess.cons`, which the failing attempt has not written
    to.*
      `addJSConsToSess` runs only once the WHOLE filter has succeeded, so the no-config branch
      is the **common** shape of this route, not a corner — and it discharged nothing
      **whatever the scan answered**. The charge, the pending deliveries and the
      deliver-subject
      subscription are all created before the failing exit. `refundSharedCharges`'s comment
      ("a SUBSCRIBE disposes of the consumer through `releaseSharedConsumer`, which
      refunds as
      part of the release") is true of the INLINE cleanup, which passes the join's returned
      config, and was false of the landed continuation; it is corrected in place.
    - *The consequence was overstated in an earlier draft and is now stated from the
    code.*
      There is **no double count** — `processSharedJSConsumer` samples `alreadyCharged`
      and does
      not re-charge — so the defect is budget **permanently spent**: `tmaxack` stays
      raised for
      a membership that no longer exists, and the ceiling check then refuses later joins the
      session is entitled to. Indefinite for a never-expiring resident session, as this bullet
      already insisted.
    - *`held` merges two facts, and the obvious fix destroys a live membership.* The scan
      marks
      this session's own remaining `sharedSubs` before it walks any record. A multi-level
      key
      joins a second consumer for its level-up parent, and that parent IS the plain filter's
      consumer, so `$share/g/foo.>` and `$share/g/foo` share a durable — an unconditional
      local
      cleanup purges a surviving filter's state. Worse, `undo()` is shared by both rollback
      branches, so the **`priorExisted`** branch, whose write is an ADD that KEEPS the
      membership, registered the same continuation; only the own-hold arm stood between that
      and a live membership's consumers being released.
    - *The pre-save delivery barrier had the same keying defect* (`cons`-keyed barrier,
    durable-
      keyed void snapshot), which on the common shape left the barrier up and made the snapshot
      "only a moment in time" — the exact window the membership-removal write exists to close.

  **What landed:** a three-way per-durable disposition (own-held → nothing; foreign or
  refused
  → this session's state, no delete; unheld → both), with own-hold decided in the caller so
  `sharedDurablesHeldByOthers` is untouched; the release re-keyed on `(durable, deliverSubject)`
  as `releaseSharedGroupTarget`, with the config form a thin wrapper and the `held`-gated
  delete
  in exactly one place; `mqttSharedGroupNamesForKey` as the ONE derivation of what a
  shared key
  joins, with `mqttSharedDurablesForKey` and `mqttSharedKeyOwnsDurable` projections of it;
  a
  `cons` sweep that deletes EVERY match; the staging barrier keyed the same way with the doomed
  key EXCLUDED from its ownership test; the SAME fence on the confirmed inline half
  (`cleanupFailedSubClassified`), which holistic round 3 found had none — the scan withholds
  the consumer delete and nothing else, so the overlapping case tore down a surviving
  membership's delivery, identifiers and charge; and no landed release registered at all
  on the
  restore branch. **Two claims were refuted in review and must not return:** that a forked
  verdict
  leaves the strand alive (this route's forked arm EVICTS the session, so there is no resident
  object to strand — no conformance residual was added), and the double-count reading above.
  **Coverage:** nine unit cases, a production-route case whose two arming confirmations
  are
  load-bearing, and one row (**C7**) added to the last-holder scan's own table for the
  **indeterminate-walk arm, which predated `b659236` and was pinned by nothing at either
  layer**. Sixteen mutations were run and each kills a named test. `docs/CONFORMANCE.md`
  residual 3 of the unreadable-record entry is now closed; items 1 and 2 there are unchanged.

  **F-105 dependence (audited 2026-10-05; corrected 2026-10-06 by WP-F W3):** the property
  this bullet closes is now enforced by F-105 code, so the closure is conditional on
  F-105's integrated gate. What replaced the mechanism its passages describe: the
  three-way disposition is two-way. Own-held (decided by `targetsOwed` over the surviving
  memberships) does nothing; otherwise this session's state is discharged and the consumer
  is handed to the group record's verdict execution (`releaseSharedGroupTargetClassified`,
  then `executeSharedReleases`), never to a scan's answer. The one derivation is
  `mqttSharedGroupNamesFor(key, membership)`, keyed on the membership's acquisition
  incarnations, projected by `mqttSharedDurablesFor` and `mqttSharedOwnsDurable`;
  `mqttSharedGroupNamesForKey`, `mqttSharedKeyOwnsDurable` and the scan are gone from
  production. See F-105's "Checked bullets resting on F-105".

  - [x] **Release a shared UNSUBSCRIBE's and a SUBSCRIBE rollback's own state
    independently of
  the last-holder answer** ([F-083](findings/shared-qos2.md#f-083)). **Alpha blocker — done.**
  Filed by F-082's second consult as a separate caller with its own commit path. **The item
  WIDENED during planning, by an explicit decision recorded in the lane's `DECISIONS.md`:**
  enumerating the callers of the whole-durable release turned up a FOURTH unfenced site that
  no bullet owned — `finishSubscribeRollback` and its staging half in
  `rollbackSubscribePacket` — where a failed SUBSCRIBE that rolls back cleanly strips a
  *pre-existing, still-persisted* overlapping subscription of its delivery, its pending
  state
  and its charge. Because that is the same invariant and F-082's own twin, it landed here
  rather than as a fifth site-by-site item.
  **What closed it, and it is a PROPERTY rather than four patches:** the fence is now
  "does a
  membership that SURVIVES this operation still reach this durable?", asked of an explicit
  immutable `mqttSharedSurvivors` key set that each operation supplies — the live map minus
  the packet's whole doomed set at UNSUBSCRIBE staging (a SET, because one packet can leave
  both overlapping filters and a single `exceptKey` lets each protect the other), the
  live map
  at its commit half, and the PRE-PACKET SNAPSHOT at both halves of a rollback, whose
  live map
  still names the key being rolled back. `sharedKeyStillOwnsLocked` is deleted and its two
  F-082 callers converted, so one mechanism states the rule.
  **Three defects the bullet had not stated, found while planning:** the no-`cons`
  branch also
  released *neither* level-up durable in the mixed shape, because it excluded `" fwc"` sids;
  "this config is doomed" and "this durable is owed" are different questions, so a
  doomed key's
  config must go even when its durable survives (the commit half is three operations,
  not one);
  and `removeUnsubConsumer` was a REDUNDANT MECHANISM, deleted rather than guarded a third
  time — `execDeferredConsumerDeleteClassified` already was the correct plain teardown, while
  the deleted one mutated `sess.cons` without `sess.mu` and never unsubscribed the deliver
  subject.
  **A regression the review caught and the tests now pin:** nothing may discriminate a
  shared
  sid from a plain one LEXICALLY. A literal `.` encodes as `//`, so the ordinary plain filter
  `$share.foo` has the sid `$share//foo`; a `$share/` prefix guard strands its consumer. The
  authorities are `f.shared` and `cc.DeliverGroup`.
  **ONE site was deferred, not two:** the 3.1.1 forced leave's `cons`-versus-membership
  target
  mismatch, to the forced-leave item below, which has since **closed it**. Teardown is vacuous
  for this property and owes no separate fix, so it was deferred nowhere.
  **A defect the holistic review found in this change and this change fixed:** the
  SUBSCRIBE
  rollback's own `existedBefore` still classified sids lexically, so a rollback
  unsubscribed a
  pre-existing plain `$share.foo` subscription it was supposed to restore. **And a coverage
  limit stated rather than implied away:** the two planned resolver-continuation tests
  were not
  written, so the premise that the surviving-key sample stays true across the ownership
  gate is
  argued from the call sites rather than tested; the continuation mechanism itself is untouched
  by this item. See the dossier's coverage section.

  - [x] **The 3.1.1 forced leave's
  post-CONNACK destructive exit.** **Done. The leave is now a convergence obligation rather
  than a CONNECT exit, and it also closes the target mismatch F-083 deferred here plus F-045's
  forced-leave instance.** Surfaced by F-050's review. Moving the leave before the
  CONNACK was
  tried in F-050's plan and stays refuted — it converts an UNACKNOWLEDGED attempt into a
  permanent destructive mutation — so the leave did **not** move; what changed is how its
  failure is classified.
  **The finding was narrower and sharper than this bullet said.** `saveDefinitelyFailed`
  cannot
  contain a transient store failure at all: `persistRecord` arms uncertainty for every non-CAS
  store error, so the ordinary failure is **ambiguous**, and the ambiguous arm's remedy was
  DEAD CODE — `resolveUncertainNow` took the ownership gate the CONNECT already holds and
  needed a fence the frame already owned, so it reported "unresolved" without attempting
  anything. Five arms now: landed commits; ambiguous parks the continuation then
  resolves **in
  line under the accept transaction**; 10164 goes through the bounded contention retry; 10071
  disposes with a **classified** vocabulary — fork or ownership-unsettled since F-084,
  takeover-or-fork when this landed — instead of a bare close;
  everything else rolls the removal back inside the still-open transaction and fails closed.
  **Continue only on proof** is the invariant, which is why the restore verdict can
  simply wait
  for the settlement instead of needing a second admission barrier.
  **Two of this bullet's own claims were wrong and are recorded rather than quietly
  dropped.**
  "Can delete a zero-expiry session" is **vacuous** here: a non-clean 3.1.1 CONNECT always
  grants `mqttSessionNeverExpire`, and a clean start has no persisted membership to
  leave, so
  the two conditions cannot co-occur — pinned by a test so a later change that makes it
  reachable fails rather than passes. And a candidate harm I asserted while planning — the
  stranded entry being NAK'd into the group — is **unreachable**: both resume NAK loops require
  a live ack subject, which a register-rebuilt entry does not have.
  **The mismatch, closed:** doomed configs and release candidates come from the
  membership keys
  plus `cons`; the pending purge comes from the pending maps; a pending-only target is
  purged and
  **never** deleted. The pending derivation runs at **staging** too, because the write
  that
  removes the membership must also carry "these allocations were never transmitted" — otherwise
  a crash leaves a live register with no cancellation and no membership able to trigger another
  leave. `srcAckOwed` retirements stay exempt. The hand-rolled release sequence is
  deleted in
  favour of `releaseSharedGroupTargetClassified`, which also fixed a refund driven by `cons`
  entries that missed every membership-only durable.
  **Also landed:** an unparseable membership key is **refused pre-CONNACK** (no Will,
  lifetime
  ungranted) rather than silently stranding its durable; the restore verdict opens after the
  settlement and every post-CONNACK refusal terminates it with `abandonRestoreGate` alone;
  `recordStateBlocksServing` samples the session after the commit, covering cleanup
  writes by
  property rather than by enumeration.
  **Residuals** in `docs/CONFORMANCE.md`: three surviving destructive exits, an enlarged
  plain
  QoS 0 drop window, `resolutionAfterContinuations`'s marker gap for *other* callers, the
  continuation path's late-rejection asymmetry, and the pre-existing malformed-key
  exposure in
  the orphan sweep. **Shape:** 6 plan-review rounds (2 producing nothing — one cancelled for
  plan drift, one failed launch), 19 findings all folded or refuted.

  - [x] **Generalise the pre-CONNACK refusal signal.** **DONE — and the bullet was wrong
  about how much was left: it said ONE site and there were TWO.** The reconciliation verdicts
  (S2) were the site it named. `grantRequestedLifetime` had **both** of the properties this
  bullet gave as the reason S2 stayed open — it disposed the session internally before
  returning, and it collapsed a takeover with a fork — and the fixture that made it look done
  (`TestPreConnackRefusalSignalAtTheLifetimeGrant`) injects a PRE-STORE failure, so it drives
  the local arm only and the disposing arms had no coverage at all. `feedback_named_sites_are_a_sample`
  for the second time on this one bullet.
  **What landed.** Both helpers now RETURN an outcome and a disposal vocabulary instead of
  acting on the disposing half, and one caller-side closure, `refuseDisposing`, runs
  **abort → signal → dispose** for all THREE disposing sites **in the post-accept-save,
  pre-success-CONNACK window** — the shared-membership ownership write included, which was
  already in that order but is no longer the only site that states it. That window is the
  claim's scope: exits earlier in the CONNECT still refuse without a CONNACK, which is a
  stated residual rather than an oversight and was out of scope for F-050's half too.
  `mqttResumeReconcileVerdict` is **deleted** in favour of one
  `mqttPreConnackOutcome` shared by the two steps, with the merged verdict SPLIT into
  `DisposingNonForked` and `DisposingForked`; the classification behind both is extracted
  once
  as `classifyPreConnackSave`. `abortAcceptedResume`'s reserved `bool` is deleted — its stated
  purpose (carrying S2's foreign-versus-forked fact) is discharged by a typed result — and
  so
  are `connAckReasonForDisposal` and `grantRequestedLifetime`'s now-unused `asm`/`c` parameters.
  **The client is told something DIFFERENT from what the disposal claims, and that is the
  mechanism rather than an accident.** Three plan-review rounds established that sending
  `mqttDisposalTakenOver.reasonStr` would make this item's own purpose false: the disposing
  arms are also reached when the record could not be read, was absent, did not decode, was
  released, or sat at or below our base. So the CONNACK has its own two-value vocabulary —
  `0x89` + "ownership could not be established; try again" for a body claiming our parent,
  `0x83` + "session ownership could not be confirmed" for every other disposing verdict, and
  `0x83` + the step's own diagnostic for local and unresolved. `preConnackRefusalFor` is
  exhaustive with **no `default`**, because a default is how a future outcome would
  inherit an
  ownership claim it has no evidence for; an unlisted outcome gets a loud log and a bare close.
  A `panic` there was proposed by review and **refused**: `internal/mqtt` has zero production
  panics, and crashing a broker over an unhandled enum inside a reporting helper is worse
  than
  what it guards.
  **`docs/ARCHITECTURE.md` promised something the code never did.** It said the CONNACK
  carries "the vocabulary's Reason String", and `mqtt.go`'s shared-ownership site
  forwarded its
  own error text instead. That sentence is rewritten rather than fulfilled.
  **Residuals, all three in `docs/CONFORMANCE.md`:** the signal deliberately UNDER-claims
  (F-084 owns the other direction); the Reason String is dropped WHOLESALE, never truncated,
  for a client with a tight Maximum Packet Size; and the failure CONNACK is written with an
  **unbounded** socket write while the gate, the transaction, residency and binding are
  held,
  so a non-reading client can delay a local same-ID CONNECT. Bounding the shim's write is
  out
  of scope.
  **Shape:** 7 plan-review rounds (1 relaunched — a launch whose job never registered)
  plus
  one escalation consult that CUT THE ITEM. Rounds 1-3 each cut the same seam wider — what
  the
  broker is licensed to CLAIM — until the enumeration itself was the defect
  (`feedback_close_the_class_with_a_property`), and the consult split at the claim boundary:
  this item ships the signal, F-084 owns the claim. **Refuted and recorded so none
  returns:** a
  caller-side vocabulary re-derivation (two routes, and the 10071 route mints the fork grant);
  a signal callback inside the helper; sending the vocabulary's Reason String; fixing the
  takeover over-claim inside this item (its remedy needs a successor-evidence predicate that
  cannot yet be stated as a proof, and it changes F-078's Will disposition); and a mutation
  that "proves" a shared classifier is shared rather than copied (two correct copies survive
  it).
  **What a future change must not undo:** the CONNACK never reads a disposal vocabulary,
  the
  projection has no `default`, and the disposal is the CALLER'S so the signal can precede
  it.

  - [x] [F-084: Prove a successor before claiming a
  takeover](findings/ha-arbitration.md#f-084-prove-a-successor-before-claiming-a-takeover).
  **Alpha blocker - DONE 2026-08-27.** The successor predicate was **avoided rather than
  found**, and that is the resolution: no persisted shape proves a successor, so the claim
  was WITHDRAWN from every route that cannot hold one. `mqttDisposalTakenOver` now has
  exactly ONE reader, reachable only from `discardTakenOverClient` - the in-process
  same-client-ID replacement this broker holds both ends of under the ownership gate. Every
  record-derived route reports a fourth vocabulary, `mqttDisposalOwnershipUnsettled`
  (`0x89`, `mqttReasonStrOwnershipUnsettled`), which MINTS NOTHING. Three pure mappers
  collapsed into `mqttDisposalForLineage`, and the two classifying helpers now return the
  fork BIT beside the vocabulary - because both `0x89` vocabularies share a reason code, and
  the site that recovered the bit from `voc.reason` would have minted a grant for every
  unsettled record. **The Will half was FIVE routes, not one**, and four were real losses
  the review rounds found one at a time: a foreign disposal dropped a parked deferred action
  on the floor; a detached client's close publishes only an IMMEDIATE Will so an
  owed-delayed one vanished; the remote-persist eviction performed NO Will action at all;
  and the takeover discard reached classes that demonstrated no takeover. The fifth,
  `mqttAbortAcceptedConnect` discarding the ARRIVING connection's Will, is correct and
  untouched - which is why the rule is scoped to the DISPLACED session's Will, a
  pre-CONNACK disposing arm having both in play taking opposite actions. **What
  landed:** a two-phase `mqttForeignDisposition` (identity check first, then extract the
  stale client and any parked action and choose one typed `mqttWillAction`, all in one
  `sess.mu` section; execute
  outside it, after the caller's record transaction closes, and inside the ownership gate
  at every caller that holds one - the remote-persist eviction runs its action from a tail
  with no gate held at all, the INVERSE contract, and has its own oracle);
  `dischargeDeferredWill`, extracted Will-only from `applyDeferredDisconnect`'s lower half
  and called by it too so the discharge exists once; and the discrimination
  `settleBoundRejectionGated` needed, since `sess.c != c` is ALSO true when `sess.c` is nil
  and nil is what a resolver or a record-derived disposal leaves behind. **Refuted, so none
  returns:** reusing `applyDeferredDisconnect` (it applies the whole deferred teardown,
  assumes residency and an ours-verdict, and takes a fence two callers already hold - a
  self-deadlock); deleting `mqttRepairSaveVerdict` (five load-bearing control outcomes,
  three direct callers - `mqttDisposalForRepairSave` was the deletion actually wanted); and
  recovering the fork bit from a reason code. **Residuals, in `docs/CONFORMANCE.md`:** a
  genuine remote takeover is now under-reported as `0x89` and its Will is PUBLISHED rather
  than suppressed - an availability cost accepted over telling a live client to stop
  reconnecting on evidence a forged, replayed or malformed record produces identically; plus
  the in-memory consume window, newly reachable on these routes. **Shape:** 5 plan-review
  rounds (10 -> 7 -> 7 -> 1 -> 0 findings, ZERO repeats), 2 consults - one selecting the item
  and settling the five decisions its own bullet said had to come first, one granting a
  bounded revision 5 as a RECORDED exception to the pre-registered three-round cap.
  **What a future change must not undo:** the single reader, the explicit fork bit, the
  Will
  action chosen by LICENCE rather than by vocabulary, phase 2's position, and the
  absence of a re-park.

  ---

  **Everything below is the finding AS FILED on 2026-08-26, kept for the analysis it
  carries and NOT describing current behaviour.** Its present tense is the broker as it was:
  the takeover claim it enumerates has since been withdrawn from every route named, and the
  Will-disposition question it defers is answered above. Read it for the population and the
  reasoning, not for what the broker does.

  Split out of the pre-CONNACK-signal item above by an escalation consult
  after three plan-review rounds cut this same seam wider each time. *The finding:* the broker
  asserts **`mqttDisposalTakenOver` — "session taken over by a new connection", `0x8E`, which
  tells a client to STOP reconnecting — on evidence that establishes no successor at all.*
  `mqttLineageNotForked` expressly covers a legitimate successor, an unrelated lineage, a
  record carrying no identity, and a verbatim REPLAY of our own bytes, and every one of them
  concedes to the takeover vocabulary. *The population, which took three rounds to enumerate
  and is a sample rather than a census:* `disposalForDirectRejection`'s four conceding branches
  (unreadable, absent, undecodable, at-or-below-base); `resolveContendedRecordSave`'s **two**
  gone arms — the in-loop one and the post-exhaustion one — plus `movedVerdict`'s below-base
  fail-safe and its above-base non-sibling arm; `mqttDisposalForRepairSave`,
  `mqttDisposalForForeignResolution` and `mqttDisposalForRejection`, each of which maps a
  verdict that has already discarded the evidence; `disposalForRemotePersist`; and **two
  hardcoded takeovers that bypass every mapper**, in `mqtt.go` and in `sess_rejected_write.go`.
  *Why it is not a wording fix:* the remedy needs a predicate for what POSITIVELY proves a
  successor, and the obvious candidate — a usable record above our base whose digest differs
  and whose `PWID` names our `ackWID`, with both identities non-empty — identifies a
  *child-shaped body*, not proven ownership. Restore adopts `WID`/`PWID` **without
  validating
  provenance**, valid-shaped bodies can arrive through unconditional publication, and replay
  policy is unsettled. So it first needs decisions on trusted-versus-valid-shaped provenance,
  replayed child records, **parentless clean-start successors**, **descent beyond one hop**,
  and — for each resulting class — whether it **discards or consumes the Will**. That last
  one
  is behavioural, not vocabulary: F-078's live-SUBSCRIBE Foreign arm discards the Will *as
  a
  takeover* while Forked and Local take ordinary non-foreign handling, so an honest
  "unsettled" class reaching that arm would change Will disposition, and every bound
  displacement path needs the same review. *Design already sketched and reviewed:* a fourth
  vocabulary carrying `0x89` and reusing `mqttReasonStrOwnershipUnsettled` (the
  `mqttDisposalTeardownIncomplete` precedent — same code and text, its own `logReason`) that
  **mints nothing**, because a fork grant is a repair anchored to a record proven to claim
  our
  parent; a **sixth** outcome rather than folding unsettled into a disposing arm, since
  outcome-not-vocabulary is what F-078 switches on; and the deletion of
  `mqttDisposalForRepairSave` and of `mqttRepairSaveVerdict`, whose whole job is to guess
  a
  vocabulary from a verdict that discarded the evidence. *Refuted, so neither returns:*
  documenting it as a residual (it would ship a newly visible false claim), and fixing only
  the named mappers (the two hardcoded sites bypass them, so the invariant would be false
  immediately). *Tests it owes:* real direct-10071 **and forced-10164** fixtures, since a
  plain record deletion reaches the direct arm and credits nothing to the contended one; a
  no-mint mutation for the new outcome; and the DISCONNECT-side coverage on a **live v5
  shared-SUBSCRIBE** path — NOT the 3.1.1 forced leave, whose `mqttBuildDisconnect` returns
  nil for 3.1.1 so that route cannot send `0x8E` or `0x89` at all. *Availability consequence
  to weigh:* moving existing DISCONNECT paths from `0x8E` to `0x89` invites repeated
  reconnects while permanently unclassifiable state remains — worse than silence, better than
  a false takeover claim. *What the pre-CONNACK signal already does NOT do, so this is the
  only remaining exposure:* the failure CONNACK never carries the takeover text on any arm.
  - [x] [F-065:
  Re-read copy absence after the reaper's own SourceAcked
  transition](findings/ha-arbitration.md#f-065). **Alpha blocker — done, with the stated
  guarantee RE-SCOPED** (the same shape as F-063's), which is what the tick rests on.
  `reapQ2AffClaim` evaluated `reapable` on a copy reading taken BEFORE its own
  `SourceAcked` transition — a full JetStream round trip with retries — so a copy stored
  during it was invisible and the claim was deleted out from under it, leaving a copy
  the delivery gate can only NAK, forever, holding a `MaxAckPending` slot. Seven
  plan-review rounds established that this is **three routes, not one**, and only the
  first is the ordering: a stale-snapshot materializer needs no reaper race at all (its
  `advanceQ2AffClaim` returns no-op SUCCESS on a claim that has since completed, so any
  remedy keyed on "my CAS failed" misses it), and a group callback dispatched before the
  reap re-wins the create-only election, which `DeliverNew` cannot cancel. **What
  landed.** (1) The reaper's copy reading is taken after its own last write and is the
  last observation before the delete. (2) The claim carries an **incarnation nonce**,
  minted at the election and never rewritten, which scopes the copy subject and the
  copy's `Nats-Msg-Id` and fences the four incarnation-specific transitions inside
  `advanceQ2AffClaim` before it mutates — `SourceAcked` (a fact about the source
  message) and the teardown discards (facts about the session) deliberately opt out. (3)
  A materialization attempt WITHDRAWS the exact copy sequence it proved present once it
  proves no claim of that copy's incarnation remains; every store outcome now converges
  on the presence proof, so a landed-but-unreported store is resolved rather than
  abandoned. (4) The delivery gate disposes a copy whose live claim names a different
  copy subject — a positive fact, unlike the `claim == nil` arm, which still fails
  closed because an absence is not a fact. (5) A per-claim OPERATIONAL validator refuses
  an unattributable record on the six direct decision paths, while
  `mqttQ2AffClaimIdentityOK` deliberately stays as it is: it is the account-wide
  attribution floor, and tightening it would abort an unrelated session's teardown. (6)
  The claim record is now size-bounded at all, in the admission calculation and in the
  runtime backstop, over every reachable terminal shape — a QoS 1/2 Will bypasses
  admission entirely. (7) `DenyDelete` on `$MQTT_out` is now fatal at startup. **A
  second residual this change ADDS, and the tick also rests on it:** on the
  successful-PUBREC path a failed `AffineDone` leaves the copy unacknowledged and
  durably retires its register, and if the client completes PUBCOMP, disconnects before
  AckWait redelivery and keeps a non-expiring session that never resumes, nothing
  revisits any of it — the live session record makes orphan reconciliation preserve both
  the claim and, through a SEPARATE key (`refs.hasInbound` on the session generation,
  not `refs.hasAffine` on the affine one), the retired register. Before this change that
  path already accumulated the copy-and-claim pair and deleted the register
  unconditionally, so what F-065 adds is one retired register per such exchange:
  **unbounded in count, bounded in kind**, with the same growth law as the pair. F-064's
  accumulation half owns the live-generation reconciliation that must collect all three,
  and its bullet now names the register class explicitly so the remedy cannot close
  without it. **The residual, which the tick rests on:** the withdrawal is a
  compensation, not an arbitration — claim and copy are on separate streams and
  JetStream's atomic batches are per-stream, so no claim-side state can arbitrate a
  copy-side store. A copy stranded by an attempt that crashed, whose delete was
  unconfirmed either definitely or indeterminately, or whose final claim reading was
  indeterminate, is bounded by ordinary teardown or the gate's superseded-incarnation
  disposal, and is otherwise retained. (A later orphan-sweep consumer deletion was a third
  bound until F-036 course E removed it.)
  Stated in `docs/CONFORMANCE.md`. Also refuted and recorded so none returns: a
  `storing` phase marker, an owner-independent disposal on claim absence, a reaper-side
  cleanup, and two compensation designs keyed on which CAS failed.

  - [x] [F-066: Make the ten broker-owned streams one recreation
  unit](findings/ha-arbitration.md#f-066). **DONE 2026-09-01**, after six plan-review
  rounds, one decision consult on a fired stop rule, and the 2026-08-31 owner ruling
  that set the contract. The by-construction generalisation stays ABANDONED; what
  landed is the ruling's enforcement.
  *The finding:* deleting ONE broker-owned stream and restarting produced a SUCCESSFUL
  start — `ensureBrokerStream` sees one stream, creates it when absent, and validates
  the fresh stream against its own owner's list, with nothing comparing it to the other
  eight. The cost is not an empty stream but stale cross-stream references matching new
  unrelated state: an assignment claim naming a `$MQTT_msgs` sequence a recreated
  `$MQTT_msgs` reissued to a different message, a persisted drain naming a durable on a
  stream that no longer holds it.
  **What landed, in two mechanisms that catch different things and neither of which
  subsumes the other.** (1) A COUNT rule in `mqttPreflightBrokerStreamBundle`, an
  account-wide preflight over the SAME spec table the provisioning loop iterates, taking
  a new lookup-only interface so it cannot create what it is deciding about: zero present
  admits, ten admits, one through nine REFUSES naming present and absent. Each lookup's
  `(info, error)` PAIR is dispositioned together and UNDECIDED fails startup rather than
  reading as absence — the fail-open that would otherwise turn a transient outage into
  "the account is empty" and silently reprovision. (2) A per-stream EPOCH stamp:
  `mqtt_sess_epoch`, minted by `$MQTT_sess` and adopted from the authoritative re-read,
  is now stamped into all ten and compared by every validator through
  `validateBundleEpochStamp`, whose refusal of a malformed or empty EXPECTED epoch is
  itself load-bearing. `validateTeardownStream`'s private check was DELETED into that
  helper rather than left beside it. The count rule catches a member ABSENT when a broker
  starts, and its value is that the broker no longer silently recreates it; the epoch
  catches a member PRESENT but from another incarnation, which the count cannot see.
  Neither catches an operator who recreates a member by hand and copies the surviving
  stamp onto it — that is forgery, and outside what this claims.
  **Two structural changes came with it.** The retained-observer provisioning moved OUT
  of the spec table into `mqttProvisionEphemeralConsumers` beside the session-record
  watch — `$MQTT_rmsgs` was already the last entry so no ordering changed, and what it
  buys is that "no consumer is created until every stream validated" is checkable at one
  call site; the loop's now-dead rollback calls went with it. And `brokerStreamSpecs`
  lost every API parameter it carried for that step.
  **A guarantee was deliberately NOT built, and the reasoning is the item's main
  lesson.** Three revisions tried to guard "no durable mutation from a table callback
  before a later validator" — a `postDurable` placement test, then three assertions,
  then a claimed structural deletion — and review refuted each: the permitted
  `$MQTT_sess` post, `validate` and `observe` remain open, and the closures capture the
  receiver, so `as.jsa` is reachable whatever the parameter list says. A consult ruled
  the guard OUT: the invariant holds today, this change strengthens it, it was never
  this finding's obligation, and the same hypothetical exists identically at HEAD. It is
  recorded as a fact of the current code, READ NOT GUARDED, and no safety conclusion
  rests on it — the ordering argument rests instead on the initialization order at one
  call site.
  **Residuals, all in `docs/CONFORMANCE.md`.** The preflight is ten independent lookups
  rather than an atomic snapshot: under create-only concurrency an admission cannot
  wrongfully succeed, but an operator deleting streams behind the scan with `$MQTT_sess`
  last can be observed as ten-present and get a mixed old/new bundle — which needs the
  all-brokers-stopped precondition violated, and is why that precondition is
  load-bearing rather than advisory. Restore skew WITHIN one incarnation is
  undetectable. A concurrent first start can refuse one broker: `Server.Start` is fatal
  on a failed initialization and offers no retry, so recovery is a restart and depends
  on the supervisor — and if the broker that admitted zero dies having created one to
  eight streams, every peer refuses until the documented reset. The reset does not
  reconstruct a `Sources` topology on `$MQTT_rmsgs`.
  **What it does not buy:** the hazard is unreachable through a SUPPORTED operation, not
  eliminated; every stale-identity path still exists, and startup validation remains a
  snapshot. Completeness is demonstrable at this HEAD and GUARDED, not proven — the
  ten-entry table is hand-maintained, and what guards it is the preflight deriving its
  names from that table plus `TestStreamValidatorFieldCoverage` requiring every
  validator to reject a foreign stamp.

  - [x] [F-034: Add write lineage to outbound in-flight
  registers](findings/ha-arbitration.md#f-034). **Closed 2026-08-22 as an audit plus a
  census: no lineage was needed, and the audit found a real defect with a remedy that is
  not lineage.** The verdict is **not** what the title says: no lineage is owed, and the
  independently reachable defect is the drain release deleting by a persisted absolute
  `RegSeq` after an `$MQTT_inflight` recreation — a route F-066's own table documents
  and assigns to "that mechanism's owner". The remedy is an identity-conditioned
  exact-sequence delete plus **subject** addressing for the drain plus the retained
  enumeration watermark that BOUNDS the teardown's reap: two mechanisms closing two
  different windows, no new persisted field, and explicitly **not** a per-register CAS
  arbitration protocol, since the route is identity confusion rather than contention.
  Provenance classes A (terminal paths) and D (the orphan sweep) are **refuted as
  already protected** rather than conditioned, because the conditioning could not be
  mutation-proved. Every destructive path that reads a register body now binds that
  body's packet identifier to its subject — where destructive means DELETE **or**
  OVERWRITE, a correction three review rounds in the making: two earlier rounds swept
  *delete* sites and the read-after-CAS reconciliations replace a record at the sequence
  they just classified, which frees an identifier on evidence recovery refuses to read.
  Seven sites carry it, one mutation each — and the rule round 7 settled is wider than
  the binding: `loadInflightRecord` has **four outcomes** (empty, present but
  unreadable, readable, indeterminate) and every destructive path disposes of all four
  explicitly, which is what ended two rounds that each found one more outcome. Replacing
  the teardown purge also **retired a stream prerequisite**: nothing on `$MQTT_inflight`
  purges by subject any more, so `validateInflightStream`'s `DenyPurge` rejection is
  obsolete and removed, the project-wide field-disposition audit records the waiver with
  its reason, and the `docs/ARCHITECTURE.md` matrix moves that field from fatal to
  waived. Holistic round 20 found it and it is fixed here rather than filed, because
  F-034 is what made it obsolete. **The live-owner residue routes to F-036, and the
  necessity result is landed** as a passing reproducer that positively asserts the open
  precondition: `TestInflightLiveOwnerContentionNeedsAnOwnershipPredicate` shows every
  F-034 check is satisfied by a register a live peer may still be using, so naming
  cannot be what separates the two actors — the predicate needed is F-036's, and its
  fourth manifestation records it. **What the census delivered:** [the
  persisted-identity census](findings/persisted-identity-census.md), closure re-anchored
  from "every durable-state transition" (reopened three times in review, each time
  deeper inside JetStream, with no dry-out criterion) to the bridge's **interpretation
  surface** — 161 sites, AST-derived, bijection-checked both ways — plus a
  field-by-field disposition of every persisted body type and of
  `StreamConfig`/`ConsumerConfig`/`ConsumerInfo`/ `SequenceInfo`, over a type universe
  DERIVED by a recursive walk from the persisted roots rather than hand-listed. Eight
  tests enforce it. F-066's sample is applied as an ORACLE afterwards and found four
  real defects in the first enumeration. Two findings filed and not fixed: F-067 and
  F-068, plus **F-069**, **F-070**, **F-071** and **F-072** — all found while verifying.
  F-069 and F-070 are fixture premises with demonstrated mechanisms, and F-070's
  attribution is settled against a clean baseline that flakes nearly three times as
  often as this change does. **Three further findings this verification produced — a
  fixture race, a CONNECT-deadline enforcement defect and an unexplained
  write-uncertainty re-mark — are deliberately NOT filed in this commit**: holistic
  round 23 found their filings still entangled with F-034's own summaries, and the
  consult that followed ruled them out of this change and into a filing commit of their
  own. They are not lost; they are not this commit's to carry. **F-071 turned out to be
  a real DEFECT rather than a fixture premise and was classified as an alpha blocker at
  the time. The 2026-09-02 scope decision now parks it in chapter P.** It is explicitly
  not an MQTT conformance violation: the specification permits reordering on a shared
  path, and what it breaks is an INTENDED implementation property plus a product
  judgement (see its own bullet for the basis). Reproduced from a single publishing
  Client; the broker's own packet-identifier allocation shows it transmitting a
  later-published message first. Four explanations of mine were refuted before the
  evidence settled it, and the classification itself was wrong four times. **F-066's
  completeness deferral is NOT discharged, and that is stated rather than glossed.**
  Review of the first implementation established that the interpretation-surface guard
  is bijective against a hand-maintained root set and keys sites by unqualified function
  name, so a new branch inside a named function, a new wrapper around the transport
  primitives, or a same-named method on another receiver can still pass. It is a guard
  that catches real drift, not a proof that none is possible, and the plan's own
  no-fallback rule says such a class may claim neither completeness nor the discharge.
  Message headers and subjects remain named and not enumerated field by field. Same
  shape as F-032; the consequences it had to bound were a freed identifier still in
  flight and a register whose deletion was proven, and both are bounded. **It also
  inherits a census F-033 deliberately did not attempt.** F-066 establishes that
  cross-stream identity coupling exists as three ad hoc instances rather than a
  mechanism, but explicitly makes NO claim to have enumerated the identities involved —
  four attempts were each falsified by review. F-034 owns `$MQTT_inflight`, which holds
  four of the confirmed cross-stream references, so its audit opened with that
  enumeration and delivered it: a **field-by-field pass over every persisted record
  type**, every field dispositioned, cross-stream or not. Not a filtered sweep — each of
  F-033's four failures was a filter presented as a sweep (streams already known,
  `json:` tags, a keyword grep, a hand-listed carrier set), and two were refuted by an
  entry in the table they introduced. Add the two questions F-033's route inventories
  never asked: *which `stopAll` procedures name this stream alone*, and *what does this
  stream's persisted state reference that another stream issues*. One input is already
  known and must be dispositioned as a PAIR rather than as two fields: a register's
  `(Q2AffClaim, Q2AffCopy)` names a record on `$MQTT_qos2assign` and a message on
  `$MQTT_out`, and F-065 established that neither field alone identifies the delivery —
  the binding that does is the register's own source.

  - [x] **Slice A of the arbitration
  design: the session-record watch.** Ownership changes only by a write to the client's
  record subject, so the observation channel is now a per-broker ephemeral consumer on
  `$MQTT_sess` instead of the at-most-once `$MQTT.JSA.*.SP.*.*` notification, and a
  quiet session still costs no write per interval. An observation establishes that the
  record MOVED and so is no longer ours; it names no successor, which is why the
  displaced client is answered 0x89 and not 0x8E (F-084). The watch is lossy by construction,
  so convergence rests on the PAIR: a committed-read backstop on the periodic pass
  re-reads each resident session's record, feeds the same trigger when it MOVED, arms
  the ownership marker when it is GONE (the shape no sequence comparison can see), and
  re-fires any trigger a refusal left undecided — which also closes "a refused eviction
  can be lost entirely". The eviction worker gained a digest arm so a watch delivering
  this broker's OWN ambiguous landing can never evict the rightful client over its own
  write, and refuses equally when an unusable load leaves nothing to compare. Residuals
  in `docs/CONFORMANCE.md`.

  - [x] [F-036: Revalidate quiet resident-session
  ownership](findings/ha-arbitration.md#f-036). **DONE 2026-09-03, in two units. Unit 1
  landed 2026-09-02 (the incarnation completion lease, `901ee8f`); unit 2 — the
  two-dimensional PUBREL classifier and sites 1 and 3 — closes the box.**

  **UNIT 2, 2026-09-03.** Six plan-review rounds (blocking findings 8 → 4 → *no output* →
  3 → 0 → 0 CLEAN; round 3's job was lost by the companion and was relaunched as round 4)
  and one scope consult that decided both open design questions. **The `advisor` tool was
  AVAILABLE, which is the first time in any F-036 attempt** — all seven earlier consults
  were Codex-only. Lane `~/.claude/lanes/f-036-unit2/`; the dossier section **"F-036's
  seventh attempt (2026-09-03)"** is canonical, with the mechanisms in
  `docs/ARCHITECTURE.md` under "A deleted record is retired under a licence, not under its
  absence", "The generation-retirement watermark" and "Orphan reconciliation".

  **Site 2, the classifier.** A PUBREL now reads the retirement watermark between its hold
  proof and its onward store — the ordering is `R → P → W → S` — and refuses the fan-out
  when the watermark is valid AND either it names this incarnation OR its `mseq` exceeds
  the sequence this broker is acknowledged for. It answers the population NEITHER existing
  fence can see: a hold the discard's one-shot enumeration never named, and one F-062's
  cutoff SPARED above its own watermark. Both are decidable because the watermark is
  written before any teardown deletion and an unconfirmable write aborts the teardown with
  nothing deleted. **The identity prerequisite the second attempt found unproved is
  supplied by unit 1's packet lease, and it was verified against the tree rather than
  assumed** — but the plan's first revision OVERSTATED it and review round 1 corrected
  that: the lease pins the incarnation and therefore the generation, NOT `sess.seq`, so the
  sample takes the record transaction. Corrupt, indeterminate and `seqUncertain` fail the
  release closed with no PUBCOMP; a confirmed refusal releases the hold nonterminally and
  only THEN reaps its own claim.

  **Site 1, the bound deleted record.** `reconcileResidentOwnership` already armed the
  ownership marker for a record deleted under a bound session; nothing consumed it for a
  client that never sends again. The periodic pass now retires such a session itself under
  a LICENCE — a valid watermark whose `mseq` exceeds our sequence, plus the record
  authoritatively absent, plus a full in-fence revalidation — never under the absence
  alone. Round 1 found the plan's ordering was DRIFT from the consult's ruling and
  impossible as written, because `retireBoundIncarnation` detaches and mints before it
  returns: the proof therefore runs in a PRE-DETACH callback, after `prepareClear` has
  opened the record transaction. Failure is refusal, never disposal under a live owner, and
  the unwind guard lives inside the helper because the caller does not yet hold the
  prepared clear. The Will is NOT suppressed (F-084): a watermark proves a teardown, never
  a successor. **A convergence gap found while reading the existing bound-rejection test,
  not by review:** once the marker is armed the periodic pass diverted to the unbound
  settlement forever, so the FIRST refusal would have been final — a bound armed session
  now goes to the ownership backstop instead.

  **Site 3, the sweep.** One property rather than three site fixes: any arm that can still
  destroy live session-generation state requires a valid watermark whose generation EQUALS
  the candidate's, and RETAINS without it. Gated at three arms, each on its own terms —
  and revision 2's claim that all three share `refs.hasInbound` was FALSE for
  `deleteOrphanPubRelConsumers`, which round 2 caught as round 1's finding recurring. A
  bounded positive-only memo remembers the monotone matching observation; no negative
  outcome is ever cached. **The safe non-destructive tail still runs without proof** —
  once the payload is absent, an unchanged claim and a terminal tombstone are reaped — which
  is what keeps the gate from being a blanket retain. `discardAbsentWinnerClaim` is
  deliberately UNGATED and out of scope under the supported-scope ruling.

  **A structural inventory guard over the gated arms was designed, reviewed THREE TIMES
  and DELETED**, and that deletion is the item's clearest lesson: its narrow claim was
  honest, but the plan still concluded "a fifth arm cannot be added silently", which does
  not follow — a primitive-only AST inventory cannot catch a new unsafe CALLER of an
  already-inventoried destructive helper, and completing it needs the bounded SSA
  reachability this repository has ruled disproportionate. The property rests on the census,
  the behavioral negatives with exact payload assertions, and their positive controls.

  **The accepted price, in `docs/CONFORMANCE.md`:** a permanent, unbounded retention leak
  on the sweep side (nonterminal holds with full payloads, absent-generation registers,
  PUBREL consumers with their copies) wherever a matching retirement is not observable,
  because the single-slot watermark makes that population permanent; plus the no-licence
  half of site 1, where a bound silent client may still strand. A leak is preferred to a
  loss — the same trade course E took. **No bounded variant exists from current facts**,
  and Codex refuted the one I pressed for: a dead generation with an absent record and a
  live remote one with an absent record are observationally identical to the pass.

  **31 mutations killed; ELEVEN initially SURVIVED** and each produced a new
  discriminating
  test — the between-read seam, the split-index seam, the abnormal-exit Goexit, the
  Will-publication observation, the `seqUncertain` release, the tombstone-present fixture.
  **One mutation is reported as NOT DISCRIMINATING rather than forced:** making the
  tombstone delete unconfirmed survives, because the mandatory absence read that follows
  SUBSUMES the confirmation, so the plan's claim that an async delete is insufficient proof
  was itself corrected. Revision 1's `>` → `>=` mutation was deleted as invalid, equality
  being unreachable for a bound session.

  **SIXTH ATTEMPT, 2026-09-02: unit 1 IMPLEMENTED**, after the user OVERRODE the fifth
  attempt's binding fallback ("no sixth F-036 design; F-072 next") with a direct
  instruction to solve this finding, Codex leading and stop triggers converted into
  revision triggers. Seven plan-review rounds (findings 12 → 10 → 10 → 3 → 4 → 1 → 0,
  CLEAN at round 7) and three scope consults; the `advisor` tool was unavailable for
  every consult, so all three were Codex-only. Lane
  `~/.claude/lanes/f-036-sixth-attempt/`; the dossier section **"F-036's sixth attempt
  (2026-09-02)"** is canonical for the design, and `docs/ARCHITECTURE.md`'s "The
  incarnation completion lease" for the mechanism.

  **What unit 1 CLOSES: the incarnation admission invariant**, including the
  single-broker clean-start silent loss of an acknowledged QoS 2 message. Property P —
  a session object's incarnation generation changes only while no client is bound to it
  — is now MECHANICAL: `setIncarnationLocked` requires a retirement permit that only a
  closed, drained incarnation tracker over `sess.c == nil` can mint. Ownership is
  acquired at three STRUCTURAL boundaries rather than at a list of sites — per packet at
  the single pre-dispatch point against the tracker the CONNECTION was bound to, per
  delivery inside the one `sess.processSub` wrapper every session-scoped callback
  reaches its subscription through, and per timer FIRE (never per delay, and before the
  client-ID gate) — which is what finally closes the off-read-loop continuation class
  that stopped attempts 2, 3 and 4. `clear()` is split into `prepareClear`/`destroy`,
  the clean-start arm detaches between them under a capsule carrying the displaced
  client and its parked action, and every disposition arm consumes that capsule instead
  of re-reading `sess.c` (the `sess.c`-reading superseded disposal was DELETED).
  **Failure is refusal, never disposal under a live owner**: a drain ceiling or an
  unresolved-state refusal reopens admission and leaves the incumbent bound with its
  Will, socket, expiry and disconnect state intact — the disposition the fourth attempt
  could not supply. Residuals in `docs/CONFORMANCE.md`, including that a clean session's
  discard may be DEFERRED (parked non-resumable, completed by the drain trigger or the
  next CONNECT) rather than abandoned, since [MQTT-3.1.2-6] requires it and the expiry
  sweep skips expiry-zero records.

  **One real regression the existing tests caught, and the rule it leaves:** detaching
  before the destructive half made `sweepDeadSendHoldsLocked` read every F-089 debt of
  the displaced connection as ownerless and retire a LIVE one, freeing an identifier the
  client still held. A displaced connection now stays the authority for its own send
  holds while nothing is bound in its place, self-clearing at the rebind — which is the
  event F-089 already said retires the debt. **Any change that moves a detach earlier
  must re-check every predicate that reads `sess.c` to decide liveness.**

  **What unit 1 does NOT do, and what unit 2 must:** the two-dimensional PUBREL
  classifier and sites 1 and 3 are untouched, and the lease identity is exactly the
  identity prerequisite the second attempt found unproved — a classifier caller can now
  prove it acquired the currently bound incarnation, separately from the subject-derived
  session generation and the unrelated per-exchange `rec.gen`. The teardown-owned
  release of shared group durables was ruled OUTSIDE this boundary (scope consult 2):
  its scan-then-delete race on a deterministic group name was the pre-existing
  shared-subscription residual, needed group-scoped arbitration rather than an
  incarnation lease, and unit 1 neither created nor claimed to close it. (History: that
  race is CLOSED since by F-105's shared-group lifecycle transfer, 2026-09-22.)

  **Course E, LANDED 2026-09-01, is a separate earlier change.** What landed there is
  one deletion —
  `deleteOrphanQ2AffConsumers`, so the sweep's candidate-reclamation route no longer infers
  affine-consumer deletion from ABSENCE — which removes two of the ten destructive decisions
  and dissolves F-033's adoption blind spot. Read that scope exactly: teardown-marker
  adoption still reaches `deleteTeardownTargets`, so a sweep pass can still CAUSE an
  affine consumer delete, and that pre-existing route is not claimed race-free — it is a
  manifestation this finding keeps, recorded in the dossier. What remains, and is the whole
  of why this is still a blocker: the **incarnation
  admission invariant** (including the single-broker clear-to-bind silent loss of an
  acknowledged QoS 2 message), the **two-dimensional classifier** whose identity
  prerequisite is unproved, and **sites 1 and 3**. Nothing in course E touched any of them.
  Lane `~/.claude/lanes/f-036-course-e/`.

  **FIFTH ATTEMPT, 2026-09-02, STOPPED UNRESOLVED**, with no implementation code and
  **no design** — the stop came in the PREMISE phase, one step earlier than the fourth's.
  The gate (pre-registered by consult before any design existed) made a written premise
  audit a deliverable of its own and required it to prove the asynchronous boundary
  finite; the audit's adversarial challenge failed it under the gate's §2 and fired stop
  trigger 1, which under its own rule permits no errata pass and no design round. Advisor
  was available for the gate consult and unavailable for the challenge, so the challenge
  was Codex-only; all eight checkable claims in its verdict were verified against the
  tree and held. Lane `~/.claude/lanes/f-036-completion-ownership/`; the dossier section
  **"F-036's fifth attempt (2026-09-02)"** is canonical for what the challenge
  established and the refuted design direction. **The box stays open and every
  previously outstanding item stays outstanding** — nothing was fixed or narrowed.

  What it ESTABLISHED, so a sixth attempt does not re-derive it: (1) the unit-1 ingress
  set includes **timer-installed callbacks** — the close path's `scheduleSessionExpiry`
  AfterFunc reaches `expireSession` outside every WaitGroup — and the three
  account-internal delivery callbacks; (2) the fire-and-forget `sendq` population is
  **four classes, not two** — exact-sequence deletes (identity-safe), consumer acks,
  **consumer deletes** (`deleteConsumer(noWait=true)`, three live callers) and an
  **application-visible publication** (`mqtt.go:16430`) — and shared-consumer identities
  were, at the time, deterministic group-wide names no generation scoped, so a queued
  shared ack or delete could act on state a new incarnation or another member had since
  recreated (history: since F-105's shared-group lifecycle transfer a group durable's
  name carries a physical incarnation no successor reuses, and a delete is issued only
  for a retired one); (3) the narrower choke-point claim SURVIVES — every production
  message-delivery handler registers through `client.processSub` — which is what makes a
  structural ownership boundary attachable there; (4) the **pre-`clear()` takeover-tail
  move is REFUTED** three ways: the tail never detaches (`sess.c` stays bound through the
  re-mint), a synchronous detach cannot stop already-admitted completions
  (`recordWithheldDeliveryLocked` deliberately ADMITS `sess.c == nil` and recreates
  `sess.deferred`), and before `clear()`'s unresolved-refusal arm it discards the
  incumbent's Will with no successor established (F-084). Disposal is not quiescence,
  detach is not quiescence, `sess.c == nil` is not quiescence. A sixth attempt starts
  from the corrected constraint list in the dossier — ownership transferred into every
  queued effect including shared deletes/acks and application publications, socket close
  treated as cancellation never completion, re-mint only at owner count zero, the
  unresolved-refusal arm preserved.

  **FOURTH ATTEMPT, 2026-09-02, STOPPED UNRESOLVED**, again with no implementation
  code, after one design pass and one consolidated adversarial challenge under a gate
  pre-registered before the design existed. It took up the direction the third attempt
  named — operation-duration quiescence at the object-reuse transition — scoped to
  unit 1, the incarnation admission invariant. The gate failed criteria 1, 2, 5 and 6,
  passed 3 and 4, and fired three stop triggers, which under its own rule permits no
  revision. Advisor was unavailable, so the gate was Codex-only. Lane
  `~/.claude/lanes/f-036-quiescence/`; the dossier section **"F-036's fourth attempt
  (2026-09-02)"** is canonical for the design, the five findings that stopped it and
  the mutation audit. **The box stays open and every previously outstanding item stays
  outstanding** — nothing was fixed or narrowed.

  What it ESTABLISHED, so a fifth attempt does not re-derive it: (1) the **ownership
  term `sess.c != c` is the admission barrier** and `routeClosing` is redundant beside
  it — but that governs ADMISSION only, not effects already admitted, queued, or raised
  off the read loop; (2) the examined wait-for graph **is acyclic**, though not for the
  reason the design gave, since teardown is not the only displaced-side consumer of the
  client-ID gate; (3) a **wall-clock ceiling is achievable**, while a safe disposition
  once it fires is not; (4) three positive facts hold — a packet following CONNECT in
  the same buffer is gated, PUBREL evaluates the pre-dispatch gate before deriving its
  hold subject, and `mqttEnqueuePubResponse` is synchronous with its handler; (5) **two
  effect classes defeat any read-loop-scoped signal**, queued JetStream effects reached
  through `deleteMsg(..., false)` and off-read-loop delivery/register continuations —
  and the record fence does NOT cover the second, because `persistVoidedRegs` mutates
  before taking it; (6) a timeout arm needs **completion-owned cleanup**, which is a new
  proof boundary rather than a placement or an error code. A fifth attempt must govern
  BOTH escaped effect classes and supply a safe post-timeout disposition. None of this
  shows quiescence impossible in general; it shows a read-loop-scoped signal cannot
  express it.

  **THIRD ATTEMPT, 2026-09-01, STOPPED UNRESOLVED**, again with no implementation code,
  after one property-and-route pass and one adversarial challenge under a pre-registered
  stopping rule. It delivered what the second attempt said was OWED — a lifecycle property
  and a mechanically generated interprocedural effect matrix instead of a fifth
  enumeration — and the challenge refuted enough of both that no plan may rest on it. The
  dossier is canonical; the gate is this. **What it ESTABLISHED and must not be
  re-derived:** the property **P — a session object's incarnation generation changes only
  while no client is bound to it** — is SOUND, is stated over lifecycle TRANSITIONS (so it
  covers `clear()`'s indirect `recordEpoch` advance without naming it, which is what
  refuted design 3 could not do), has exactly TWO enforcement points because
  `setIncarnationLocked` has two production callers, and is violated by exactly ONE call
  site, the CONNECT clean-start arm at `mqtt.go:12852`. P retires the per-connection
  incarnation stamp **for the entry gap only**. The five-route ownership table is audited:
  PUBLISH and PUBREL carry NO ownership term at all; PUBACK/PUBREC and PUBCOMP do. Route A
  (detach before the re-mint) does NOT violate F-089 or F-091 — that question is answered
  — but is unsafe without a drain, because the takeover's close is asynchronous. **What it
  RETRACTS:** its own headline. The matrix's size does not prove that per-effect placement
  has no closed set; at least 67 of the 150 measured static sites are not placement targets,
  and the "indirection-sound" closure that reached 89% of the package is precision collapse,
  not a reach boundary. No later attempt may cite those numbers as a proof. **What it leaves
  OPEN, and where a fourth attempt starts:** in-memory writes by a stale continuation are
  NOT harmless — `retireSourceLocked` and `noteQ2AffCarrierRefused` lazily RECREATE the maps
  `clear()` nilled — success-shaped responses were measured and then left with no
  disposition at all, and the answer neither this pass nor any before it proposed is
  **operation-duration quiescence at the object-reuse transition**: no re-mint until the
  displaced route and read loop have drained, governing all three effect classes at one
  lifecycle boundary rather than site by site. Lane
  `~/.claude/lanes/f-036-property-audit/`, which keeps the SSA instrument and its outputs;
  the dossier records the seven defects in it so a rebuilt one does not repeat them.

  **Alpha closure after the 2026-09-02 scope decision — HISTORICAL; this is the ruling
  the closure was carried out under.** As of that date F-036 remained an alpha blocker:
  its clean-start loss was reachable through ordinary QoS 2, and its quiet-owner and
  inbound hold decisions were not affine-only. Those supported paths were closed with the
  quiescence, classifier and sites named here. Do **not** require the change to preserve the
  experimental shared-QoS-2 affine integration. If a sound general fix breaks an affine
  branch or its tests, record the regression under chapter P's single accumulation item;
  do not widen F-036 or reject the fix on that ground.

  **ATTEMPTED BY CONSTRUCTION
  AND STOPPED UNRESOLVED 2026-08-27**, after three plan-review rounds and two scope
  consults. **No implementation code was written**: the only working-tree changes the attempt
  produced are this documentation filing and the `RWID` comment correction it propagates.
  Slice A above converges every MOVED record, bound sessions included. What is left is
  three enforcement sites of one causal defect — *the previous owner keeps serving, or is
  misread as gone, after durable ownership moved*: (1) the DELETED record on a BOUND
  session, where arming the ownership marker hands the case to the record-rejection
  settlement, whose unbound half refuses while a client is attached and whose bound half
  runs pre-dispatch on that connection's own readLoop, so a client that negotiated Keep
  Alive 0 and then goes silent still holds a session another broker tore down; (2)
  `arbitrateQoS2ReleaseClaims` enumerating hold subjects ONCE, so a peer that has not
  observed the ownership marker delivers onward after the session was discarded — widened
  rather than narrowed by F-062's landed cutoff, which also spares an enumerated hold
  above the watermark; (3) the orphan sweep inferring liveness from this broker's
  residents plus persisted records only, so a live remote generation whose record is
  absent has its state condemned. A fix must answer all three; the attempt's scope consult
  ruled that restating the widened manifestations as residuals cannot close an alpha blocker.
  **What the attempt ESTABLISHED, so none of it is re-derived.** *The site-1 licence is
  found and it is sound*: a valid retirement watermark whose `MarkerSeq > sess.seq`, plus
  the record absent, plus a full in-fence revalidation, positively proves a teardown marker
  landed on this client's record subject after the record this broker is acknowledged for
  — because a claim marker is a CAS against the sequence the claiming party believed current,
  a failed `recordGenRetirement` aborts before any deletion and leaves the record MARKED,
  a landed local write moves `sess.seq` while an ambiguous one arms `seqUncertain`, and
  apply-time watermark regression can only cause a false NEGATIVE. It survived three separate
  attempts to break it, including a session RESTORED at CONNECT (authorship of the base is
  irrelevant) and `finishAdoptedTeardown`'s transient session (never in `as.sessions`). *No
  new durable state is needed for it.* **`RWID` was NOT the predicate this finding needs,
  and the field has since been REMOVED ENTIRELY** — the watermark is one slot per client,
  so requiring `Gen`/`RWID` equality recreates exactly the superseded-watermark false
  negative the licence exists to close: after two teardowns the original silent bound
  session could never be proven displaced. The disproof was propagated as a
  PROPERTY rather than as a site list, because four review rounds each found one more
  instance of the list and that made the enumeration itself the defect. **That property is
  now DISCHARGED**: the vanished-record item was the last candidate reader, decided against
  the field, and removed it, so every site that spoke to `RWID`'s RETENTION, its READERS or
  its REMOVAL has been rewritten to say the field is GONE and why. Res judicata, so the next
  reader does not re-run the sweep: sites describing the DELETED create inheritance, or
  F-036's own disproof, are historical and remain accurate untouched. *The
  gate-safety question this bullet used to pose is answered*: `docs/ARCHITECTURE.md`'s refusal
  to claim the shared client-ID gate for a bound session is about the rejection PROBE's absent
  verdict, not about the lock — the periodic pass's own ambiguous-write arm already claims
  it as the bound client — and SUBSCRIBE's and UNSUBSCRIBE's gates are both taken at entry,
  before any subscription effect or membership mutation, so a disposal that wins the gate
  costs a connection it was about to close anyway. *For site 2, only the PLACEMENT and TWO
  of the four outcomes are established; two are OPEN, and two successive review rounds
  each caught this bullet claiming more than that.* Settled: read the
  generation-retirement proof AFTER the release claim is acquired and BEFORE the onward
  fan-out, the point past which the store cannot be taken back; a watermark naming THIS
  generation refuses the fan-out, releases the hold nonterminally and answers PUBREL
  **0x92**, which is what the durable claim already answers when the discard is on another
  broker, and it is sound because a valid watermark naming the generation is POSITIVE
  evidence; and an UNREADABLE or indeterminate watermark **fails the release** and sends
  no PUBCOMP, because the surrounding PUBREL contract already suppresses PUBCOMP for an
  unresolved proof and the hold verification's own indeterminate arm fails rather than
  delivers. Those two refusals are wire-distinguishable — 0x92 with the hold released
  versus no PUBCOMP at all — which is what makes them separately testable. **OPEN 1: "no
  watermark" is NOT proof, and an earlier draft of this bullet wrongly said it was** on
  the grounds that nothing deletes a watermark. Per-record permanence did not survive
  stream recreation: `$MQTT_teardown` recreated ALONE used to stamp the current epoch,
  validate clean and **admit empty**, losing every open fence. So retire G1, recreate
  that stream alone, and a stale broker releasing G1's surviving hold reads "no
  watermark" and performs the very post-discard fan-out site 2 exists to prevent.
  **DISSOLVED by F-066, which LANDED 2026-09-01**: this arm's precondition was precisely
  that `$MQTT_teardown` recreated alone is admitted, and under the all-ten recreation
  unit that is a REFUSED state — an account holding nine of the ten fails the
  account-wide preflight before anything is provisioned. The arm therefore needs neither
  the partial-recreation residual nor some other durable proof, and the owed retirement →
  recreation → stale-PUBREL fixture is a startup-refusal fixture instead
  (`TestBrokerStreamBundleRefusesAPartialAccount`, which covers all ten positions).
  What remains reachable is administrative deletion or a partial restore, which the
  compatibility boundary does not support.
  **OPEN 2: the DIFFERENT-generation outcome, and the reason it is open is NOT
  that historical per-generation state is necessary** — a draft of this bullet claimed
  that too, and it is unproven. Proceeding on a bare generation mismatch is certainly
  wrong, because the slot is replaced by increasing `MarkerSeq` alone: retire G1, then
  create and retire G2, and the slot names G2 while a broker still serving G1 reads
  "different generation" and fans out. But the watermark also carries `MarkerSeq`, and this
  same attempt established `MarkerSeq > sess.seq` as positive proof that the record subject
  advanced — and a PUBREL is bound to `c.mqtt.sess`, with its hold subject derived from that
  incarnation. So the unexplored direction is a **two-dimensional** classification, the
  generation relation crossed with `MarkerSeq` against a fully revalidated `sess.seq`, and
  it must be proved or refuted before anyone concludes historical storage is required. The
  loss example a draft gave here — "a live generation whose client merely had a LATER
  generation retired" — is **backwards** for that classifier and is withdrawn. What the two
  settled outcomes DO catch is both populations this manifestation names, the
  never-enumerated hold and the one spared above F-062's cutoff, whenever the standing
  watermark still names the hold's own generation; and they promise NOTHING about a fence
  landing after the read, which is the pre-existing post-claim residual and stays. Cost: one
  watermark read per QoS 2 PUBREL release, on a path that already performs a claim CAS, an
  onward store and a delete. **The no-cache rule applies to the NEGATIVE outcomes only** —
  absent, unreadable/indeterminate, and different-generation — because each can be
  invalidated by a retirement landing afterwards, and there a finite window never arbitrates
  ownership. A draft of this bullet prohibited caching outright; that over-applied the
  principle. A VALID MATCHING-generation observation is positive and monotone: once a
  generation is retired it never becomes rightful again, single-slot replacement can erase
  discoverability but not the fact, and the CONNECT reader already treats any valid matching
  `Gen` as retired. So that result may be memoized per client and session generation, with
  eviction merely falling back to another read. *The
  destructive-decision census is complete*: ten decisions, six keyed by the SESSION
  generation (the hold discard, the hold purge and the release-claim delete in
  `reconcileOneInboundQoS2`; the absent-reference register delete; and both the absent and
  different-generation arms of the PUBREL consumer delete) and four keyed by the AFFINE
  generation (`discardAbsentWinnerClaim`, `deleteOrphanQ2AffConsumers`' step-2 `Discarded`
  transitions and its step-6 consumer delete, and `materializeQ2AffOnBehalf`'s combined
  absent-or-different-generation arm) — together with the decisions deliberately EXCLUDED
  and the positive fact discharging each (`settleRetiredAffineRegister` and the
  live-reference reaps act on source state, a pair proven by source, a terminal claim, or
  a positively identified superseded copy). **What BLOCKS the affine half, and it is
  structural.** The retirement watermark records only the SESSION generation:
  `mqttTeardownTarget` carries both `gen` and `q2AffGen` but `recordGenRetirement` writes
  `Gen: t.gen` alone, and `refs.hasInbound(idHash, ps.Gen)` versus `refs.hasAffine(idHash,
  ps.Q2AffGen)` are genuinely different keys. So no retirement proof can license the four
  affine decisions, and **recording `Q2AffGen` in the watermark is refused**: it is a new
  persisted field, and a latest-only field would prove only the currently recorded affine
  retirement anyway.
  **Three designs REFUTED during the attempt, so they are not tried again.** *"A retired
  affine generation's claims are already terminal, so the absent arms are only reachable
  where nothing proves death"* — false: teardown's `discardQ2AffClaimsFor` is a stream scan
  plus per-claim CAS and installs NO fence against future claim creation, so a broker still
  serving the affine generation can create a fresh nonterminal claim afterwards through the
  ordinary create-only path. *Keeping `materializeQ2AffOnBehalf`'s arm as a named exception
  to the invariant* — rejected: absence plus possession of the payload still does not prove
  the session ended, writing `Discarded` can withdraw an already-materialized copy from a
  previous owner still serving, and an invariant with an exception is not one. Its correct
  disposition is to RETAIN — write nothing, ack nothing, return false so `moveStartedSharedQoS2`
  leaves the move deferred and AckWait re-offers it — which is worse only in AVAILABILITY,
  and whose residual is an **unbounded redelivery stall** that can consume the group's
  `MaxAckPending`; `MaxDeliver` cannot bound it, because the shared-group validator REQUIRES
  the server-normalized `-1`. Acking without a terminal fact is refused too: it would regress
  F-063's closed `sourceAckAuthorized` guarantee. *"Step 6's consumer delete falls out of
  step 2, so one gate suffices"* — false, and this is what stopped the attempt:
  `mqttQ2AffGenerationSettled` returns true for an EMPTY inventory and for a generation whose
  claims are all `Discarded` or `Lost`, so with no transition written it still succeeds
  vacuously and the consumer is deleted with no positive fact — in three cases, including
  a nonterminal claim created after the verification scan.
  **SECOND ATTEMPT, 2026-09-01: the ordered pre-plan investigation RAN and the item
  STOPPED AGAIN**, after five consults and four plan-review rounds, with **no
  implementation code written**. F-066's enforcement landed (`1205d32`), so OPEN 1 is
  dissolved and the prerequisite was met. **Gate-relevant outcome only; the dossier is
  canonical for the mechanism, the evidence and the refuted designs.**

  - **Q2 is SETTLED and has LANDED 2026-09-01**: course E — delete
    `deleteOrphanQ2AffConsumers` outright — is implemented, with the transitively dead
    `mqttQ2AffGenerationSettled`, `mqttQuarantineConsumerKey`, `mqttParseQ2AffDurable` and
    `mqttSessRefs.affDurables` removed with it. Its accepted price is **larger than this
    bullet first recorded**, and implementation measured it: not a consumer leak but an
    unbounded CORRELATED leak of the consumer, its interest-retained copies, and the claims
    whose copies remain present — `reapable` requires copy absence as well as `SourceAcked`,
    and the removed step 6 is what used to supply it. Plus a class the plan review added:
    every authorized cleanup route that FAILS after dropping its reference has no retry
    anchor. Stated in `docs/CONFORMANCE.md`. **This does not close the box** — see below.
  - **Q1 is CONDITIONALLY PROVED**: the sequence classifier holds, so **historical
    per-generation watermark state is NOT required**; its identity prerequisite is not
    proved, and that is what blocks planning.
  - **This item is now release-blocking on SINGLE-BROKER grounds.** The attempt found a
    third manifestation that is **silent loss of an acknowledged QoS 2 message with no
    cluster, no crash and no peer** — an ordinary clean-start reconnect, through this
    broker's own reused session object. It takes **no new finding ID and no second
    checkbox**, because it is this finding's own causal defect and a second checkbox would
    duplicate completion semantics and invite an accidental split. It stays release-blocking
    even if multi-frontend HA scope were ever reconsidered.
  - **Scope wording:** read "ONE executable item" below as **one completion item**, not
  one
    atomic commit. The assembled work is two units — the incarnation admission invariant,
    then the classifier with sites 1 and 3 — and the first is the
    second's foundation with nothing thrown away between them. Course E was grouped with
    the second unit and a 2026-09-01 consult established that the grouping was **not** a
    code dependency: the sweep runs outside any client packet route or incarnation
    admission, and deleting its consumer route leaves nothing awaiting the invariant's
    authorization. It therefore landed on its own, ahead of both units.
  - §8's comment-corpus sweep gains a sixth family from this attempt.

  Working lane `~/.claude/lanes/f-036-preplan/` holds four plan revisions, the ledger with
  every round classified, all five consults and all four plan rounds verbatim; every durable
  conclusion is filed in the dossier rather than left there.

  **The order below is HISTORICAL and SUPERSEDED by the 2026-09-01 attempt above.** Both
  of its pre-plan questions are now answered — the classifier is proved conditionally and
  (a) is decided as course E, which has since LANDED — so a third attempt must NOT re-run
  them and must not choose
  a positive authorization for the affine consumer delete: every one was refused, the route
  is now GONE rather than guarded, and the
  "independently terminal" definition it calls owed is DISCHARGED by the existing
  `mqttQ2AffClaim.terminal()`. **The current gate is: course E has landed, and planning for
  everything else is blocked on the identity/lifecycle property and its route audit.** The
  text is kept because (b) through (e) are unchanged and still owed.
  *Superseded — the order this was attempted in, ruled 2026-08-31.* F-066's enforcement
  lands first, which dissolves OPEN 1. Then a BOUNDED pre-plan investigation settles exactly
two   things before any plan is written, because both are what stopped the last attempt
  rather than anything a plan could absorb: prove or refute OPEN 2's two-dimensional
  classifier, and decide (a) below — authorize the step-6 affine consumer delete
  independently of step 2, or remove its destructive arm. Site 1 and the test
  infrastructure (b) through (e) are then carried into ONE implementation. This is
  sequencing, **not** a split: the scope ruling that this is one executable item stands,
  and a bounded investigation that closes neither question leaves the box open rather
  than licensing a partial mechanism.
  **What is OWED before this is attempted again**, and it is mechanism plus test
  infrastructure rather than another classifier revision. *(a) is DISCHARGED by the
  2026-09-01 attempt — course E removes the destructive arm outright and
  `mqttQ2AffClaim.terminal()` supplies the definition — and is struck: a positive
  authorization for the step-6 affine consumer delete INDEPENDENT of step 2 was sought and
  every candidate refused.* (b) a valid abnormal-exit construction for the
  bound disposal — deferred WRAPPERS reading mutable variables, as `mqttProcessConnect`
  already does, because a `defer` of a variable captures its value at the `defer`
  statement — together with a take-and-clear before invocation rather than after, since
  clearing after the Will action cannot prevent a second application when it panics; (c) a
  **pre-disposal** identity-change seam, because `applied == false` is decided inside the
  disposal before it returns and no post-disposal hook can reach it; (d) a controllable
  between-read seam, so removing the in-fence proof re-run is mutation-provable; and (e)
  gate- and phase-reach assertions, without which removing the off-gate absence check, the
  separately listed in-fence checks, the gate-held precondition on the Will, and the defer
  ORDER are all masked by checks that refuse anyway. The lane is `~/.claude/lanes/f-036/`:
  it holds plan revision 3, the review ledger with every round's findings classified new/repeat/
  caused-by-last-fix, both consults, and all three round results verbatim. Revisions 1 and
  2 were overwritten in place and survive only in the round prompts and results beside them,
  which is why every durable conclusion is filed HERE rather than left there.
  **Scope was confirmed to be ONE executable item** throughout, so this is not a candidate
  for splitting. Two earlier manifestations of this finding remain as stated: the
  post-enumeration serving direction and the sweep's false-absence inference, both widened
  into it by F-032's audit, plus F-033's affine-consumer adoption blind spot and F-034's
  live-owner register contention.

  - [x] Make ephemeral-consumer cleanup guaranteed rather than
  best-effort. **DONE.** Both per-broker ephemeral consumers now derive their name AND
  delivery subject from a per-`Server`-run identity, so a retried initialization asks for
  the same consumer instead of stranding one under a name nothing can compute again —
  which was the real defect: a stray whose rollback delete was lost had a name that
  existed only in the failed attempt's stack. Consumer metadata carries owner, purpose,
  run and schema. See "Per-run ephemeral consumer identity" in ARCHITECTURE.md.

  **This bullet's own premise was imprecise, and the correction matters more than the
  fix.** "Every MQTT connection retries the whole initialization" is inherited from
  upstream's multi-account world; here there is one account and `Start` initializes
  eagerly. But `startMQTT` opens the listener BEFORE that eager call, so a CONNECT
  arriving in the startup window drives initialization itself and a failed attempt IS
  retried within one run — until one succeeds or `Start`'s own failure tears the server
  down. The retry population is real and is a production one; four code comments stating
  it without that scope are corrected (§8's comment-corpus sweep gains them).

  **Two things the prescription did not anticipate, both measured against the pinned
  server.** A repeated named create ADOPTS the existing consumer at its current position,
  so a stable name alone would have left a retry's retained INDEX empty — invisible to
  every wildcard subscription, for the life of the process; the observer therefore proves
  its name absent (a definite delete, one re-attempt, then fail closed) before creating,
  while the watch safely adopts its own. And no error distinguishes "our create landed"
  from "a foreign consumer is at that name", because both server paths fail before
  looking the name up — so a rollback delete is licensed by a CONFIRMED create and
  nothing else, which means an ambiguous create is left for the next attempt or for
  `InactiveThreshold` rather than deleted.

  **Residuals**, all five stated in CONFORMANCE.md: the guarantee is conditional on a
  same-run retry (at most one consumer per run/account/purpose); the rollback is
  best-effort and outside it; `InactiveThreshold` still governs a crash, a replacement, a
  lost rollback delete, an unreconciled ambiguous create and the normal shutdown of a
  successful broker; across rapid restarts cardinality is bounded by restart rate ×
  inactivity window; and a nuid name collision is stated rather than mechanised. See the
  [platform
  audit](findings/nats-platform.md#stable-ephemeral-consumer-identities-and-metadata-—-done).

  - [x] Give a vanished record its own disposal vocabulary. **CLOSED AS REFUTED
  2026-08-29, and `RWID` was REMOVED.** The vocabulary cannot be built, and the negative
  result plus the field's removal is what landed. **Why it cannot be built:** the item's
  own predicate was "positive evidence that THIS record was retired, which is what `RWID`
  states", and `RWID` does not state that. A write identity names a record's BYTES, not one
  OCCURRENCE of them, and two live routes put one identity on two occurrences —
  `storeFramedSessionMsg` replays acknowledged bytes verbatim and the readback adopts the
  higher sequence WITH the original `WID` (`sess_resume_reconcile.go`), and restore adopts
  `WID`/`PWID` from record JSON unvalidated. Worked: a session is acknowledged for
  `{seq:10, WID:X}`; bytes carrying `WID:X` land again at 20; a teardown claims occurrence
  20, marks at 30 with `PWID:X`, records `{RWID:X, MarkerSeq:30}` and deletes. Against the
  stale session `RWID == sess.wid` holds AND `MarkerSeq > sess.seq` (30 > 10), yet neither
  teardown removed occurrence 10. `MarkerSeq` bounds SOME claimed occurrence below the
  marker and never says WHICH, so **no conjunction of surviving durable fields is
  occurrence-exact**. A design keyed on historical evidence alone was refuted separately:
  with record `W` retired and a successor `Z` standing, the watermark still names `W`, so
  the report would blame a retirement for a disposal `Z` caused. **What an exact reader
  would need instead:** the claimed PREDECESSOR SEQUENCE, stored on the marker where
  `claimTeardown` knows it and copied into the watermark, compared for equality with
  `sess.seq` — two new REQUIRED fields on two PERMANENT shapes, because
  `finishAdoptedTeardown` holds only the marker's own sequence. **Refused as
  DISPROPORTIONATE, not as blocked**, and the distinction is deliberate: the route is
  analyzable, since `$MQTT_teardown` is epoch-coupled to `$MQTT_sess` and every
  broker-owned stream is recreated as one unit (F-066), so it need not open fresh F-066
  surface. What refused it
  is the price — two permanent record fields to specialise a DISCONNECT Reason String and
  a log line. **What this leaves, as a permanent stated residual in `docs/CONFORMANCE.md`:**
  a vanished record, an unreadable one, an undecodable one, a below-base one and a
  valid-shaped stranger all report one undifferentiated `0x89` "ownership could not be
  established", so an operator cannot tell an ordinary retirement from a store fault from
  that line. No client-visible behaviour changed FOR CURRENTLY VALID SUPPORTED STATE — the
  qualifier is load-bearing, see the format-gate disclosure below. **`RWID`'s removal**,
  which this box
  owned: the field had no semantic reader and could not acquire one, and a future exact
  reader needs `RetiredSeq` rather than a restored `RWID`, so the "expensive to re-add"
  default no longer supported keeping it. **Two errors in the removal set this bullet used
  to carry, both caught by walking the tree:** the `retiredWID` parameter of
  `recordGenRetirement` was NOT dead — it carried the precondition that a record with no
  identity fails its teardown CLOSED, which nothing else in the teardown path checks, so it
  survives as an explicit guard; and deleting "its threading from BOTH callers" would have
  removed `clear()`'s `retiringWID` capture, which also parents the teardown MARKER. **A
  format gate went with the field and is disclosed rather than claimed away:** a watermark
  with a valid `id`/`gen`/`mseq` but no `rwid` was CORRUPT and is now VALID, which changes
  four readers — the CONNECT discard, the sweep's condemnation, the record-absent create's
  refusal, and the writer — but creates no new LOGICAL false positive, because the
  validator only ever checked non-emptiness so an arbitrary string already passed. 12 plan
  rounds, 2 consults.

### 0.3 Confirmed alpha blockers

These are concrete correctness, data-loss, race, or liveness defects in features
the README currently reports as supported. They remain blockers independently of
the HA-scope decision except where the dossier explicitly requires a remote
takeover.

**Filing rule (2026-10-12 convergence triage).** A pre-existing finding met by another
item's work does NOT open a new box in §0.3 or §0.4; every such box sends document-order
selection backwards. It goes to its destination instead: an accepted behavioral gap to a
stated residual in `docs/CONFORMANCE.md`, a verification observation to the
[verification ledger](findings/release-investigations.md#verification-ledger), an audit to
§2, with a one-line note in the item that met it. The exceptions are Will or message LOSS
on a common path, data corruption, and security, which get a §0.3 box. A finding whose
dangerous mechanism is still unexcluded stays attached to its existing investigation until
it is classified: "pre-existing" establishes provenance, not safety. Accepting a residual
never licenses changing a test assertion.

- [x] [F-079: A denied publish is accepted, stored and
  delivered](findings/runtime-correctness.md#f-079). **Alpha blocker — done.** The broker
  never learned the authenticated identity's NATS permissions at all, so
  `pubAllowedFullCheck` returned `true` by design and every effect that did not ride
  `c.nrc` proceeded on the full-rights service connection: the `$MQTT_msgs` store, the
  retained store, the QoS 2 hold, and internally replayed Wills with their live fan-out.
  It now asks, with **one `$SYS.REQ.USER.INFO` request on the client's own connection at
  CONNECT**, and enforces the answer itself. **Stated as a property, not a route list**,
  because the dossier's history is that each round produced another route: *before the
  first effect, authorize the snapshot against EVERY effective application subject the
  publication will create*. That set is not always a singleton — a Sparkplug birth also
  derives, publishes and retains a `$sparkplug/certificates/...` subject on the service
  JSA, and a Will whose topic is a birth topic is one publication with two effective
  subjects, a composition the first two plan revisions both missed. Gates: QoS 0/1 in
  `mqttProcessPub`; QoS 2 **inside `mqttStoreQoS2MsgOnce`**, because an occupied packet
  identifier must be answered from the existing hold whatever subject it names
  ([MQTT-4.3.3] transfers ownership at PUBREC) — which costs one bounded JetStream read
  per denied QoS 2 PUBLISH, stated in `docs/CONFORMANCE.md`; and every Will site reads a
  decision **captured at CONNECT**, because all four run inside `mqttHandleClosedClient`
  where the snapshot is already invalid and asking would have suppressed every
  AUTHORIZED Will on any NATS blip. **No persisted shape changed**: a `$MQTT_willq`
  record IS the evidence, because a denied Will is never enqueued. The observable: MQTT 5
  gets PUBACK/PUBREC `0x87`, MQTT 3.1.1 gets the connection closed ([3.3.5.2] permits
  either, and a success-shaped ack is the defect restated) — a deliberate divergence from
  the reserved-`$MQTT.>` refusal beside it, documented rather than harmonised.
  **Two things this fixed that were not in the finding.** A pre-existing bug:
  `mqttHandlePubRetain` read the SHARED parser object `c.mqtt.pp` instead of the
  publication being delivered, so a retained QoS 2 release stored whatever the client
  sent last — and with a denied Sparkplug birth in between, manufactured its derived
  effect. And a leak this work introduced and its own fixture caught: the identity reply
  used nats.go's `_INBOX`, which an MQTT `#` subscription matches, so one client received
  other clients' identities and permission lists as ordinary messages; the reply now
  lands under the reserved namespace on the service connection.
  **Constraint:** service-owned publication and persistence paths authorize against the
  permission snapshot taken at CONNECT; an ACL change on a still-live connection may
  remain unapplied there indefinitely until MQTT reconnect, while NATS-owned paths apply
  it immediately.
  **Security prerequisite, and it is UNCHECKABLE:** an MQTT identity must be denied
  publish on `$MQTT.>`. The identity reply is an unsigned account subject and a request
  to `$SYS.REQ.USER.INFO` is observable to an account subscriber (measured), so a
  principal able to publish there can race the server and answer for any client — a
  fabrication claiming "no permissions configured" reads as unrestricted. **No test of
  the reply can detect this**, because the forger controls what the reply claims; a
  self-referential check was tried during review and refuted (a reply denying only the
  one inbox satisfies it while granting everything else), and was removed rather than
  weakened. The deny is the same one the reserved namespace has always required, for the
  stronger reason that such a principal can already forge session records directly — so
  an account whose MQTT users are unrestricted has no isolation to protect either way.

- [x] [F-080: The authenticated identity's NATS account is never
  bound](findings/runtime-correctness.md#f-080). **Alpha blocker — done.** The same
  request answers with the identity's account, and `isClientAuthorized` now requires it
  to equal the service connection's own account — fetched at `Start` **before the
  listener opens**, so no connection is ever accepted that the broker could not judge.
  Everything unknown refuses: a timeout, an undecodable body, an empty user, a
  permissions object present but missing a direction, a permission entry that is not a
  valid subject filter, an elapsed expiry. An **absent** permissions object is the shape
  a server with no ACLs emits and means unrestricted — the opposite reading would refuse
  every publish on the default deployment. **The snapshot's validity is the connection's
  validity**: the per-client connection re-authenticates on every reconnect, so a
  disconnect invalidates the snapshot and then closes the MQTT socket. Invalidation is
  the primitive and the close only its consequence, because `readLoop` tears down after
  the current parse returns and a PUBLISH already in hand would otherwise still use it. A
  service-connection disconnect invalidates the service epoch too, since nats.go flushes
  its reconnect buffer **before** the reconnect callback runs — a callback-only check
  would discover an account change after the effects had already landed in it. Costs, all
  stated in `docs/CONFORMANCE.md`: an MQTT connection no longer survives a transient NATS
  outage, and the refusal is a bare socket close because `authViolation` sends no CONNACK
  (that is F-061's, untouched here). **The end-to-end cross-account fixture does not
  prove the comparison** — such a client cannot even be identified, so it is refused by
  timeout, and a mutation run showed the fixture passing with the comparison deleted;
  a seam-driven test isolates it.

- [x] [F-081: A subscribe deny overlapped by a wildcard is not enforced on
  delivery](findings/runtime-correctness.md#f-081). **Alpha blocker — done.** `mperms`
  had no writer and `checkDenySub` was a stub returning `false`, so both halves of the
  inherited per-message guard were dead. They are now fed from the snapshot's subscribe
  permissions, **deny side only** — NATS already applied the allow side when it accepted
  the subscription — and `checkDenySub` regained upstream's **two-argument, queue-aware**
  shape, because `USER.INFO` serialises a queue-scoped deny as `"subject queue"` and a
  queue-blind check silently downgrades it. **Three guards, not the dossier's two.** The
  affine shared QoS 2 route (`qos2_shared_affine.go`) reaches the client through the
  service connection with **no guard at all** — a route the dossier does not name — and it
  cannot use the funnel or `sub.queue`, because its subscription is created with a **nil**
  queue and by delivery time no longer names the group the copy came from. Its guard
  derives the queue from `claim.GroupDurable` instead (the durable's `<qkey>_<inc>` tail
  is parsed and the LOGICAL queue derived from `<qkey>` alone). **The verdict is
  tri-state, and that is load-bearing**: an indeterminate verdict — no valid
  snapshot, or an underivable group — is withheld WITHOUT acknowledging, because
  it is a fact about us and not about the message. Collapsing it into a boolean
  satisfies every non-delivery assertion by destroying the message instead of
  withholding it. A **proven deny** is disposed of per route, and the predicate is
  whether the copy is still the group's to offer: a plain subscription acks it
  terminally, since no other consumer will ever be offered it; the ordinary shared
  group consumer hands it back on the paced refusal backoff, since it was selected
  without its ACL and an allowed member may still take it - paced and not fixed
  because a sole denied member has nobody to hand it to, and a fixed delay against
  `MaxDeliver` of -1 is a retry loop rather than a retry; the affine shared QoS 2
  copy records `AffineDone` and acks it terminally, since the assignment already
  moved the exchange to one session and no other member can. Stating any one of
  those three as the universal rule licenses either the shared data loss or an
  unbounded redelivery loop. Plain QoS 0 on a user topic keeps **no** broker-side
  guard: it rides `c.nrc`, NATS filters it, and leaving it unguarded is what makes
  it a control that tells the two mechanisms apart — a mutation removing the
  broker's guard must leave it passing.
  **The affine arm IS proven, and getting there took three wrong fixtures.** It is
  reachable precisely because this finding is about a filter that OVERLAPS a narrower
  deny: `bob` is denied `aff.topic` but `$share/g/aff/#` is
  admissible, so he can resume a session `alice` created — the principal is account plus
  client ID — and the copy already moved to that session's affine consumer is redelivered
  under his stricter ACL. A fixture using the *exact* denied subject instead fails at
  subscription restore, upstream of the guard, and asserts a non-delivery with another
  cause; that version passed while both guard mutations survived it. The committed
  fixture has four arms — an allowed resume RECEIVES the copy, a subject-denied one does
  not, a QUEUE-SCOPED denied one does not, and a later allowed resume finds nothing,
  which is what attributes the deny to this guard rather than to mere withholding, since
  its deny arm is terminal. Removing the guard kills three of them; taking the queue from
  `sub.queue` instead of `claim.GroupDurable` kills the queue-scoped one.
  **Constraint:** service-owned QoS 1/2, retained, and affine deliveries enforce
  subscribe denies from the permission snapshot taken at CONNECT; a live ACL change may
  remain unapplied there indefinitely until MQTT reconnect, while the client's
  NATS-owned subscription paths apply it immediately.

- [x] [F-051: Bound a connection that never sends CONNECT](findings/resolved.md#f-051).
  A CONNECT deadline (`internal/mqtt/connect_deadline.go`) bounds every accepted
  connection between accept and CONNECT, closing the MQTT 3.1.1 / 5.0 [3.1.4]
  SHOULD the broker did not implement **for a connection that stays silent** — with the
  late-admission case, where a delayed callback lets a CONNECT past the window and then
  finds it admitted, stated as an unenforced half in `docs/CONFORMANCE.md`: the timer the
  ported code armed was
  unreachable, because neither `Info.AuthRequired` nor `srvMQTT.authOverride` has
  a writer in this repository. **Deliberately wider than the finding's wording:**
  the bound is unconditional, not gated on a configured client dialer, because
  the resources a silent connection holds are identical with
  `-auth-passthrough=false` and gating would have invented a conformance residual
  for nothing. The arbitration between the deadline callback and the CONNECT it
  races is a state under `c.mu` (`Timer.Stop()` reports that the timer fired, not
  whether its callback has decided); the loser does nothing, and a CONNECT that
  lost is refused **before** `isClientAuthorized`, so it dials no per-client NATS
  connection. `authTimer` became `connectTimer` and `setAuthTimer`/`clearAuthTimer`
  were deleted; `authViolation` is untouched, being the CONNECT-authorization
  failure path and a different fact. `-connect-timeout` /
  `MQTTNB_CONNECT_TIMEOUT` (default 5s) is the operator surface, refused at `<= 0`
  because the bound is fail-closed. `TestMQTTAuthTimeout` was tightened: with the
  timer never armed its negative case read a success CONNACK and passed.

- [x] [F-043: Fix the `removeSessFromFlappers` data race](findings/resolved.md#f-043).
  The parked deferred close in `sess_uncertain.go` held nothing while deleting from
  `asm.flappers` — the one access to that map in the tree with no lock, against a CONNECT
  that reads it under `asm.mu` and a cleanup timer that sweeps it under `asm.mu`, so the
  exposure was the runtime's own concurrent-map abort and not merely a `-race` report. A
  census of every runtime `flappers` / `flapTimer` access establishes it as the only
  unguarded one; the helpers were renamed to the `…Locked` suffix this package already uses
  so a future call site's omission is visible at the call. **Wider than "add the lock":**
  the removal is now conditional on the close still OWNING the session. Under the gate neither
  producer can run (`evictDecideOnce` refuses, `discardTakenOverClient` is downstream of a
  CONNECT needing the same gate), so the entry is provably stale and is removed as before;
  on the gate-less route nothing can be proven — the holder it lost to can release between
  `lockSessionForTeardown`'s last attempt and the park, letting `evictDecideOnce` jail this
  client ID in the interval — so that route leaves the jail alone rather than cancelling a
  live displacement's. The two cases are now separate entry points
  (`mqttDeferDisconnectOwned` / `mqttDeferDisconnectUngated`) so the ownership precondition
  is stated at the call rather than carried by a boolean named for something else. Cost: up
  to one `mqttSessJailDur` of reconnect enforcement on an entry that
  may have been stale, which is the fail-closed direction and replaces the residual the
  unconditional fix would have had to state. **Reproduced before it was fixed, at both lock
  states**, as the finding asked: three call sites reach `mqttDeferDisconnectEx`, but the
  never-expiring one parks after ending its record transaction with the gate held and is
  lock-state-identical to the plain gated one, so the two untagged `-race` tests separate
  gate-held from gate-NOT-held and each baseline reproduction names its own route in the
  stack. `make full-suite` runs without `-race`, so the tests are deliberately in the default
  tree. Lock ordering verified rather than assumed: the removal holds no `sess.mu` (the park
  released it) and no record fence on any route, so it adds no edge to the established
  `as.mu` → `sess.mu` order, and the section performs one map delete with no outward wait.

- [x] [F-087: Rebuild every in-flight register rewrite from the authoritative
  reread](findings/ha-arbitration.md#f-087-rebuild-every-in-flight-register-rewrite-from-the-authoritative-reread).
  **DONE 2026-08-30.** One rule over the frozen five-writer set: a retargeted CAS starts
  from the record it just read and mutates only the fields the operation owns — monotonic
  facts merge forward, identity facts are replaced only as a set by the same-delivery
  takeover, and `Claim`/`ClaimSeq` belong to the contest alone; a body proposed at the
  caller's OWN cached sequence stays sparse, because that CAS is itself the proof. That
  makes `Retired ∧ Cancelled` durably reachable for the first time, so **`Retired`
  dominates `Cancelled`** at all four readers (recovery's classification, the contest's
  close arm, the store's tombstone reap, the affine sweep's settle gate): a record
  carrying both reinstates its suppression and frees nothing. The post-contest routing now
  judges every OWNED result by the body the contest RETURNED, so a newly retired record
  leaves the live path instead of being installed as deliverable — the blocking instance.
  Sequence application is LINEAGE-aware (the operation's traversed chain, not its base).
  The same-delivery takeover gained its four converging dispositions: adopt a RETIRED
  occupant behind a generation fence, reap a pure CANCELLED one and retry (ordered above
  the takeover), replace one that is ours, and QUARANTINE a live foreign or unclaimed one
  in place — held, never abandoned, because abandoning mints a second identifier for one
  message. Every register delete whose caller can name the exchange nonce routes through
  one verdict-reaping helper — where it can also REACH the verdict set, which lives on a
  session; the orphan sweep with none resident is the stated exception (F-090, §P) — guarded
  by an AST walk so a new delete site cannot rejoin the stranding class. Every
  JETSTREAM-BACKED QoS 1/2
  PUBLISH transmission runs inside a SEND SECTION whose
  registry holds the identifier until the write returns, across a `clear()` and across a
  successful PUBREC's entry replacement; removal paths condemn and park rather than free.
  The PUBREL transmission is deliberately NOT covered (F-089 below, filed by this item's
  holistic review, which also retracts the false argument that once excluded it), and
  neither is a RETAINED QoS 1/2 send — F-091, which closed it 2026-08-31 with a reservation
  in this same registry rather than a section, fenced by the session incarnation.
  **Residual, documented in `docs/CONFORMANCE.md`:** the quarantine has no live release —
  both settled shapes the register vocabulary offers (an observed absence, a cancellation
  tombstone) are consumable by the rival's own supported lifecycle, so a live release
  could free an identifier whose durable custody it cannot settle. It is removed only by
  the next SUCCESSFUL resume's reconciliation (STAGED by the walk, committed past every
  refusal return) or by generation retirement, at the cost of one identifier and one
  Receive Maximum slot for the rest of that connection. **The live release is filed
  below** and any future design for it must prove exclusion against every supported writer
  and recovery lifecycle, not against one racing write. Thirteen plan-review rounds, three
  escalation consults and one user reopening are recorded in `~/.claude/lanes/f-087/`;
  22 mutations were applied and each killed its paired test.

  **F-105 dependence (audited 2026-10-05; corrected 2026-10-06 by WP-F W3):** the property
  this bullet closes is now enforced by F-105 code, so the closure is conditional on
  F-105's integrated gate. What replaced the mechanism its passages describe: for the
  ordinary class, and shared QoS 1 (ordinary since 7a), F-105 7b deleted the claim contest,
  and it deleted the foreign-claim quarantine for every class. Ownership is the connection's
  accepted entitlement, compared inside each owner-dependent authority batch, and an
  occupant naming another delivery is abandoned fail-closed (`mqttSlotDispAbandon`), never
  quarantined by claim; what survives for the experimental slot-only class (chapter P) is
  the claim comparison on slot acquisition (`mqttSlotAcquisitionBaseHolds`).
  Mechanism: `docs/ARCHITECTURE.md`, "Resume restores from the inventory, and ownership is
  the entitlement's (F-105 7b)". The quarantined-identifier follow-up was closed by census
  on 2026-10-11 (its own bullet below). See F-105's "Checked bullets
  resting on F-105".

- [x] [F-089: A PUBREL transmission must hold its packet identifier until its
  PUBCOMP](findings/ha-arbitration.md#f-089-a-pubrel-transmission-must-hold-its-packet-identifier-until-the-write-returns).
  **Alpha blocker, unconditional — done 2026-08-30**, after five plan-review rounds.
  Filed by F-087's holistic review (round 3); scope settled by that lane's `DECISIONS.md`
  D6. `mqttDeliverPubRelCb` called `trackAsPubRel`, released `sess.mu` and performed a
  deadline-free socket write with nothing reserving the identifier, while
  `mqttProcessPubComp`'s `untrackPubRel` freed it with no send-section check.
  **The Acceptance shape in the dossier was REFUTED and must not be re-derived.** It
  asked for F-087's PUBLISH shape verbatim — a write-return section plus a PUBCOMP park.
  That closes the primary race and leaves the SECOND-ORDER one open, which is the one
  the dossier calls worse: `endSend` frees the identifier the moment the write returns,
  and the duplicate's own later PUBCOMP then acknowledges away a re-minted exchange's
  PUBREL record. At the F-089 landing, the park was also machinery the PUBREL side did not
  need: the register was then deleted at PUBREC and authority moved to the PUBREL record.
  F-092 later replaced that handoff with the whole-exchange owner slot documented below.
  **What landed instead:** one hold on F-087's existing registry with TWO release
  conditions — the write has returned, AND every PUBREL transmitted under that identifier
  has been PUBCOMPed, which is the event [MQTT-2.3.1-4] actually names. The two differ
  across a connection change, so the hold carries both.
  **The acknowledgement debt is OWNED by a connection and read through `sess.c`**, not
  dropped at named transition sites. Review round 1 proved the sites cannot be enumerated
  safely: `clear()` never assigns `sess.c` at all, a persistent-session disconnect nils
  it and never calls `clear()`, and the clean-start path runs `clear()` while the
  displaced client is still bound — so dropping the debt there leaks forever on a
  never-expiring session and frees an identifier the client still holds on a clean start.
  A retransmission RESETS a dead predecessor's count rather than adding to it. The
  ownership identity is the connection's `cid`, a monotonic never-reused scalar rather
  than a client pointer: an inert hold outlives its connection by design, so a pointer
  would pin the departed client's entire object graph to a resident session; a ZERO
  identity is refused rather than recorded, so no hold can carry one and no ownership
  comparison can be satisfied by two defaults. Found by
  the holistic review, which also strengthened both AST guards (pairing counted ACROSS
  classes, allow-list entries qualified by receiver type) and replaced a synchronous
  fixture with one that blocks a real transmission through `mqttDeliverPubRelCb` at a
  new in-section seam.
  **PRESENCE IN THE REGISTRY IS NOT A RESERVATION**, and round 2 found the tombstone that
  makes this a rule rather than a habit: `beginSendLocked` refused on presence, so an
  inert hold `bumpPI` was willing to hand out stranded the identifier, its register and a
  Receive Maximum slot for the session's life. An inert hold is TAKEN OVER, every reader
  goes through `sendReservedLocked`, and an AST ownership-boundary guard stops a new
  reader appearing outside that API. `clear()`'s sweep is hygiene and nothing rests on it.
  The structural guard is rebuilt on the AST over every non-test file in the package: it
  selects calls, resolves a simple local alias of the packet-type constant, and requires
  the deferred `endSend` to LEXICALLY PRECEDE each enqueue, paired one-to-one.
  **Residual, in `docs/CONFORMANCE.md`:** a PUBREL whose enqueue is suppressed for
  exceeding the client's Maximum Packet Size (unreachable for a connected client) owes a
  PUBCOMP that never comes, holding one identifier for the rest of that connection.
  F-091 was separate and is now DONE — a retained send allocates through a different site
  and uses neither enqueue helper, so it took a reservation in the registry instead of a
  section; see its bullet below.

- [x] **F-091: a retained QoS 1/2 send does not reserve its packet identifier.** Filed
  2026-08-30 by F-087's holistic review (round 7). **Done 2026-08-31**, after six
  plan-review rounds; lane `~/.claude/lanes/f-091/`. Not an alpha blocker on its own
  reading, but the same RB2/RB6 class as F-089 and taken with it. *The finding:* a retained
  QoS 1/2 message allocates in `serializeRetainedMsgsForSub`, frames itself, and is buffered
  on `sub.mqtt.prm` to be written by a LATER `queueOutbound` — with no send section
  anywhere. Across that span its only reservation was the `pendingPublish` entry, and
  `clear()` destroys exactly that while resetting `last_pi`, so the identifier was handed
  straight back out while the displaced connection's write was still in the socket. Class
  G of F-087 excluded retained sends because they carry no register: true, and beside the
  point — the IDENTIFIER is still allocated.
  **The acceptance's second branch is REFUTED, and the argument is recorded so it is not
  re-derived.** "A retained identifier cannot collide across a discard" fails three ways:
  the frame's own identifier cannot be acknowledged before its write returns, but `clear()`
  supplies the free without any acknowledgement; the sockets are NOT different during the
  exploitable interval, because `clear()` does not rebind `sess.c` and a delivery to the
  still-bound connection's own live subscription passes `beginSendLocked` and writes to that
  same socket; and a buffered frame IS still written afterwards, since the SUBACK tail's
  flush is unconditional and consults no session state.
  **What landed:** the reservation a retained send lacked is the one `sendsInFlight`
  already
  provides — the registry F-087 built to survive `clear()`, which `bumpPI` already consults.
  `mqttSub.prm` became `[]mqttRetainedFrame` carrying each frame's identifier;
  `mqttSendHold` gained `retainedGen`; and the reservation is taken in the SAME `sess.mu`
  acquisition as the allocation, under `asm.sessLocked[sess.id]` so no successor can rebind
  between the two.
  **Two blocking findings shaped it, and neither was the retained path itself.** Round 1:
  a third term in a shared registry obliges every mutator, and `beginPubRelSendLocked` (which
  refuses only on a live token) and `consumePubRelAckLocked` (which re-spelled the liveness
  condition) were both blind to it — reachable because `mqttProcessPubComp` fire-and-forget
  acknowledges its PUBREL record, so a lost ack leaves that record AckWait-eligible while
  a retained allocation may take the identifier. Round 3: releasing on the write's RETURN
  reopens the defect past a `clear()`, because the entry that would otherwise carry custody
  on is gone — so written completion is fenced by the session incarnation (`sess.gen`, which
  `clear()` re-mints and an acknowledgement does not), while ABANDONMENT releases
  unconditionally.
  **A subscription leaving `c.subs` must not take an owed frame with it**, over a frozen
  three-site set — `processUnsub`'s delete, `processSub`'s same-sid replacement and
  `closeConnection`'s swap, the last for totality since `sess.c` is nilled first — but the
  two dispositions DIFFER, and holistic round 4 is why. The teardown sites ABANDON, releasing
  the reservation, because holding on would reserve the identifier against reallocation for
  the session's life while nothing will ever write the frame. **The replacement
  TRANSFERS**: a v5 SUBSCRIBE may carry one filter twice with the first entry asking for
  the replay, so releasing the displaced frames would silently discard a retained delivery
  the client is owed. They move onto the replacement, ahead of anything that packet
  serializes, and nothing is released there — which also removed the deadlock the first
  fix for this needed, and the capacity leak, by construction. The acceptance's second
  half landed too: `TestMQTTInflightEveryQoS12SendIsReserved` gained a **reference-based
  frozen close set** over identifier-bearing PUBLISH framing (`mqttMakePublishHeader`,
  `mqttMakeAliasedPublishHeader`, `WritePublishHeader`), rejecting any production
  reference that is not the callee of a direct call — an alias escapes a call-name guard —
  and its "retained sends are an accepted exclusion" paragraph is deleted. Also landed: a
  `sweepDeadSendHoldsLocked()` at the rebind, which is the only point a hold's owner
  changes, as bounded-memory hygiene with nothing resting on it. **Residuals, in
  `docs/CONFORMANCE.md`:** the guarantee is about the ALLOCATOR and the PUBLISH path —
  while the reservation is live, `bumpPI` will not hand the identifier out and no PUBLISH
  send section may open under it. PUBREL admission is untouched, and the whole
  stale-PUBREL multiplexing class is F-092, filed with all three refuted fences. What the
  written and abandoned paths release is the SUPPLEMENTAL reservation, not the identifier
  — the `pendingPublish` entry reserves that until the client acknowledges, or until the
  next resume for a frame abandoned with a rolled-back subscription. The post-write
  stale-PUBREL hazard is F-092, filed with both refuted fences. And the identifier of a
  retained send was not carried across a RESTART until F-105 7c (below) - there was no
  register by R6, the entry was dropped on resume, and a later allocation could re-mint it
  while a QoS 2 client still held it. Two pre-existing items stay open and are named
  rather than absorbed: a retained frame on a rolled-back subscription keeps its
  `pendingPublish` entry until the next resume, and the `trackPublishRetained() == 0`
  downgrade delivers at QoS 0 where [MQTT-3.8.4-6] / Spec5 [3.8.4-8] require the delivered
  QoS to EQUAL min(published, granted) — this item only refused to add a second route into
  it.

  **F-105 dependence (audited 2026-10-05; corrected 2026-10-06 by WP-F W3):** the property
  this bullet closes is now enforced by F-105 code, so the closure is conditional on
  F-105's integrated gate. What replaced the mechanism its passages describe: since 7c a
  retained QoS 1/2 delivery is an admitted exchange with its own register, manifest and
  (QoS 2) `rel` record on `$MQTT_inflight`; the custody ledger charges it and a resume
  restores it from the register (`installRecoveredRetainedLocked`), so its identifier
  survives a restart. The `trackPublishRetained() == 0` QoS 0 downgrade above still holds.
  See F-105's "Checked bullets resting on F-105".

- [x] **F-092: a stale stored PUBREL can be transmitted under an identifier another
  exchange owns.** **CLOSED 2026-09-05 by the eighth increment**, which completed the last
  open item (#2, the custody-after-drain stall). Three mechanisms landed, and the division
  of labour between them is the design: an accepted-incarnation PROOF, a transmission
  FENCE ordered `action CAS -> proof -> socket write`, and an inline custody REPAIR on
  admission's custody-only refusal. A slot withheld at recovery because its action was live
  keeps the predecessor's claim once that action drains; the successor now takes custody on
  the delivery that would otherwise have been refused, under five conditions (every other
  admission check passed, a foreign non-empty claim, a quiescent slot, a proven-current
  incarnation bound to this connection, and a strictly older claim sequence), at most once
  per delivery. **The retry loop is the PUBREL consumer's own re-offer** — a refused record
  is never acknowledged and the consumer has no delivery limit — which is why Decision 7's
  successor-side reconcile WORKER was superseded: four plan-review rounds each found another
  gap in the in-memory placeholder lifecycle it needed, and an escalation consult ruled the
  worker out entirely rather than patch a fifth. The claim ordering is now enforced by every
  custody writer BEFORE its first write, closing the schedule where a paused older resumer
  overwrote a newer owner's custody with a CAS that met no conflict. And a required terminal
  advance whose outcome cannot be resolved refuses the resume instead of accepting a
  connection whose identifier stays charged. Residuals — the narrowed acquisition-to-write
  window, the redelivery-driven progress bound, the storage-progress dependency of that
  refusal, and the pre-existing stranded local reservation — are stated in
  `docs/CONFORMANCE.md`. The earlier work record follows: the sixth attempt
  (`~/.claude/lanes/f-092-sixth-attempt/`). One `$MQTT_inflight` record — the owner slot —
  now owns an exchange for its WHOLE life, so the identifier has exactly one durable
  claimant instead of two independently changing durable sets nothing could arbitrate
  between, and **no SLOT-BACKED PUBREL bytes are transmitted without first ATOMICALLY
  ACQUIRING a non-revocable action on that slot**. A live retained exchange is the one
  exception: it has no register (R6) and so no slot, and is authorized by the live in-memory
  owner identity of its incarnation, refused rather than transmitted once that identity is
  gone. A record naming a different exchange is refused and deliberately NOT acknowledged,
  so AckWait re-offers it and a refusal can never lose a PUBREL the client is owed.

  **What the attempt learned that the plan had wrong.** The stated invariant "a
  `publish`-state slot with a matching nonce is REFUSED (the phase is the permission, not
  the nonce)" was **incorrect**, and consult 3 confirmed the refutation: a stored PUBREL
  cannot exist without a PUBREC — the ordinary producer stores only on the successful-PUBREC
  path, the only other is route-B recreation from a persisted intent, and the builder refuses
  an absent owner nonce — so a valid same-nonce record IS durable evidence of PUBREC. And
  requiring `pubrel_ready` was not merely over-strict: the store makes a record deliverable
  and JetStream delivers it BEFORE `storeMsg` returns, so no CAS ordering can make the phase
  visible first, and the requirement made every ordinary outbound QoS 2 exchange wait out
  AckWait — refutation 0's regression by another route. The corrected invariant is the
  atomic acquisition above, satisfied by a **direct arrow to `pubrel_ready`** that
  installs the canonical record identity and the action token in ONE CAS.

  **Only the retained shape is authorized without a slot**, and only by the exact live
  in-memory owner identity of its incarnation, which is why a retained record surviving a
  restart is refused rather than transmitted (R6 closed). A drained exchange's persisted
  intent is recovery evidence, not transmission authority: after the past-PUBREC drain keeps
  its owner slot to terminal, its PUBREL acquires the same durable action as every other
  slot-backed exchange.

  **The residual is stated in `docs/CONFORMANCE.md`:** an abandoned action holds its
  identifier for an unbounded TIME. Finite failures recover; process loss, a permanently
  blocked holder and indefinitely unavailable storage do not. There is no lease, TTL or
  steal, because a broker can pause after its own expiry check and an MQTT client cannot
  validate a fencing token. The per-session bound is the actions admitted under historical
  limits, ultimately 65,535 — NOT the current effective capacity, since Receive Maximum is
  re-evaluated on every CONNECT — and there is no global bound across sessions.

  **What the sixth attempt left, and why it did not ship.** Two consecutive holistic
  review
  rounds returned NO-SHIP, both on F-092's core hazard rather than on peripheral gaps, and
  the second was explicit that "recording these risks does not make the claimed F-092
  closure safe". `make full-suite` is RED on two drain tests
  (`TestMQTTDrainRecreateEscapesASkippedStore`,
  `TestMQTTDrainClusterLateForeignPubRelStore`), both traced to the unbuilt route B. The
  lane is `~/.claude/lanes/f-092-sixth-attempt/`; `review-ledger.md` carries every round,
  both convergence verdicts, three consults and the eleven implementation bugs found by
  running rather than reading.

  **The two entangled blockers the seventh increment settled**, after the cheaper ways
  round
  them were refuted:

  1. **Admission now proves actionable custody.** Recovery refreshes the positively
     eligible `publish` and `pubrel_ready` population before CONNACK, and admission requires
     `cur.Claim == sess.regClaim`. Action acquisition also carries the observed custody
     tuple into its CAS predicate, so a successor's transfer between the admission check and
     the CAS is refused rather than followed. The blanket `publish` rewrite remains
     withdrawn because it rewrote cancelled and tombstoned records.
  2. **Restart and skipped-store drain cases have no authority admission can recognise**,
     which is route B. A recovered in-memory reservation was tried as authority and
     REFUTED: it is process-local, so during a takeover two brokers hold matching ones and
     a lagging predecessor transmits without seeing the successor's changes — F-092
     recreated by its own fix. Codex's alternative is the right one and is the work:
     "affirmative persisted authority comparable to a drain intent."

  **Also refuted, and not to be re-proposed:** §8.1's absent-slot disposal. A register can
  be legitimately released by the drain path while the exchange is still in `publish`,
  so an
  absent slot does NOT prove the exchange is over; narrowing it twice still left it
  acknowledging away a PUBREL the client was owed. It now fails closed, and residue costs
  `$MQTT_out` space (R3) rather than a message.

  **What the sixth attempt had built, tested and mutation-checked at that checkpoint**,
  and
  what the final increment completed rather than re-derived:

  - **Admission's custody check was still missing there.** It had been implemented and
    reverted because `regClaim` is not persisted and the refresh did not yet cover every
    state admission reads. The seventh increment added the universal positive classifier,
    completed the ready-state coverage and restored the check plus its exact-CAS predicate.
  - **§8.1's absent-slot disposal is withdrawn as unsound**, and the reason generalises: a
    register can be legitimately released by the drain path while the exchange is still in
    `publish`, so an absent slot does NOT prove the exchange is over. Narrowing it twice
    still left it acknowledging away a PUBREL the client was owed. The disposition is fail
    closed — refuse, never acknowledge — so genuine residue occupies stream space (R3)
    rather than costing a message.
  - Route B's full nine-crash-cut recovery, and canonical/same-nonce duplicate disposal
    with its complete terminal scan.
  - **Fourteen census rows remained `planned` at that checkpoint.** The seventh increment
    maps them to live tests and the final mode (`MQTT_CENSUS_FINAL=1`) now passes.

  The original filing follows.

  **F-092: a stale stored PUBREL can be transmitted under an identifier another exchange
  owns.** Filed 2026-08-31 by F-091's holistic review (round 5) and its reconcile consult.
  NOT an alpha blocker on the reading that reached it, but it is the same
  identifier-multiplexing class as F-088/F-089 and should be judged with them. *The finding:*
  `mqttProcessPubComp` removes `pendingPubRel[pi]` and then fire-and-forget acknowledges the
  PUBREL record, so a lost acknowledgement leaves it AckWait-eligible while nothing
  names the identifier. After reuse its redelivery reaches `beginPubRelSendLocked`, which
  sees no token, and `trackAsPubRel` then installs `pendingPubRel[pi]` alongside the new owner's
  `pendingPublish[pi]` — two exchanges through one pi-keyed slot, with the duplicate
  check at the callback recognising only an already-FILLED entry.
  **F-091 does NOT filter any part of this class**, deliberately: PUBREL admission is
  unchanged from before it. THREE candidate partial fences were tried during F-091's review
  and all three were REFUTED — see below. Do not re-propose any of them; F-092 owns the whole
  class with one coherent design.
  **Refutation 0, the retained-term refusal itself:** refusing while a retained
  reservation is live blocks the retained QoS 2 exchange's OWN legitimate PUBREL, because
  the delivery-side flusher keeps that reservation live until after it also writes the
  triggering live PUBLISH — so a slow client's PUBREC arrives while the write is blocked
  and the record waits out AckWait, about thirty seconds, where HEAD transmitted at once.
  The compensating "retire the reservation at PUBREC" then needed two further fixes and
  still lost custody across a concurrent `clear()` in one ordering or the other, so the
  refusal and the retirement were both DELETED (holistic rounds 8-10, lane `DECISIONS.md`
  D4). **Refutation 1, with measurements:** refusing
  on any live `pendingPublish` entry made `TestMQTTDrainClusterLateForeignPubRelStore`,
  `TestMQTTDrainPubRelRecreatedFromIntent` and `TestMQTTDrainBarrierFailsClosed` time out
  with the client waiting for a PUBREL it was owed, because in-flight recovery installs it
  as `pendingPublish[pi]` and the persisted drain intent then legitimately recreates a
  CORRELATED PUBREL under that same identifier. "Any live entry means a different
  exchange" is simply false.
  **Refutation 2:** refusing on a SOURCE-LESS entry — no ack subject, no durable, no
  register — looks exact, because no stored PUBREL can correlate to one. But an abandoned
  retained frame leaves that empty entry behind until the session's next resume, so the
  record would be refused for the life of the connection. It was implemented, caught by
  F-091's holistic round 6, and removed. (It is also not a retained-entry classifier: a
  cancellation tombstone carries the same triple.)
  **The settled design is correlation, not entry kind:** the discriminator is the record's
  `(durable, source sequence)` correlation against the source correlation of the entry that
  owns the identifier, and a refusal WITHHOLDS without acknowledging so AckWait re-offers
  it. **The rule "refuse a mismatched OR UNCORRELATED one" was this bullet's original
  wording and it is CORRECTED by the 2026-09-02 attempt below** — refusing every
  uncorrelated record indefinitely withholds an ordinary retained QoS 2 PUBREL, which is
  refutation 0's regression arriving by another route. The established boundary is: refuse
  a **positive** mismatch (both correlations valid and unequal, or exactly one valid); defer
  absent/absent while a `pendingPublish` entry owns the identifier, which is what stops a
  PUBREL preceding a still-buffered retained PUBLISH; **admit** absent/absent once a
  legitimate PUBREC has made the owner an absent-correlated `pendingPubRel`, where the two
  records are indistinguishable and the cost is at worst an extra PUBREL/PUBCOMP cycle; and
  treat a **malformed** correlation as a third, distinct refusal state (see the tri-state
  constraint below). The correlation is already parsed as `corr` in `mqttDeliverPubRelCb`,
  so the decision cannot be made by the two-argument `beginPubRelSendLocked` alone: the
  caller decides, and the attempt below establishes that the opener must be left
  **unchanged** rather than grown an argument.
  **QoS 2 subtlety not to lose:** PUBREC does not release
  the identifier, it transitions it into the current exchange's PUBREL phase, so a mismatched
  record must stay withheld until PUBCOMP releases it — "until the acknowledgement" is wrong.
  If exact retained identity is ever needed, add a marker rather than infer it from fields.
  Residual stated in `docs/CONFORMANCE.md`.

  **ATTEMPTED AND STOPPED UNRESOLVED 2026-09-02**, with **no implementation code**: the
  only
  working-tree change the attempt produced is this filing. Four plan-review rounds, one
  scope consult and one not-converging escalation consult, and a pre-registered stopping
  rule that fired. Lane `~/.claude/lanes/f-092/` keeps **revision 4 of the plan only** —
  the earlier revisions were overwritten in place and are not archived — plus the ledger,
  the four review outputs, and the two rulings (`DECISIONS.md` Decision 1, scope; Decision
  2, the escalation). **F-093 below is part of this item, not a separate one** — that was
  Decision 1's ruling and nothing since contradicted it, so a later attempt takes both or
  neither.

  **What the attempt ESTABLISHED, and must not be re-derived.** The settled design in the
  paragraph above — correlate the record's `(durable, source sequence)` against the entry
  owning the identifier — is *necessary and not sufficient*, and three successive traps sit
  behind it, each of which a revision fell into:

  1. **Correlation alone is not enough.** Several unrelated states deliberately carry empty
     `jsDur`/`sseq` — a cancellation tombstone (`inflight_register.go:2224`), an opaque
     recovered reservation (`:5331`), a demote (`:5209`) — so an absent-versus-absent
     comparison reads them as "the retained exchange" and admits. An *eligibility* question
     exists beside the correlation question.
  2. **Eligibility is not the same question as source readability.** A successful PUBREC
     installs `pendingPubRel` before removing `pendingPublish` (`mqtt.go:16629`), is
     explicitly allowed while a PUBLISH send section is live (`:16642`), and writes a valid
     correlation onto the record from the removed entry (`:16695`, `:16746`); every affine
     PUBLISH entry carries `q2AffClaim`/`q2AffCopy` (`qos2_shared_affine.go:3388`) and its
     PUBREL likewise carries a valid correlation. Gating the *source copy* on eligibility
     leaves the owner absent-correlated while its record is valid — "exactly one valid" —
     and **permanently refuses ordinary affine QoS 2**. Any design must keep the two
     separate.
  3. **Classifying every owner state is itself the premise that fails.** Enumeration does
     not close: the census of `pendingPublish` writers is exact (ten sites) but entries
     change class **in place** afterwards — `confirmInflightRegister` clears `regPending`
     and sets `regSeq` (`inflight_register.go:3367`), recovery refreshes `regSeq` (`:3624`,
     `:5292`) — and `retrackPublishLocked` reinstates an arbitrary pointer. This is
     `feedback_close_the_class_with_a_property` in its own subsystem.

  **Facts the attempt settled that a successor should take as given, not re-litigate:**
  the register disposition for the transition must be **subject-and-identity addressed**
  (`releaseInflightRegisterByIdentity`), never `deleteInflightRegisterFor`, which refuses
  `regSeq == 0` and reaps at that exact sequence (`inflight_register.go:1677`, `:1224`) —
  the addressing F-034 forbids. `untrackPublishEntry` deletes `cpending[jsDur][sseq]`
  **unguarded** (`mqtt.go:11449`) where `inflight_register.go:3702` shows the guarded
  form;
  that wants correcting for every caller. The duplicate branch must be **scoped to a
  correlation-matching record and ordered below the admission revalidation**, or a stale
  callback acknowledges a record away against a stale owner. `SrcDur`/`SrcSeq` must be left
  empty unless the correlation is valid, which is what keeps the five `mqttStoredPubRel`
  consumers safe. And a **third route into F-093** exists beyond the two named below: during
  a takeover overlap another broker can store the correlated PUBREL after this broker's
  recovery, because PUBREL delivery uses an ordinary non-queue subscription
  (`mqtt.go:18808`), and that record can meet a matching **affine** PUBLISH owner here.

  **Three further constraints the rounds established, each of which a successor must carry
  or re-earn.** (1) **A malformed correlation is a third state, not an absence.**
  `mqttParsePubRelCorrelation` (`pubrel_correlation.go:193`) folds *absent* and *malformed*
  into one `Ok == false`; under the compatibility boundary an unsupported persisted shape
  is refused, so it needs the absent/valid/malformed tri-state `mqttParsePubRelAffineRef`
  already has, `SrcDur`/`SrcSeq` populated only when valid (which is what keeps the five
  `mqttStoredPubRel` consumers safe without touching any of them), and — because a
  malformed record has **no convergence event** — a malformed record arriving live with no
  owner must **reserve its identifier**, or `bumpPI` hands out an identifier the client may
  still be holding for a PUBREL exchange. (2) **Any such reservation must be protected from
  the PUBREC path.** `mqttProcessPublishReceived` calls `trackAsPubRel` for every successful
  PUBREC without checking the existing owner (`mqtt.go:16629`) and both write arms overwrite
  unconditionally (`:12018`, `:12035`), so a duplicate or stale PUBREC replaces a reserving
  entry with an ordinary placeholder and a failed store then momentarily unreserves the
  identifier. The guard has to be a real handler-level early return, the way `regForeign`
  is already declined at `:16615`, not a void `trackAsPubRel` no-op with the rest of the
  PUBREC path still running. (3) **No irreversible register work while a PUBLISH send can
  start.** The opener owns transmission admission only; it cannot undo destructive work
  performed before it, so settling a register while the PUBLISH entry is still installed
  lets a concurrent redelivery enter `beginSendLocked` (`inflight_register.go:3739`) and put
  the payload on the wire after the register is gone.

  **Round 4's fourth blocker was about oracles, and it is worth keeping** because the same
  fixtures will be reached for again: a mutation naming only its fixture and no outcome
  assertion is not paired with anything; a mutation whose "correct" baseline also fails the
  race oracle is invalid against its own baseline (which is how B1 was found); a mutation
  about premature PUBCOMP needs its test to actually inject one; and every negative-outcome
  test needs a **route-reached** assertion, or a handler that returns immediately satisfies
  every stated negative — see `feedback_fixture_must_reach_its_route`.

  **The affine question is ANSWERED and is not an obstacle.** The stored PUBREL's own
  `pubRelAffRef` **is** the durable carrier of the affine obligation past PUBREC — the
  record stores the pair before affine completion (`mqtt.go:16746`), the callback copies it
  onto the entry (`:18180`), PUBCOMP discharges it through `dischargeQ2AffPubRelProof`, and
  resume reconciles it through `reconcileQ2AffPubRelProofs` (`qos2_shared_affine.go:3850`).
  So `q2AffClaim`/`q2AffCopy` do not forbid removing the PUBLISH entry. They select a
  different settlement arm: the source must be **retired** rather than the register
  released, through `retireSourceLocked` (held lock, `mqtt.go:11495`) and
  `secureRetiredSource` (no lock, `inflight_register.go:1991`), with
  `resolveRetiredSource` (`:1851`) as the convergence step where the peer's acknowledgement
  already landed. Retiring a source this broker never acknowledged is correct and drainable.

  **What the attempt RETRACTS.** Its own escalation remedy. After three rounds re-cut the
  admission seam, a consult ruled two changes: *deny by default* (admit only an affirmative
  set, defer the unclassified complement, so taxonomy completeness stops being a
  correctness premise) and a **settlement-pending `pendingPubRel` placeholder** as the
  off-lock reservation, chosen over a fourth reservation registry. Round 4 refuted the
  combination, and the refutation is the most valuable thing here:

  - **The placeholder reserves the identifier but un-suppresses the SOURCE.** Removing the
    PUBLISH entry's `cpending` link is what lets a fresh source redelivery miss the old
    identifier entirely, reach `bumpPI`, allocate a *different* one and transmit
    (`mqtt.go:11292`, `:11340`, `:11374`). Recovery's own code exists to prevent exactly
    this and says so — it restores the source mapping because "`trackPublish` reaches
    `pendingPubRel` only through the mapping" (`inflight_register.go:5243`). Reachable in
    the plain arm too, since `finalizeDrainDelivery` acknowledges the source
    fire-and-forget (`unsub_drain.go:848`). **Any successor must install the source
    suppression atomically with the handoff**, not after it, and must cover the plain
    lost-ack case and not only the affine one.
  - **Deny-by-default and a resumable placeholder contradict each other.** A
    `settlePending` owner is by construction not affirmatively admissible, so the preflight
    returns before the retry path it needs. The two halves of one ruling are incompatible,
    and no local revision resolves it: an affirmative "this record resuming its own
    placeholder" row is required, which re-opens the very completeness question deny-by-
    default was adopted to retire.
  - **`clear()` cannot leave the record retryable.** `clear()` captures the retiring
  PUBREL
    durable (`mqtt.go:10727`), removes both pending maps and `cpending` (`:10742`), re-mints
    the generation (`:10812`) and schedules the old consumer's deletion (`:10866`,
    `sess_teardown.go:820`). The next incarnation uses a different PUBREL subject, so the
    old record is teardown material, not retry state. A stopping rule demanding
    "retryable after `clear()`" was demanding the wrong thing; a successor states discard
    semantics there instead.

  **What is still OPEN, and where a fifth attempt starts.** The **marker** this bullet
  already names is the live candidate, and the attempt's refusal of it is now weaker than
  when it was made: it was set aside because "a matching marker proves identity, not
  permission to act", so the `regForeign` and opaque authority checks survive it. That is
  true and it is not disqualifying — the identity half is exactly what three rounds of
  inference failed to reconstruct, and `mqttPending.regNonce` **already is** an
  allocation-minted per-exchange identity, register-carried and recovery-restored, so the
  work may be carrying it on the record rather than inventing anything. A retained send has
  no register at all (R6), so retained still needs a minted marker. Also unresolved and
  bounded: the **undecidable** case, a stale *uncorrelated* record colliding with a new
  uncorrelated (retained) exchange on one identifier — both sides legitimately absent. The
  boundary the attempt did establish is that while the retained exchange is still
  `pendingPublish` the stale record must defer (that is what stops a PUBREL preceding the
  retained PUBLISH), and that once a legitimate PUBREC has made it an absent-correlated
  `pendingPubRel` the two are indistinguishable and the cost is an extra PUBREL/PUBCOMP
  cycle, never a payload under the wrong exchange.

  **Historical sixth-attempt checkpoint (2026-09-03; superseded by the closure above).**
  At
  that point the in-flight register
  carries the vocabulary a durable owner slot needs, and every mutation of it is gated.
  That is the substrate the rest is built on. **The register's lifetime is UNCHANGED —
  it is still deleted at the PUBREC handoff.** Re-phasing it to survive as the
  exchange's owner was implemented, failed the ported suite's restart and cross-broker
  paths (the slot outlived the exchange, because the terminal release it needs belongs
  to the unbuilt admission work), and was **WITHDRAWN rather than committed
  half-done**: a surviving slot
  with no terminal release leaks an identifier per QoS 2 message, which is worse than the
  hazard it fixes. What the rounds did settle is the premise five whole-item plan revisions
  failed on: rounds 1-2 established that a stale record cannot be arbitrated against a live
  one, because identity does not confer authority and two snapshots cannot be compared
  fail-closed. What landed, all green under `make test` with no data races: a mandatory,
  never-defaulted `state` on the register and its single legal-shape validator
  (`internal/mqtt/inflight_slot_state.go`); a **closed schema** at the one decoder, so any
  unrecognised field makes a record unreadable; a mandatory `Nmqtt-Q2Owner` header on every
  stored PUBREL with a tri-state parser, decoded as one shape by `mqttDecodeStoredPubRel`;
  a mandatory `OwnerNonce` **and packet identifier** on the drain intent, required by both
  the recovery comparison and the asynchronous delivery gate; one gated rewrite path
  (`casInflightSlot`, which replaced the rebuild-from-arguments pattern in the mark
  helpers — that pattern silently dropped every field the rebuilder did not know about);
  and the phase gate wired into the register-mutation and identifier-release edges C01,
  C04, C05b, C08, C09, C19, C21, C26, C27, C28, C39a/b.

  **What had been built and then DELETED before that checkpoint, on a review ruling**,
  because
  nothing produced any of it: the four non-`publish` states; the five durable fields
  `ActionToken`/`ActionClaim`/`ActionClaimSeq` and `PubRelDurable`/`PubRelStreamSeq`;
  `mqttInflightSlotRef.actionToken` and its `holdsAction`; the `mqttSlotDrainAction`,
  `mqttSlotTransferCustody` and `mqttSlotAdvancePhase` dispositions; and the whole local
  half — `mqttSendAction` with its four states, `mqttSendHold.action`,
  `mqttSendHold.pubIdent`, `actionHeldLocked` and its term in the liveness disjunction.
  The `publish` state is the only one anything writes, and the validator forbade all five
  fields in it, so their entire function was to recognise and refuse their own hypothetical
  semantics — an INCOMPLETE closed schema, since a sixth plausible control field would have
  been dropped silently by `encoding/json`. A closed schema at the decoder does that job for
  every key instead of five guessed ones, which is why the deletion STRENGTHENED the
  F-056/F-057/F-058 posture rather than weakening it. `DisallowUnknownFields` alone was not
  enough and a further round proved it: `encoding/json` folds case and allows duplicates
  with the last value winning, so the decoder walks the keys itself — exact case, no
  duplicates, permitted set derived from the struct's own tags. All of it returns
  with the admission half, atomically, with the producers and consumers that make it
  load-bearing. The rules those fields carried are recorded above and below.

  The validator ended up behind **two** boundaries rather than one, and two review rounds
  were spent getting there. `storeInflightCAS` governs every write. Every READ goes through
  `mqttDecodeInflightRecord`, because validating the recovery scan alone still left the
  destructive readers — drain identity release, the gated reap, generation teardown —
  authorizing a delete from an identity-valid but illegal body. Enumerating the readers was
  the defect, so an AST guard keeps the decoder the only one there is — and that guard's own
  allow-list turned out to exempt three functions that did not exist, which is why its
  property is now bidirectional: every access is exempt, every exemption exists, every
  exemption is used.

  **What was NOT built at that checkpoint:** PUBREL **admission** was
  unchanged and the register is still deleted at PUBREC, so the stale-record hazard is exactly
  as before. Named explicitly because a holistic round found the docs claiming
  otherwise: a **retained** exchange's stored PUBREL surviving a restart is still
  transmitted. Its owner is syntactically valid, so the recovery filter — which
  rejects only an absent or malformed one — keeps it, and the ownership gate in the
  delivery callback is keyed on a source correlation a retained record never carries.
  A retained send has no register at all (R6), so nothing durable can authorize it
  and admission is the only place this closes.

  Outstanding at that checkpoint were the PUBREC re-phasing and its terminal
  release, the durable action claim that fences the no-deadline socket write
  (with its arm-before-CAS holder
  lifecycle, pre-write abort and drain retry), §7's exact live-PUBLISH-send-token refusal,
  the terminal states and PUBCOMP's terminal cleanup, canonical/same-nonce duplicate
  disposal, route B's four-way dispatch and its nine crash cuts, the
  `mqttInflightClaimWithheldAction` recovery outcome, the remaining census edges, and the
  test and mutation matrices. The lane `~/.claude/lanes/f-092-fifth-attempt/` holds the
  signed-off plan (`plan.md`, slice-1 revision 6, IMPLEMENTABLE at round 7), six decisions,
  the ledger and every review payload.

  **Two rulings from the implementation worth keeping.** A `Claim` with no `ClaimSeq` is
  LEGAL — the cancellation tombstones at `inflight_register.go:1956` and `:2686` write
  exactly that and the arbitration compares `cur.ClaimSeq < claimSeq`, so a zero sequence
  loses rather than being incoherent; only a sequence with no claimant is refused. And a
  slot deletion needs THREE things, not two: subject addressing (F-034), identity
  authorization, and a **freshness fence** on the observed sequence — a rewrite means a
  concurrent operation re-targeted the exchange and reinstalled memory around it, so a stale
  terminal continuation must preserve the record. F-034 mutation-tested the opposite as M26,
  and the "a moved register is preserved" case of
  `TestMQTTInflightTerminalCleanupRemovesRegisterThenMarker` is the fixture that catches it.

  **Supported-scope rule since 2026-09-02:** a later F-092/F-093 implementation must close
  the ordinary, retained, drain and supported recovery paths. It need not preserve or
  classify an experimental affine PUBLISH owner. The affine facts below remain useful
  historical evidence, but an affine-only failure is appended to chapter P rather than
  widening this item or blocking its general solution.

  **F-105 dependence (audited 2026-10-05; corrected 2026-10-06 by WP-F W3):** the property
  this bullet closes is now enforced by F-105 code, so the closure is conditional on
  F-105's integrated gate. What replaced the mechanism its passages describe: custody
  transfer and the custody-only repair are the experimental slot-only class's alone since
  7b; an ordinary slot's ownership is its exchange's, arbitrated by the connection's
  accepted entitlement inside the acquisition batch. The retained exception is gone since
  7c: a retained exchange has a register slot, and its canonical PUBREL is a `rel` record
  on `$MQTT_inflight` created in the PUBREC advance batch and acquired like any other
  slot. See F-105's "Checked bullets resting on F-105".

- [x] **F-093: the UNSUBSCRIBE drain leaves a recovered PUBLISH entry reserving its
  identifier.** **CLOSED 2026-09-05 with F-092** ("take both or neither"). Its ordered
  handoff was built and tested in the seventh increment; the eighth closed F-092's last
  open item, which is what both boxes were waiting on.
  The ordered handoff below is built and tested, and
  it
  is the half that was missing. The ordered handoff runs under one session-lock
  section: the same-nonce PUBREL
  owner is installed, then the source suppression, then the PUBLISH entry is compare-deleted,
  then its `cpending` mapping is compare-deleted GUARDED on the identifier it names — unlike
  `untrackPublishEntry`, which deletes unconditionally and would remove a mapping a different
  identifier now owns. A completed exchange leaves no `pendingPublish` entry and no mapping,
  so it no longer reserves an identifier and a Receive Maximum slot for the life of the
  session, and no longer charges that identifier twice while both entries live.

  One subtlety worth keeping: the suppression is installed as **already durable** when an
  action was acquired, because a past-PUBREC slot IS the durable statement that the exchange
  is past PUBREC — marking it insecure made PUBCOMP withhold the acknowledgement until a
  second mark was written for a fact the slot already implied, and the client got a duplicate
  PUBREL. On the drain-intent path no retirement is installed at all: the drain state machine
  owns that source.

  The original filing follows.

  **F-093: the UNSUBSCRIBE drain leaves a recovered PUBLISH entry reserving its
  identifier.** Filed 2026-08-31 by F-091's reconcile consult, which found it while refuting
  the general PUBREL fence. NOT an alpha blocker: the residue is one identifier and one
  Receive Maximum slot per occurrence, not a data loss. *The finding:* `finalizeDrainDelivery`
  releases the recovered in-flight register, but nothing appears to remove or transition the
  recovered `pendingPublish` entry and its `cpending` mapping; the callback then installs
  `pendingPubRel`, and PUBCOMP removes only that PUBREL entry. The stale PUBLISH entry can
  therefore keep reserving the identifier and its Receive Maximum slot for the life of the
  session. Filed rather than folded into F-091 because it is a drain-bookkeeping defect
  reached from a different route, and because F-091 neither introduced it nor is chartered
  to fix it — but note it is the reason the general fence in F-092 cannot simply be tightened
  without looking here first.

  **NOT A SEPARATE ITEM, decided 2026-09-02 and confirmed by the attempt above.** F-093's
  correction **is** F-092's match-success arm: prove the record matches the PUBLISH-phase
  owner, settle that owner's register and drain obligation, replace `pendingPublish[pi]`
  with `pendingPubRel[pi]`, and withhold every non-matching record until that owner reaches
  PUBCOMP. Doing F-093 alone would implement half of that transition in
  `mqttDeliverPubRelCb` and then reopen the same seam for F-092's refusal arm; doing F-092
  alone turns this bounded availability leak into an **unbounded** one, because F-092's
  fence withholds a non-matching record without acknowledging it and that only converges if
  the entry owning the identifier eventually resolves — and this box is precisely an owner
  that never resolves. Take both or neither.

  *Premise verified in the tree, so the bullet's "nothing appears to remove" hedge can
  go:*
  `finalizeDrainDelivery` (`unsub_drain.go:607`) never touches `pendingPublish` or
  `cpending`, and the only `untrackPublishEntry` in `unsub_drain.go` is `stageDrain`'s
  never-transmitted `regPending` arm (`:390`). Recovery installs the entry at
  `inflight_register.go:5306` with `reserveSource`; `trackAsPubRel` (`mqtt.go:12006`) then
  installs `pendingPubRel[pi]` beside it; `mqttProcessPubComp` removes only the PUBREL entry
  (`:12045`). `bumpPI` skips any identifier with a live `pendingPublish` entry (`:11095`)
  and `qos12InflightForCap` counts it against Receive Maximum (`:11135`), where with
  `rmax != 0` the one identifier is charged **twice** while both entries live. **Three
  routes reach it, not one:** route A through `finalizeDrainDelivery`; route B where the
  resume barrier confirms and removes the claim itself and acks the source
  (`unsub_drain.go:1666-1753`) so `finalizeDrainDelivery` is skipped **and the register is
  never released at all**, which is why the *next* resume rebuilds the entry and
  re-establishes the reservation rather than clearing it; and route C, a peer's correlated
  store during a takeover overlap, which can present an **affine** matching owner. The
  live-path contrast is that `mqttProcessPublishReceived` untracks the PUBLISH entry at
  PUBREC under one lock (`mqtt.go:16629`, `:16660`), so on a live connection the two never
  coexist.
  Route C's affine subcase is experimental under the 2026-09-02 scope decision and is not
  part of this item's supported acceptance; routes A and B remain unchanged.

- [x] **F-094: adopting a drain erases an unfinished past-PUBREC intent.** Filed
  2026-09-04 by F-092 slice 1's holistic round 19; DONE 2026-09-05 after 5 plan-review
  rounds (findings 8/8/8/3/0) and two escalation consults. Lane `~/.claude/lanes/f-094/`.

  **What was wrong.** `adoptDrainLocked` refused only an `ownerUnusable` sequence, so a
  VALID past-PUBREC intent was adopted: the drain left both maps and its whole `seqs` map
  was dropped. The delivery callback finds a drain through `drainByDur`, so the
  still-unacknowledged source then bypassed every drain branch and was re-offered at
  AckWait as a **fresh PUBLISH of an exchange the client had already answered**, on a
  different packet identifier because PUBREC had removed the original `cpending` link.
  The window is PUBREC-after-staging until the stored PUBREL is delivered back — *not*
  until PUBCOMP, which two plan revisions got wrong.

  **What replaced it.** `drainQuarantinedLocked` became `drainSidHoldLocked`, returning a
  reason: an unusable identity (unchanged, operator-only) or a past-PUBREC intent. The
  intent predicate is **unqualified** — `st.intent` alone. Two narrower shapes were tried
  and refuted and the code comment records both counterexamples, so neither may be
  re-proposed: qualifying with `claimed || unclaimUnconfirmed` misses a finalized intent
  whose source ack was lost, because `finalizeDrainDelivery` clears that guard before the
  fire-and-forget ack; the same test at drain level misses it again at the last
  outstanding sequence. Both refusal messages now name the true convergence — they
  previously told the operator to remove a record that, for an ordinary intent, is doing
  nothing wrong.

  **The release had to be built.** An unqualified hold lasts as long as the drain object,
  so retirement was repaired: retirable now means DURABLY empty (no `claimed`, no
  `unclaimUnconfirmed`), ownership is proven through `sess.drains[sid]` rather than
  `drainByDur` — without which a drain whose delivery subscription failed to restore could
  never be retired and its filter was refused for the life of the session — and
  reconciliation sweeps twice, before its early return for an already-empty drain and
  after its save for one its own pruning emptied. Collapsing those two into one relocated
  sweep loses the second case.

  **At fork repair the two holds refuse different rows.** An unusable identity refuses
  every row, unchanged. A past-PUBREC intent refuses only the destructive ones, `live` and
  `absent`; the `draining -> draining` rows preserve the drain either way, so the hold lets
  them THROUGH the preflight and each keeps its existing outcome — the equal row succeeds,
  the differing row stays the conflict it already was. The
  `absent` row is not exempt: `mqttSurvivorDecidedGrant` compares drain PRESENCE only, the
  lineage recognition does not establish descent from our baseline, and our own omission
  can come from pruning a source proved absent rather than from completion.

  **Residuals in `docs/CONFORMANCE.md`**, and the boundary is deliberate: F-094 closes the
  ADOPTION route to that duplicate PUBLISH and nothing more. Two bullets below carry what
  it does not.

- [x] **Drain retirement does not prove disposal before releasing the sid.** Filed
  2026-09-05 by F-094's plan-review rounds 2 and 3. **Closed 2026-09-06** after 8
  plan-review rounds (4 against `d75bdb1`, 4 after revalidation against `67864e7`), a
  Codex scope consult, and 12 mutations each confirmed to fail its paired test.

  **What landed.** `retireDrainLocked` releases the sid and closes the delivery path
  under **one** `sess.mu` hold: the drain is proven to still own its sid, the object is
  marked terminally `retired`, `sub.mqtt.closed` is set on its delivery subscription,
  and both drain maps are dropped — in that order, without releasing the lock. That is
  what excludes an already-dispatched callback, which re-checks `closed` and only then
  looks its drain up under one acquisition of the same lock; `processUnsub` never
  reached it, because the `*subscription` it already holds stays valid. The consumer
  delete then **waits** and is classified three ways — deleted, confirmed absent,
  unknown — replacing a `noWait` request whose error check could never be true. The
  terminal flag gates the four routes that could hand the object back
  (`rollbackSubscribePacket`, which now restores **each map's own entries** and skips a
  retired one; `restoreDrains`; `restoreDrainLocked`; `clearDrainInactivityTimers`);
  adoption and the delivery reclaim need none, since both read `sess.drains[sid]`.
  Retained continuations queue their disposal, detached inside `endRecordTxLocked` and
  issued after the unlock, so no JetStream request is made while the record fence is
  held. Reconciliation's consumer-absent removal refunds its local consumer charge on a
  **proven-landed** save — a live leak, since such a drain is charged, survives a resume,
  and `tmaxack` is otherwise reset only by `clear()`.

  **The required property, QUALIFIED by what shipped.** Old-path transmission after
  suppression is removed: **closed**. An outstanding deletion destroying a consumer
  returned to service: **closed for this drain object within this resident session
  object's lifetime**, which is narrower than the unconditional wording this bullet
  originally carried. A record restore builds a new session object while preserving the
  generation, so a fork sibling's record can rebuild the drain on a fresh `*mqttDrain`
  and a delete this object issued can land on it. That exposure is **pre-existing and
  unwidened** — the previous unconfirmed delete had it identically — and closing it would
  need a retirement fact a *sibling's* record could see, which no record of ours can
  supply. Coherent sid ownership and accounting: **closed**.

  **Residuals in `docs/CONFORMANCE.md`**, three of them: an unconfirmed delete is
  reclaimed *eventually* by the orphan sweep rather than retried (and is reported); the
  fork-sibling scope above; and an inactivity-clear create already in flight when a drain
  retires. Mechanism in `docs/ARCHITECTURE.md` under "UNSUBSCRIBE drain →
  Retirement and disposal".

  **Preserved failed-design analysis — still design CONSTRAINTS, do not re-propose.**
  Reordering a fire-and-forget request proves nothing. Refusing adoption alone does not
  block provisioning. Unsubscribing does not exclude a dispatched callback. Clearing the
  terminal flag after a timeout lets a late delete destroy a consumer returned to
  service. A post-delete ownership check does not prevent resurrection through a snapshot
  taken earlier. Refunding only in `finishDrainIfEmpty` misses the other removal exits.
  Keeping the drain in `sess.drains` until the delete is confirmed holds the sid forever
  on an indeterminate outcome, because `st.intent` is never cleared. A session-scoped
  retired-**name** set is object-scoped in fact and cannot establish the fork guarantee it
  would exist for. A durable retirement fact in **our own** record cannot supply knowledge
  a **sibling's** record lacks.

- [x] **An adopted drain with no delivery subscription silently stops delivering.**
  Filed 2026-09-05 by F-094's plan-review round 2. **Closed 2026-09-06 for delivery
  READINESS only**, after 5 plan-review rounds and an escalation consult that narrowed
  it; the obligations its original wording also carried are split out into the three
  bullets below, because ticking it whole would have overclaimed.

  **What landed.** A QoS > 0 live SUBSCRIBE that meets a drain with no delivery
  subscription **reclaims** it — `reclaimDrainDeliveryLocked` in `unsub_drain.go`
  installs the subscription and adopts in one `sess.mu` section — or answers the filter
  `0x80`. Called from both `processSubs` insertion points, main and level-up companion,
  with a failure routed into each one's existing consumer-error cleanup arm rather than
  a bare `continue`. Three details are load-bearing and each was established by a review
  round: the install uses the **SUBSCRIBE's own** terms, because the metadata refresh
  runs after the lock is released and the drain's terms would let the callback ack away
  a message the client is owed; the adoption is in the **same** critical section, so
  nothing observes the durable as neither drain nor consumer; and there is **no**
  `drainByDur` registration first, which under that lock is indistinguishable from its
  absence. The reclaim is recorded into the packet's restore snapshot
  (`retainRepairedDrain`), which is what leaves `rollbackSubscribePacket` unmodified.

  **The barrier-skipped route this bullet named was never live**, and the claim is
  withdrawn with the code behind it: every durable in `recoverDrainPubRels`' skip set
  is intent-bearing by construction, so F-094's hold refuses its adoption for as long
  as the drain object exists.

  **What it does NOT claim:** correct completion of every adopted exchange, any change
  to adoption's own failure or persistence behavior, and any atomicity across duplicate
  occurrences of one filter in a packet. Residuals in `docs/CONFORMANCE.md`; mechanism
  in `docs/ARCHITECTURE.md` under "UNSUBSCRIBE drain".

- [x] **A QoS 0 membership over a live `Cons` entry is destroyed by CONNECT restore.**
  **DONE 2026-09-09.** The restore no longer deletes inline: `processJSConsumer`'s QoS 0
  arm defers on both paths, and a restore-side boundary (`settleRestoredDowngrades`)
  converts the consumer into a drain and persists that before any teardown, with the
  destructive arm still releasing its `Cons` reference in the same save. The drain keeps
  **QoS 0** and three per-exchange rules carry the retransmission instead: the
  first-transmission gate does not apply to a claimed drained sequence, No Local does not
  suppress one, and one with no identity-matched exchange QoS is refused **before**
  allocating rather than registered at an invented QoS. `d.subject` now derives from
  `cc.FilterSubject` at all three stagers, which also repairs a resumed drain of a
  `#`/`*`/`*.>` filter losing its reserved marking. The restore reads its own
  `recordCASRejected`, which nothing on that path did before it wrote records. Mechanism in
  `docs/ARCHITECTURE.md` under "A CONNECT restore is the third entry point".

  **What it claims:** removal of the premature restore teardown, and completion of
  exchanges with recoverable authority — **subject to the residual below**.

  **The residual it does NOT fix, accepted by an escalation consult rather than
  answered.**
  A restore-converted QoS 0 drain can complete a QoS 1 exchange, release its register, and
  then definitely fail the save that removes its claim; the claim, the durable consumer and
  the source message are then retained **indefinitely**, across reconnects, restarts and
  healed storage. Two plan-review rounds (4 and 5) re-cut this one seam: round 4 found the
  retention, round 5 refuted the finalization proposed to end it — missing evidence does not
  mean the exchange completed, because the PUBREC window before `recordPubRelIntent` and a
  marker-opaque reservation installed without a `cpending` mapping both produce it with no
  client acknowledgement, and settling there destroys an unfinished exchange. The consult
  ruled the withhold, on the grounds that a complete completion predicate is a separate
  contract and `finalizeDrainDelivery` releases the register *before* saving the claim
  removal, so that predicate would authorize an irreversible action. This is a **documented
  limitation, not a refuted finding.** Stated in `docs/CONFORMANCE.md`.

  **A correction to this bullet's own analysis:** its fact (3) was wrong at HEAD. The
  requested-lifetime operation **does** save — `applyRequestedLifetime` is called after
  `processSubs` and reaches `saveClassifiedDetail` whenever the terms changed — and the
  restore gate's opening is **not** a save site at all. Both matter, because the first is
  a post-boundary exit that can refuse the CONNECT after the conversion is already durable.

  Filed 2026-09-09 by the failed-adoption item, which closed its sibling half and was
  **split by an escalation consult** rather than widened. This is the SECOND loss that
  bullet had been carrying since 2026-09-07, handed to it by the QoS 0 downgrade item's
  scope ruling.

  **The mirror case, and that symmetry is why one bullet held both.** Where a sid's
  membership key is ABSENT the record forgot the durable, and a sibling sweep deleted it —
  that half is now closed by continuous naming. Where the key is PRESENT at QoS 0 the
  record names the durable correctly and the **restore destroys its exchanges anyway**:
  resume reconciliation takes positive QoS only (`sess_resume_reconcile.go:477-481`), so
  such an entry is never verified, and CONNECT restore then reaches `processJSConsumer`
  with `qos == 0`, `exists` true and `fromSubProto` false, running
  `execDeferredConsumerDelete` **inline** (`mqtt.go:21740`) — no deferral, no packet
  transaction, no final-update boundary, so the drain conversion that closed the live
  SUBSCRIBE path has nowhere to happen.

  **Four things are already established with code, and re-deriving them is waste.**
  (1) `stageDrain` cannot be reused unmodified: it takes subject, QoS and options only from
  its `*subscription` argument (`unsub_drain.go:491-497`), and a cold CONNECT has none —
  passing nil, or the restored QoS 0 subscription, leaves `d.qos == 0`, `restoreDrains`
  installs at that, and the delivery callback returns for a non-shared QoS 0 result
  (`mqtt.go:20183-20186`) **before** the entry-based retransmission QoS is read, so an
  outstanding claim survives and never retransmits. (2) **`d.qos` is NOT a harmless gate
  floor** — the tempting fix. It also selects PUBREL handling
  (`drainOwesPubRelLocked`, `unsub_drain.go:1353`), and a claimed source reaching
  `trackPublish` with no installed mapping stores the *subscription-derived* QoS into the
  register (`mqtt.go:20464`), so it can become an exchange's QoS after all. (3) The
  conversion's settlement boundary must account for **every save after staging**, including
  the shared-membership ownership CAS (`mqtt.go:15571`) whose ambiguous arm calls
  `abortAcceptedResume` while deliberately preserving the attempted state, and the restore
  gate opening at `mqtt.go:16200`; a blanket "every exit rolls staging back" contradicts
  that contract. (4) The accepted-CONNECT batch has already committed before `processSubs`
  (`mqtt.go:12054`), so a later save on the accept token does not split it and the
  permanent acceptance fact is never rolled back — but that save cannot confirm a
  conversion that had not happened when it ran, the requested-lifetime operation performs
  no save, `abortAcceptedResume` does not roll back staged drains, and `restoreDrains`
  installs delivery interest without committing staging.

  **What it needs:** a restore-side confirmed-persistence and completion contract that
  preserves outstanding exchanges before any teardown, with the drain's delivery terms
  derived from the durable and the owed exchanges rather than from the QoS 0 membership.

  **Acceptance:** a **cold** restore — not a resident session with an old subscription —
  of a record carrying `Subs` at QoS 0 and a live `Cons[sid]` whose durable holds an
  unacknowledged QoS 1 exchange must complete that exchange, with the QoS 2 variant
  reaching PUBCOMP, and must not issue a *premature* `CONSUMER.DELETE` (the delete after
  the last claim completes is ordinary retirement and is expected). The
  draining-not-allowed arm must still tear down, or the conversion is unconditional and
  proves nothing. Residual in `docs/CONFORMANCE.md` under the failed-adoption entry.

- [x] **A filter that fails after adopting a drain strands its durable.** **DONE — the
  first half landed 2026-09-09 (continuous naming) and the CONNECT restore route, which was
  the only reason this stayed open, is closed by the bullet immediately above.** Filed
  2026-09-06 by the delivery-readiness item's plan-review rounds 1 and 2. It was held
  unticked deliberately while a known message loss on the same population was open; that
  loss is now fixed, and the exclusions and residuals below are unchanged by its closure.

  **The defect was worse than this bullet originally said, and that is the run's most
  valuable finding.** It described stranding — a durable becoming orphan-sweep work. The
  real route needs **no crash and no filter failure**: between `adoptDrainLocked` and the
  packet's final `sess.update` the sid has a consumer and no membership, so the record
  builder names the durable **nowhere**; a later shared filter's early persist writes
  exactly such a record; and `sweepOrphanedConsumers` protects only sessions resident on
  the **sweeping** broker while keying eligibility on the consumer's **creation** time, so
  adoption starts no new grace. A sibling broker therefore deletes a live session's
  durable, with its started deliveries, on an interest-retained stream. Same-version peers.

  **What landed.** An adoption **retains** its drain object (`sess.adoptedDrains`) and the
  record builder names it in `Drains` exactly where it drops a `cons` entry for an absent
  membership key, on the **exact consumer identity** and from **current state, never packet
  history** — which is what makes duplicate filter occurrences, the QoS 0 exclusion and
  fork repair's adoption need no special case. Release is by **settlement** at a confirmed
  outcome, never by a cleanup pass, because a changed pointer is not evidence that disposal
  was authorized.

  `Drains` **omission became transition-owned**: the builder omits a drain only where an
  owned retirement transition has marked that exact object, and marking is gated on
  retirement **eligibility** rather than on the claim set emptying — an unresolved
  reservation may still belong to the durable, and once a save has omitted the entry a
  sibling sweep deletes it, which no local postponement reaches. The mark goes on before an
  **existing** owned save, so one save both clears the last claim and omits the entry and
  `finishDrainIfEmpty` writes nothing: a second, later save would have destroyed the
  claim-retry this tree has, because it runs after the source is already acked. While
  marked, the sid refuses through the shared hold, so the durable is never **reused** in
  the ambiguous window.

  **The post-adoption claim loss is closed too**, and it was two defects in one: the
  restored drain did not claim a sequence its ordinary consumer had started, so the
  callback's unclaimed arm acked the source away **and** the recovered register held its
  identifier for the life of the session with no delivery left to discharge it. A restored
  drain's persisted `Started` set is now a **floor**, enrolled from the **installed source
  mapping** on F-072's durable discriminator — possibly attempted is owed, proven-unsent is
  not — through the same three-way classification `stageDrain` uses, with eligibility at
  the callers because no single pending entry carries the phase. A SUBSCRIBE rollback
  completes that transition too, **cancelling** rather than skipping a parked allocation,
  since `confirmInflightRegister` checks the entry's identity and not drain membership.

  Uncertainty is **attributed per source**, so one durable's opacity no longer withholds
  another's messages or blocks its retirement, with a session-wide restriction only where
  attribution cannot be proven — which needed one new fact, the walk whose **apply phase
  completed**, rebased per incarnation because `clear()` preserves the walk counter while
  re-minting. And `stageDrain` gained a third disposition for "no demonstrated claim,
  completeness unknown", which installs an **empty** drain instead of destroying the
  consumer.

  **What it does NOT claim.** The CONNECT restore route (its own bullet, above). The live
  delivery path after a failed adoption, which keeps delivering fresh messages under the
  failed filter's terms — pre-existing, not widened, and pointed at the
  closed-but-present-subscription bullet. A never-transmitted but possibly-attempted
  delivery re-offered once with DUP=1, which is F-072's accepted conservative window rather
  than a new residual. Whole-record refusal of an unsupported persisted drain shape, still
  unbuilt. And one residual this repair introduces: an already-empty reference whose removal
  write definitely failed is retried only by the next resume, so a connection that never
  resumes can hold that reference — a retained reference, never a lost message.
  Residuals in `docs/CONFORMANCE.md`; mechanism in `docs/ARCHITECTURE.md` under "An
  adopted durable is named continuously (I8)", "A restored drain's claim set is a
  floor", "Uncertainty is attributed per source" and "Staging has a third
  disposition".

- [x] **An already-started QoS 2 exchange is retransmitted at QoS 1 after a QoS 1
  re-SUBSCRIBE.** Filed 2026-09-06 by the delivery-readiness item's plan-review round 4;
  **CLOSED by making an exchange's QoS a property of the EXCHANGE.**

  **What landed.** The QoS is carried on the pending entry and read in the send section,
  under the same `sess.mu` acquisition that admits the send, **unconditionally** — there
  is no branch on whether the delivery is a retransmission, because the contested case
  that must take the entry's value (a same-source replacement, which preserves a QoS the
  caller never supplied) arrives with `isNew = true` and `started = false`. It is sourced
  only from per-exchange evidence, and the list is exhaustive: the QoS the register landed
  holding, the register's own QoS on recovery (`mqttRecoveredPending`, which is where it
  was being **dropped**), or — for a retained send, which has no register — the QoS
  selected at allocation. Zero means no evidence and is a **fail-closed withhold**, tested
  before the send is reserved so the return leaks no reservation. Both directions are
  fixed: the downgrade no longer abandons a QoS 2 handshake, and the upgrade no longer
  drives a PUBREC whose slot transition the CAS refuses.

  **The same-source replacement preserves the occupant's QoS** (`rec.QoS = cur.QoS`,
  assigned in the arm because the record is built once before the retry loop), and
  confirmation threads the QoS **as landed** back onto the entry on **both** success
  paths. The reconciliation path passes the record that actually exists, not the one the
  caller built: a nonce identifies the *allocation*, never an immutable body, and
  `markInflightRegisterCancelledPending` writes a sparse record carrying the same nonce
  with a hardcoded QoS 1.

  **The three drain PUBREL gates now use per-exchange evidence.** A re-SUBSCRIBE can
  lower a drain's `qos` below the QoS of exchanges started under it, so gating on `d.qos`
  meant a QoS 2 exchange past PUBREC inside a QoS 1 drain never had its PUBREL
  **recreated** — loss. One session-aware predicate serves all three sites: `d.qos >= 2`,
  or a persisted past-PUBREC intent, or a recovered PUBLISH entry whose register says
  QoS 2, identity-matched on durable and sequence.

  **`d.qos` can never stand in for an exchange's QoS**, and that is not a stylistic
  point: delivery QoS is `min(published, granted)`, so a QoS 1 publication through a
  QoS 2 subscription is transmitted at QoS 1 and one drain routinely holds both. An
  earlier revision proposed exactly that override and it would have reintroduced the
  defect.

  **Two corrections to this bullet's own analysis, each verified against the code.** The
  upgrade direction does **not** produce an unreadable record — `casInflightSlot`
  validates the mutated result before writing — so its harm is an **abandoned handshake**,
  not corruption. And the register's QoS is **not** immutable today: the same-source
  replacement arm rebuilds the whole record, which is the window the preservation above
  closes.

  **What it does NOT claim**, each deferred to its own bullet below rather than accepted:
  a QoS 1/2 → **QoS 0** re-SUBSCRIBE, which is teardown-shaped and destroyed outstanding
  exchanges outright — **since CLOSED for a live SUBSCRIBE by converting the consumer into
  a drain**, with the CONNECT restore path still deferred; the **replacing allocation's
  own lifecycle** under concurrent
  cancellation or failed-store settlement; and shared-subscription QoS 2, which branches
  to the affine move before the identifier is allocated and is capped at granted QoS 1
  for the alpha. Residuals in `docs/CONFORMANCE.md`; mechanism in `docs/ARCHITECTURE.md`
  under "An exchange's QoS is carried, not recomputed".

  **Two things this closure settled that its own filing had wrong, recorded here because
  they were nearly lost as orphaned text.** The population was **not drain-only**: the
  identical mismatch was reachable through the plain existing-consumer refresh with no
  drain involved, since a single-filter QoS 2 -> 1 re-SUBSCRIBE over a transmitted,
  unacknowledged exchange set `sub.mqtt.qos = 1` while the callback recomputed
  `min(h.qos, sub.mqtt.qos)` per delivery and `trackPublish` returned the existing
  identifier with `isNew = false`. So the duplicate-filter route was a new route to an
  existing population rather than a new population, and the fix had to cover the plain
  refresh rather than the drain adoption alone — which is what landed. And the protocol
  question was **settled from fetched specification text, not recall**: [MQTT-3.8.4-3]
  requires the replacement subscription to be accepted without interrupting the flow of
  publications, [MQTT-4.3.3-1] makes an in-flight QoS 2 message categorically a QoS 2
  message, and [MQTT-4.4.0-1] binds the identifier to the exchange. **The subscription's
  maximum is an input to the FIRST transmission and to nothing else.** (An earlier draft
  cited [MQTT-3.10.4-3], the UNSUBSCRIBE rule; the hook for a re-SUBSCRIBE is
  [MQTT-3.8.4-3].)

- [x] **A QoS 1/2 → QoS 0 re-SUBSCRIBE destroys outstanding exchanges.** Filed 2026-09-07
  by the exchange-QoS item, and deferred out of it by an explicit scope ruling rather than
  accepted. **Pre-existing at HEAD and not worsened by that change**, which cannot reach
  this path at all: the callback's QoS 0 arm is taken *before* the send section, so a
  started exchange whose subscription is now QoS 0 never reaches the entry read. The
  mechanism is **teardown, not transmission** — the path closes delivery, purges pending
  exchanges and registers, and deletes the consumer (`stageDeferredDeleteLocked`) — so
  widening the exchange-QoS fix is not the shape of a repair, and moving the callback's
  early return below `trackPublish` does not solve it.

  **Distinct from two siblings, deliberately.** The revocable-downgrade item is about ONE
  packet carrying QoS 0 then QoS > 0 for the same filter, a different population that does
  not own this one; and the `Cons`-on-membership-presence bullet is about persisted
  consumer *references*, not about destroying live exchanges. Ownership must stay separate.

  **Acceptance:** a QoS 1/2 exchange transmitted and unacknowledged, then a single-filter
  re-SUBSCRIBE at QoS 0, must not lose the exchange — and the QoS 2 case must still reach
  PUBCOMP. The `docs/CONFORMANCE.md` entry describing the limitation links to this bullet
  rather than owning it.

  **CLOSED for a LIVE SUBSCRIBE, by converting the consumer into a DRAIN.** The scope
  limit is in the heading rather than a footnote: the CONNECT **restore** path carries the
  same loss and is deferred, below.

  **What landed.** The QoS 0 occurrence stages a **conversion candidate** rather than a
  plain teardown when the stream permits draining at all (`drainingAllowed`, evaluated
  once per packet), and at the packet's **final-update boundary** —
  `updateWithDetailConverting`'s in-lock hook, inside the same `sess.mu` section that
  applies the membership — `convertDowngradeCandidatesLocked` moves the consumer into
  `drains[sid]`. So one record carries `Subs` at QoS 0 and the drain together (I4), and
  `Cons` names it nowhere. The started exchanges retransmit under their original
  identifiers and a QoS 2 one past PUBREC completes through the drain's existing PUBREL
  machinery, which is created-blind and needed no change. Fresh messages reach the client
  once, at QoS 0, from the raw subscription while the drain discards its copy.

  **Four details are load-bearing, each established by a review round.** The
  not-yet-transmitted allocations are cancelled at the FILTER's staging, not at the
  boundary, because `beginSendLocked` never inspects `sub.mqtt.closed` and a callback
  already past the barrier would otherwise confirm and transmit under a removed
  membership. The transmitted registers are **not** voided for a candidate: a
  never-transmitted verdict on an exchange about to be drained is a lie that any save in
  the packet makes durable, and unvoiding cannot retract it. The durable's **live**
  withheld markers are transferred around `stageDrain`, which owns `sess.deferred` and
  drops what it cannot itself discover — including a cap-withheld marker that never had a
  registration, whose loss would have been a **regression this path introduced**, since
  the old teardown performed no such drop. And the confirmation is one operation reached
  inline or from the resolver via `retainOnLanded`, which reopens the delivery the staging
  closed; a detached client reopens nothing and that is normal.

  **The rollback's resource enumeration needed the fifth detail.**
  `rollbackSubscribePacket` discovers the consumers a packet created by walking
  `sess.cons`, and a converted one has left that map — so a definite save failure
  **un-converts first**, and without that a durable this packet created is neither
  restored nor released and the rolled-back record names it nowhere. The population is
  reachable and the test forces it: `mqttSubsBarrier.wait` waits only for SUBSCRIBEs
  already in flight when it sampled, so a publisher that sampled earlier lands its store
  after the packet's first occurrence has created its `DeliverNew` consumer.

  **What it does NOT claim:** the CONNECT restore path (deferred, see the failed-adoption
  bullet above); a stream-imposed consumer inactivity limit, where the destructive
  teardown stands and is logged; a send already admitted past the delivery barrier;
  crash-safety for a withheld marker, which is memory-only; and any narrowing of the
  existing confirmed-register-versus-transmission residual, which stays F-072's -- now
  CLOSED, so that prerequisite has landed (the unsent marker distinguishes a confirmed
  register from a transmitted one). Residuals
  in `docs/CONFORMANCE.md` under "A QoS 0 re-SUBSCRIBE drains rather than destroys";
  mechanism in `docs/ARCHITECTURE.md` under "UNSUBSCRIBE drain".

- [x] [F-099: Recovery classification follows adopted slot state and ordered PUBREL
  evidence](findings/ha-arbitration.md#f-099-recovery-can-discard-a-pubrel-the-client-is-owed-from-a-stale-scan).
  **Alpha blocker, filed 2026-09-10** by F-098's plan-review loop and **DONE**. Filed as a
  lost message reachable at HEAD; a plan-review round found a SECOND live defect in the same
  function, and the item widened to the one property covering both.

  **The property.** Recovery routes every slot by the state of the body it actually
  adopted,
  and reads per-exchange evidence after the register it judges.

  **Arm A, evidence ordering — a LOST MESSAGE.** `scanPubRelRecords` ran BEFORE the
  register
  enumeration and `findExactPubRel` closed over that slice, so an absence could predate the
  record it ruled out. Three consumers read it: route B (`inflight_register.go:6411`), the
  completion verdict (`:6945`) and the retired-ready custody test (`:6955`). A peer that
  stored the PUBREL and re-phased the slot in between had its record reasoned away, the slot
  advanced to terminal and the record deleted — no PUBREL, no PUBCOMP, no report. The scan
  now runs after every batch of the initial register enumeration, which is the ordering the
  marker phase already stated for its own evidence (N-5.6) and which the PUBREL scan alone
  inverted.

  **Arm B, adopted-body routing — a RETRANSMITTED ANSWERED PAYLOAD.** The claim contest's
  moved-and-ours arm adopts the reread whole and its withheld arm intercepts only action
  states and terminal, so a slot re-phased to `pubrel_ready` during the contest came back
  OWNED into the PUBLISH population; the apply install has no state guard, and the
  pre-contest source verdict restored the `cpending` mapping too. `mqttSplitNewlyRetired`
  had stated this rule for the RETIREMENT flag since F-087 and its comment called the
  retransmission "the finding itself" — the PHASE half was missing, and at the plain-QoS 2
  site BOTH halves were. `mqttSplitAdoptedPastPubRec` supplied the phase half at both
  shared contests and the plain-QoS 2 one, which also gained the retirement half (since
  F-105 7a/7b the shared contests are deleted and the plain-QoS 2 contest runs for the
  slot-only class alone; the split still runs over the walk's live, retired and
  custody-candidate sets, so the property stands); the two past-PUBREC contests already
  conformed and are unchanged. The holistic review found the missing retirement half,
  which the item's own boundary rule ("phase AND suppression flags") had always required.

  **What it does NOT claim.** That `finished` rests on positive terminal authority — that
  is F-098's C-II, and taking it here would strand a SUPPORTED population: `mqtt.go:19832`
  sends the delivery ack unconditionally after `mqttTerminalizeOnPubComp`, including the
  indeterminate branch, so `pubrel_ready` + no record + complete source is reachable and,
  with the inference deleted, permanent under Receive Maximum 1. That population had no
  regression of its own and now has one. Nor that an absence is historical evidence (the
  scan is a forward cursor over retained records), that future stores are excluded, or that
  an owed reservation is always reaped — it is reaped only by a later resume proving source
  completion, so a lost or unknown source retains it.

  **Verification.** 6 plan-review rounds (7/4/3/2/1/0 findings), 2 design consults and 2
  holistic rounds; 9 tests and 19 mutations, each applied, confirmed failing and
  reverted. The mutations pin
  each of the three scan consumers independently, both shared contest sites independently,
  the between-batches scan placement, the suppression predicate
  (`mqttInflightSourceMayReoffer`, not `!sourceComplete`), the two wrong fixes for arm B
  (the pre-contest sink and an apply-site state guard), and the call-site inventory
  tripwire's own predicate. **The boundary is carried by those behavioral kills, not by
  static analysis**: an AST check that tried to prove per-site routing was defeated in five
  successive rounds and was reduced, on a review ruling, to a reviewed inventory of direct
  call sites that fails closed on an indirect reference and claims nothing more.
  Residuals in `docs/CONFORMANCE.md` under the terminal-advance section;
  mechanism in `docs/ARCHITECTURE.md` beside "The **scan does not filter**".

  **F-105 note (audited 2026-10-05; corrected 2026-10-06):** the closure stands; its
  phrase about Arm B's shared contests now records their deletion; see F-105's "Checked
  bullets resting on F-105".

- [x] [F-100: Resend reconciliation frees Receive Maximum capacity without client
  completion](findings/ha-arbitration.md#f-100-resend-reconciliation-frees-receive-maximum-capacity-without-client-completion).
  Filed 2026-09-10 by F-098's plan-review loop; **taken on its own**, which the dossier's
  dependency order allows ("F-099 and F-100 first if they are taken on their own"), since
  both are reachable at HEAD with no F-098 machinery.

  **The finding.** `resendPendingRedelivery` drops a pending entry whose `$MQTT_msgs`
  message is below the stream's first sequence or confirmed absent, "releasing its
  packet identifier and skipping its NAK" (`mqtt.go:14084-14119`). Recovery classifies
  the SAME population the opposite way and is right to: a **possibly-attempted**
  exchange whose source is complete or authoritatively absent is **RESERVED** —
  identifier held, no source mapping — "because the receiver may hold that identifier
  from an attempt whose PUBREC or PUBACK was lost" (`inflight_register.go:337-343`,
  `:7203-7218`). A Message Expiry TTL reap is one of the two `sourceComplete` producers,
  so this is not a rare cut. The harm is the freed **capacity**, not only the identifier
  — a later allocation of a DIFFERENT identifier transmits — which durable slot
  occupancy does not protect.

  **What is honest about the code, and why it is still a finding.** The release is
  rechecked under a final `sess.mu` and the entry is *condemned* rather than untracked
  when a send section began in between, and it carries a stated Spec5 [3.3.2.3.3]
  rationale: an entry whose message is gone can neither be redelivered nor PUBACK'd and
  would otherwise leak a slot for the session's life. That rationale is about the
  BROKER's inability to continue and is not evidence about what the CLIENT holds, which
  is exactly what recovery's proven-unsent / possibly-attempted split separates.

  **Acceptance.** Source absence frees capacity only for an exchange proven **unsent**
  (the marker present). A possibly-attempted entry stays reserved on this route as it
  does in recovery, so the two rules stop contradicting each other about one record.

  **What landed.** The reap now decides each candidate under one uninterrupted `sess.mu`,
  in three arms: an entry whose `(durable, sequence)` no longer matches what the scan
  classified is left alone (a packet identifier is not a delivery identity, and HEAD acted
  on the identifier); a **proven-unsent** entry is released; everything else is **RESERVED**
  — identifier and Receive Maximum slot held, out of the NAK set, nothing written. The gate
  is `provenUnsentReleasableLocked`, so this becomes its **fourth caller** beside the two
  Message Expiry paths and the drain's withdrawal, rather than a second predicate stating
  the same fact. The site's `condemnSendLocked` call was **deleted**, not kept: the
  predicate refuses an open send section under that same lock, so no continuation can be
  owed. And the pass's redelivery budget — `maxAck`, the not-NAK'd count, the in-flight
  PUBREL count — is now read under one lock **immediately before the arithmetic** instead
  of at the top of the pass, with the NAK set filtered there on the same identity;
  refreshing one term against a stale other would have re-created the overshoot the item
  exists to close, one code block later.

  **The one place it departs from recovery, deliberately.** The reserved entry **keeps**
  its `cpending` source mapping where recovery's drops it. Recovery runs pre-CONNACK,
  before any source interest exists; the reap runs after restoration, where a callback may
  already hold the payload, and with the mapping gone `trackPublish` would give that
  callback a **second identifier for one source**. The cost is that drain enrolment can
  claim a vanished source: the claim floor runs before `recoverDrains` and gets a pruning
  *opportunity* in the same resume, a claim staged by a live UNSUBSCRIBE after the reap
  gets none before the next one, and neither is a guarantee — that pass skips a
  barrier-unresolved durable, retains a claim on an indeterminate lookup, and restores what
  it staged on a definite save failure.

  **What it does NOT claim.** Not cross-broker authority: the predicate reads local cached
  state, plain registers bypassed the shared-owner check (since deleted by F-105 7a/7b),
  and a same-delivery replacement can
  rewrite a plain register under another allocation's nonce — so a peer can consume the
  intent and transmit while this broker still holds a proven-unsent view. That is **F-098**,
  and what holds here is only that *for the same observed candidate state, this pass's
  release condition is a strict subset of HEAD's*, which is predicate inclusion and not
  whole-execution dominance. Not class closure either: `oversizeDisposalAllowed` enforces
  the same distinction through the send token rather than this predicate, so the claim is
  four **callers**, not four edges of every capacity release. (A reserved identifier's hold
  was also not unconditional, because a consumer teardown's purge could end it; F-101 closed
  that, and the predicate now has six callers.)

  **Verification.** 5 plan-review rounds (8/7/3/3/0 findings) — round 1 reversed the
  source
  mapping decision, round 2 ruled that reversal correct, rounds 3 and 4 corrected claims
  the code did not support. 4 tests and 5 mutations, each applied, confirmed failing and
  reverted. The mutations pin the gate (M1), the unresolved-intent arm specifically (M2),
  the capacity recapture (M3), the identity fence (M5) and the fixture's reach into the
  route (M6). M2, M3 and M5 each kill exactly one test. **The capacity claim rests on M3**,
  whose oracle is a session resumed at Receive Maximum 1 holding two identifiers: with the
  recapture the pass NAKs nothing, without it the survivor is redelivered to a client whose
  negotiated maximum is one. Mechanism in `docs/ARCHITECTURE.md` beside the four-routes
  sentence; residuals and the two new findings in `docs/CONFORMANCE.md`.

  **F-105 dependence (audited 2026-10-05; corrected 2026-10-06 by WP-F W3):** the property
  this bullet closes is now enforced by F-105 code, so the closure is conditional on
  F-105's integrated gate. What replaced the mechanism its passages describe: the
  shared-owner check is gone (7a made shared QoS 1 ordinary; since 7b a claim stamp
  decides nothing). The Receive Maximum charge is the custody ledger's: capacity is two
  budgets (scheduling against MaxAckPending, and Receive Maximum over custody), and an
  ordinary exchange holds its charge and identifier until a durable ending or the
  generation's discard. `provenUnsentReleasableLocked` still gates the reap, so the
  property stands. See F-105's "Checked bullets resting on F-105".

- [x] **Two routes release a Receive Maximum charge without client completion.** **F-101,
  filed 2026-09-10** by F-100's implementation, which found them while closing its census.
  **DONE 2026-09-10.**

  **What was wrong.** `dropUnredeliverableRetained` dropped every pending retained QoS 1/2
  send on resume, and `purgeConsumerPendingLocked` deleted pending entries during a
  source-consumer teardown. Both released a packet identifier and a Receive Maximum slot on
  evidence that the BROKER could not continue, never that the CLIENT completed — the
  inference F-100 rejects. Two consequences, both client-visible: a later allocation could
  re-mint an identifier a QoS 2 receiver was still holding awaiting PUBREL
  ([MQTT-2.3.1-4]); and at the teardown site a conforming client's legitimate PUBREC then
  reached `mqttNewCorrelatedPubRel` with no exchange identity, which returns nil, so the
  handler rolled back and returned an error out of `mqttParse` — **terminating the
  connection**. That second one was not in the filed finding.

  **What landed, as two DELETIONS rather than two new mechanisms.** The teardown's purge
  and
  the void staging before it now both ask `provenUnsentReleasableLocked`, taking it from four
  production callers to **six**; a possibly-attempted entry is RESERVED — identifier and slot
  held, register left uncollected so the ladder cannot destroy the owner slot the exchange
  needs to complete, no settle parked — and its never-transmitted verdict is not recorded.
  `condemnSendLocked` is deleted from the purge, as F-100 deleted it from its own site. A
  retained send is released at its ABANDONMENT, the one site that proves no bytes were
  written, keyed on the frame's ALLOCATION nonce, and `dropUnredeliverableRetained` and its
  resume call are **deleted** — which also removes the window between a flusher's
  `queueOutbound` and its `endRetainedSends`, because resume now decides nothing. A staged
  verdict additionally FENCES its register in `beginSendLocked`: staging decides before a
  send, so without it a callback past the subscription barrier could transmit afterwards and
  make the verdict false.

  **Verification.** 3 plan-review rounds (6/6/4 findings) and 4 consults. The loop was
  declared **NOT CONVERGING** before round 4 — rounds 2 and 3 were dominated by
  caused-by-last-fix findings on one seam — and the escalation consult DELETED the mechanism
  those rounds kept re-cutting (a `mqttSendSettlePurgeReclassify` settle kind and a
  precedence relaxation), which dissolved three of round 3's four findings. 8 mutations, each
  applied, confirmed to build, confirmed failing and reverted; M8 reinstates the deleted
  resume drop and kills the end-to-end test, which is what shows the tests detect the
  ORIGINAL defect. Two of my own claims were refuted along the way: that a preserved entry's
  PUBREC could complete without its owner slot, and that the disposition of an open section
  keys on `wrote` rather than on the intent.

  **Residuals, in `docs/CONFORMANCE.md`.** A teardown overlapping an open send section
  reserves the entry in all four intent outcomes, so a later pre-write DEFINITE refusal
  **can** hold one identifier and one capacity charge despite proven non-transmission; a
  retained frame whose disposition never runs, or runs after its hold has been swept, keeps
  its identifier. Both fail-closed. The durable `Cancelled`-verdict writers other than this
  teardown's remain **F-088's**.

  **Amended by F-102 (2026-09-11).** The first residual holds for the LIVE teardown, which
  is what F-101 was about, and there is still no PROMPT reclamation — its actual acceptance
  criterion. But the hold no longer lasts the session: a later recovery over a source that
  can never be re-offered now RELEASES an eligible marker-present pair on the marker's own
  evidence, which is exactly the two intent outcomes that leave the marker intact. The
  original wording ended "and F-102 is untouched", which was true when written and is not
  now.

  **F-105 dependence (audited 2026-10-05; corrected 2026-10-06 by WP-F W3):** the property
  this bullet closes is now enforced by F-105 code, so the closure is conditional on
  F-105's integrated gate. What replaced the mechanism its passages describe: a retained
  send's capacity and identifier are ledger custody from its admission (7c), and an
  unservable replay is withdrawn when proven unsent or abandoned through the abandonment
  fact's retained arm (`outbound_retained_withdraw.go`, `outbound_retained_abandon.go`);
  every retained obligation stays owned until its settlement, a positive entitlement
  transfer or an effective connection close. The Receive Maximum charge is the custody
  ledger's. See F-105's "Checked bullets resting on F-105".

- [x] **A recovery reservation over a Lost source has no release edge.** **F-102, filed
  2026-09-10** by F-100's plan-review loop; **DONE 2026-09-11** after 4 plan-review rounds
  (8 / 7 / 3 / 2 findings, all folded, none refuted).

  **The finding.** An entry F-100's reap legitimately releases as proven-unsent leaves its
  durable pair behind. If that source's consumer is gone, **both** classifiers —
  `inflightSourceState` and the recovery batch cache's `resolve` — return **Lost**, not
  Complete, because deleting a consumer on an interest-retained stream deletes what it
  held. A Lost source never entered the consume arm, so recovery restored the entry
  instead: marker and source mapping present, **no ack subject**. The reap only scans
  entries that have an ack subject, so nothing re-examined it, and no consumer remained to
  offer the source. The client never received this exchange, so its acknowledgement was not
  a realistic release either. Not introduced by F-100: HEAD reached the same state from a
  strictly wider release.

  **What landed.** The consume arm now routes on *the source can never be re-offered* —
  `!mqttInflightSourceMayReoffer`, whose negation is exactly `{Complete, Lost}` and which
  F-099 already installed as that question's one definition — and the **unsent MARKER**
  decides the outcome. The marker is the license: its confirmed delete strictly precedes
  any write, so a present marker proves the PUBLISH never went out, and identifier
  continuity is owed only to an exchange the client may hold. A proven-unsent register is
  dropped with `mqttSlotDeleteNeverTransmitted`; a possibly-attempted one is reserved
  exactly as before.

  **The routing is a FLAG, not a classification lookup, and that is the item's real
  lesson.** `mqttRecoveredInflightReg.sourceComplete` was never a restatement of the source
  state: the classify loop assigned it only after a pure cancellation, an unproven affine
  register, a drain-AMBIGUOUS source, a **drain-OWNED** record and a retirement had each
  already `continue`d. Reading `srcCache.state()` in the reserve loop instead — which
  revisions 1 and 2 of the plan proposed — preserves every ordinary case and silently
  deletes all five exclusions, most visibly the drain-owned one, which nothing else
  excludes, and for **Complete** sources as well as Lost. The field is therefore replaced
  by a routed type `mqttRoutedSourceCleanup` whose zero means *not routed*, assigned at the
  same exclusion-preserving site and carried across the three claim-contest rebuilds.
  Mutation **M3b** is that defect, and it fails both drain-owned rows while every other
  test stays green.

  **Deliberately NOT changed.** The `cpending` mapping install still keys on Complete
  alone: a Lost reservation keeps its mapping, because a completed consumer lookup does not
  retract a payload already dispatched to a callback, and without the mapping that callback
  mints a second identifier for one source. `Live` and `Unknown` are untouched. The reap
  keeps F-100's *no durable write in any arm* property. `storeInflightRegister`'s
  fail-closed arm is untouched — it holds no marker proof for the conflicting delivery —
  so a stranded pair still refuses that identifier until a recovery pass deletes it.

  **Residuals, in `docs/CONFORMANCE.md`.** The release is **eligible** (the five
  exclusions)
  with **conditional** durable cleanup (freshness refusal, delete failure, or an unresolved
  terminal advance ending the pass before its drop loop — no crash required). A condemned
  open send keeps its capacity charge until `endSendWith` settles it. The release rests on
  a classified snapshot, not on confirmed durable relinquishment, and there is no
  within-generation fence against a concurrent sender — equally reachable through Complete
  after a TTL expiry, and F-098's rather than this item's.

  **F-105 dependence (audited 2026-10-05; corrected 2026-10-06 by WP-F W3):** the property
  this bullet closes is now enforced by F-105 code, so the closure is conditional on
  F-105's integrated gate. What replaced the mechanism its passages describe: the claim
  contest runs for the slot-only class alone since 7b, so for an ordinary register
  `mqttRoutedSourceCleanup` passes through the walk unchanged; the capacity charge is the
  custody ledger's, released only by an exact read of an ended phase or the generation's
  discard. See F-105's "Checked bullets resting on F-105".

- [x] [F-098: No cross-broker authority over an incarnation's in-flight
  identifiers](findings/ha-arbitration.md#f-098-no-cross-broker-authority-over-an-incarnations-in-flight-identifiers).
  **Alpha blocker, filed 2026-09-10**, and **ordered BEFORE F-097**, which is blocked on
  it,
  and therefore before F-088. Filed by F-097 unit 2's plan-review loop after **three**
  revisions offered three different release proofs and all three were dismantled.

  **CLOSED over the supported boundary by attempt 3 (2026-09-13).** An earlier revision of
  this bullet ticked it while two gaps were still open, and review established that closure
  was premature; both gaps are now closed and the box is ticked on that basis rather than
  on the earlier claim.

  **The two gaps, and what closing them meant.** **Cold-recovery occupancy**: the
  retired/source-complete classifier staged a `DeleteComplete` the record can never obtain
  authority for — `Retired` is backed by the client's own PUBREC, so arm 4 refuses it and
  `publish` is not a terminal phase — while the apply phase installed exclusion only inside
  the resident-`pendingPublish` branch, so a cold resume left the durable record standing
  with its identifier allocatable. The repair is NOT to restore the delete, which was itself
  contrary to C-II: the classifier no longer stages it, the source is still consumed, and
  the occupancy obligation is carried into apply **independently of any resident entry**.
  **`endSendWith`'s deferred settlements**: its non-empty branch untracked after clearing
  the send token without handing off exclusion, so for a denial or a recovery drop the
  untrack was the moment the identifier became allocatable over a slot nobody had confirmed
  gone. Exclusion is now installed under the same lock for the settlements whose durable
  delete can be refused, fail, or never be issued; the two acknowledgement kinds are
  excluded because
  each holds the identifier through its own terminal cleanup, and registerless retained sends
  were exempt because they had no durable slot for an exclusion to be about (since F-105
  7c a retained send admits a register, and the exemption is gone).

  **The one exception, and it is normative.** The experimental affine disposer's
  NON-RESIDENT arm remains independently licensed and mints no authority — asserting
  proven-unsent for a retired record would be false. Its RESIDENT arm routes through the
  consolidated reaper like every other release, so the exception is **not** uniform across
  both arms and an earlier claim that it was is withdrawn. Its consolidation and
  experimental fallout belong to section P.

  **What the consolidation delivers.** Each release-class disposition names the authority
  that licenses it, through one total table with a refusing default, and the authority
  travels as a value bound to the observation that produced it — subject, exchange
  identity and sequence — so an authority computed for one observation cannot be spent
  on another.
  Replacement is deliberately outside the release class: it is bound to the transmission
  identity but installs a successor rather than freeing an identifier. One extracted
  confirmation, lifted from the copy the terminal recovery already carried inline,
  re-reads the subject after a delete and partitions it four ways, because an
  exact-sequence delete
  reports success for an already-absent sequence and a same-identity rewrite is
  indistinguishable from a removal.

  **Exclusion is not entry retention**, which is the distinction three earlier revisions
  conflated and review dismantled each time. Every removal site removes exactly what it
  removed before, and hands the IDENTIFIER's exclusion to a sixth liveness term on the
  send hold that reserves the identifier and charges no capacity. Because all six
  hold-deletion
  sites already ask the shared liveness predicate, the exclusion is effective at every one
  of them by construction. It is a conjunction of binds, never promoted from
  unidentified to named, liftable only by a proven absence of every bound subject at the
  instance observed, and adoptable only by the exchange that owns it.

  **Two HEAD defects were found and fixed on the way.** **F-103**: the evidence loader
  admitted a STAGED cancellation verdict as a persisted one, handing the release
  authority a proof of non-transmission no restart can see. **F-104**: the verdict was
  retired after a delete that removed nothing, leaving the surviving register with
  neither its verdict nor necessarily a marker — permanently unreleasable. F-104 had
  TWO bypass sites, and the second was found by review rather than by me; a
  `forgetCancelledReg` family sweep then found six call sites in all, of which four are
  licensed by a tombstone write or by lost arbitration and are unchanged.

  **Residuals widened**, in `docs/CONFORMANCE.md`: identifier exclusion outlives the entry
  and its reclamation is deliberately incomplete, with four named cases that need
  neither a crash nor a peer. A withheld PUBREL record is one of them, and its
  population is a persisted affine reference this broker does not generate — failure
  containment rather than a shape normal operation produces. Storage is proportional to
  the accumulated distinct restrictions, not to the identifier space.

  **Prior state, kept because the next reader needs it.** Attempt 2 (2026-09-11) landed
  most of the unit and left C-IV's consumption open — that half is what this item closed.
  It froze its plan after one design consult and two plan-review rounds, then
  implemented. What landed:

  - **C-I, transmission identity.** The record carries `Acq`, advanced by every action
    acquisition and never reset; the authority identity is `(subject, Nonce, Acq)` and
    every destructive disposition CASes on the whole triple. `casInflightSlot` enforces
    the counter invariant in one clause — a rewrite leaves it alone or advances it by
    exactly one, which refuses a decrease, a jump and an overflow together — and the
    gated reap enforces it independently, because it deletes the MESSAGE rather than
    rewriting the record so the CAS's comparison never runs.
  - **A durable PUBLISH action.** The transmission's own hold is now on the record rather
    than in process memory, which fenced no peer. Acquired before admission so the size
    refusal is fenced too; the drain preserves the observed phase and never resets `Acq`.
  - **`publish_terminal`**, so terminal authority is a fact on the record instead of an
    inference from a missing PUBREL. A failed advance WITHHOLDS the release.
  - **`pubrel_intended`**, a monotone phase written BEFORE the PUBREL store, so a crash
    between them leaves a slot no peer may reap. A refusal aborts the whole PUBREC.
  - **C-IV's release-authority function**, a total function with a reserving default, and
    the shared evidence loader behind it. **Consuming it is partly done**: the
    source-complete occupant reap now obeys C-II, and the gated reap enforces `Acq`. The
    remaining destructive sites still decide for themselves.
  - **L4's `Rel`**, the drain's positive identity-bound release request, with all four
    preservation edges and recorded-and-confirmed before the delete.
  - **L1, in full.** The owed record is rebuilt from the slot alone — no application
    payload is needed — an already-landed one is bound rather than duplicated, and binding
    goes through the one readback-proven arrow. Recovery enrols a recovered
    `pubrel_intended` slot after taking custody, and a PUBREC's intent outcome is
    CLASSIFIED (confirmed, definitely not recorded, or unresolved with an identity-bound
    resolver), so a slot's mere existence is never read as proof of intent.
  - **A continuation for every exclusion**: a stored PUBREL refused behind a live
    transmission leaves a per-record waiter that the transmission's end wakes, on both of
    its halves (the durable action and the local `pubIdent`, which had no writer at HEAD).
  - **`publish_terminal` recovery**: its own branch at resume and at store time, and a
    boundary that refuses to build a sendable entry from a terminal record.
  - **Graceful shutdown finishes its own obligations**: every PUBLISH send section is
    joined, and every unresolved drain and terminal cleanup is finished while JetStream
    still answers. An acquisition whose response was lost is armed before its CAS and
    adopted for cleanup, so it is recorded too.
  - **One admission rule at every destructive edge**: phase, the C-I counter, and C-IV
    arm 2 — any live action withholds, ours included — shared by the loading gate and the
    record-in-hand delete, with a CLASSIFIED outcome. A drain release is bound to its
    persisted `Rel`, and a superseded request retires only itself.
  - **An acknowledgement's durable half is never dropped**: the identifier stays held (the
    capacity does not) until `publish -> publish_terminal -> delete` is confirmed, on both
    settlement paths, with a continuation bound to that transmission's identity.
  - **A drained acknowledgement waits for its section**: the drain's finalize releases the
    same register the acknowledgement's delete is parked on, so it is parked with it
    rather than persisting a release request for a register the writer still owns. A
    finalize that loses the record fence retries promptly, because the client acknowledges
    once.
  - **Every release request settles against its own target**: one naming another register
    is settled — through the same identity-bound release, rewrite loop included — BEFORE
    anything at the current subject is read, and one that survives a restart is enrolled
    at resume rather than waiting for a source delivery that an expired source or deleted
    consumer makes impossible.
  - **Recovery reserves what it cannot finish, identity-bound**: a `publish_terminal`
    record whose delete fails is held by the same terminal hold an acknowledgement
    installs, so the confirmed delete lifts it — not by an opaque entry, which withheld
    the session's whole identifier space with nothing to undo it.

  **What is NOT delivered, and why the box stays open.** C-IV's CONSOLIDATION. Every
  destructive edge now applies one admission rule, but the release DECISION — what licenses
  a release in the first place — is still made at several sites in their own terms rather
  than by the authority function. That consolidation is the open half. Holistic review
  Holistic review found six defects on the assembled change in round 1, three in round 2,
  two in round 3, three in round 4, four in round 5, five in round 6 and four in round 7.
  Rounds 5 to 7 kept finding the same seam — the identity an acknowledgement's cleanup is
  bound to — so that mechanism was revised once, on a design ruling, rather than patched
  again: one monotone resolution cell per exchange now settles it.

  **Two residuals were filed rather than papered over**: the availability residual widens
  from the PUBREL path to every outbound QoS 1/2 transmission, and a possibly-transmitted
  PUBLISH with no deliverable source holds its identifier.

  **The finding.** A resume PRESERVES the incarnation generation, so two brokers may
  legitimately operate the same subjects at once, and nothing bounds that within a
  generation. The retirement watermark is per-generation and says nothing about a live one.
  F-036's incarnation completion lease is **process memory** and says so
  (`sess_lease.go:33-45`: "All of this is process memory. No persisted shape changes"). And
  the check that looks like a fence is not one: `mqttProcessPublishReceived` tests
  `sess.c == c` under `sess.mu` (`mqtt.go:19120-19126`) and then RELEASES THE LOCK before
  its durable work, so a PUBREC handler admitted before a rebind continues across it; the
  takeover tail closes the displaced connection asynchronously
  (`sess_takeover_redrive.go:228`); cross-broker displacement rests on the record watch
  and a periodic backstop. **Pre-CONNACK is not a fenced moment either** — it stops the ARRIVING
  connection processing further packets and joins nothing.

  **Why it is a finding and not a preference.** Releasing a packet identifier — or
  declining
  to reserve one, which is the same authorization, since `bumpPI` then mints it — requires
  establishing that no client-facing obligation stands at it. Every attempt to establish that
  from durable state alone goes stale between the observation and the act. **Three proofs,
  three schedules, all refuted, and they are recorded in the dossier so they are not
  re-attempted:** the register's ABSENCE (the drain removes a register while the exchange
  is still `publish`, `unsub_drain.go:1363` and `pubrel_action.go:1329-1344`); NO STORED PUBREL
  AND NO PERSISTED INTENT (the scan is not a snapshot, `sess.mu` is process-local, and
  `pubrel_terminal_draining` and the drain's persisted intent at `unsub_drain.go:2354` are
  further carriers); and AUTHORITY AT REUSE (retained sends allocate with no register
  consulted at all — `mqtt.go:11138` to `trackPublishRetained:13406` to `bumpPI:13247` to
  `beginRetainedSendLocked:5159` — and releasing an identifier frees **Receive Maximum
  capacity**, so a later allocation of a DIFFERENT identifier transmits while the released
  exchange still stands). The same staleness defeats a repair at the allocation site: a
  PUBREL can be stored without changing the register, so an exact-sequence delete succeeds
  against a record that has just acquired an obligation, and that cross-stream window is
  supported behaviour (`pubrel_action.go:1213-1229`) which no atomic batch closes, batches
  being **per stream**.

  **The population involves no non-conforming client.** [MQTT-4.3.3-9] requires a receiver
  that sent a FAILED PUBREC to treat a subsequent PUBLISH under that identifier as a NEW
  Application Message, which it may accept with a successful PUBREC, and the client cannot
  see the broker's lineage. So a failed PUBREC under lineage L, a lost source
  acknowledgement, a same-source replacement inheriting L (`inflight_register.go:3998`) and
  a successful PUBREC on the heir leave completion evidence for L coexisting with a LIVE
  obligation under L. The broker's retransmission after a failed PUBREC is itself in tension
  with [MQTT-4.4.0-2]; that is a broker-side problem, not a non-conforming client.

  **Acceptance** must supply enforceable exclusion or handoff over all three competing
  operations — inherited PUBLISH transmission including the retained path, terminal
  completion and its durable cleanup, and PUBREC/PUBREL advancement including the
  store-before-phase window and the drain's persisted intent — and must cover **recovery,
  allocation and live release**, not only a live reconciler. **A further durable-state
  observation is not a candidate**: that is what the three refuted proofs have in common.

  **A planning run on 2026-09-10 produced no code and ended on a ruling.** Three
  plan-review rounds and two design consults; 21 findings. It is recorded in the
  dossier's "Where the next attempt starts", which is now REVIEWED rather than
  speculative, and the next attempt starts there rather than at the top. In short:
  the course is settled — one unit, extend the slot state machine, the PUBREL intent
  is a MONOTONE PHASE, retained sends get a durable SOURCE-LESS slot, and the
  session-wide ownership permit is rejected on **cost** (its failure domain is the whole
  session) rather than on the impossibility claim that was withdrawn as wrong. **Three
  contracts were reviewed and ruled to CLOSE their classes** — C-I transmission identity
  (`Acq`, a monotone acquisition counter never reset by a drain, with evidence bound at
  the handler rather than fetched), C-II source completion never discharging a client
  exchange across four enforcement edges, C-III universal custody. **Three did not**, and
  they are where to resume: **C-IV** must be about admissible PROOFS OF NON-TRANSMISSION
  with an explicit precedence rule, not about marker presence, because `publish` +
  `Cancelled` + no marker + no token matches no outcome and HEAD already accepts pure
  cancellation as a proof (`inflight_register.go:6578`); **L1** needs a guaranteed
  materialization continuation, since re-enrolling "at the next recovery" strands a slot
  under a healthy connected custodian; and **L4** is unsolved, with the natural fix
  refuted — persisting the drain's claim removal before the release does NOT work,
  because a removal is an OMISSION (`unsub_drain.go:324`) and the restart is then back to
  inferring authority from an absence. Two further things are not to be re-attempted: a
  fresher CAS base as arbitration, and an enumeration of carriers as a rule.

  **F-105 dependence (audited 2026-10-05; corrected 2026-10-06 by WP-F W3):** the property
  this bullet closes is now enforced by F-105 code, so the closure is conditional on
  F-105's integrated gate. What replaced the mechanism its passages describe: since 7c a
  retained send has a register and is a custody-ledger exchange, so the registerless
  exemption no longer exists; the finding's own account of retained allocation is kept as
  history. See F-105's "Checked bullets resting on F-105".

- [x] **Prompt reclamation of a held identifier exclusion.** Filed 2026-09-13 by F-098's
  C-IV consolidation, which retained **exclusion completeness** and deliberately deferred
  **reclamation completeness**. NOT an alpha blocker: an identifier held too long is an
  availability cost, where an identifier exposed while an exchange still owns it is a lost
  or duplicated message. **Done**, in `internal/mqtt/exclusion_reclaim.go`, after four
  plan-review rounds and the holistic review.

  **A trigger, not a new proof.** The three existing lift routes are each an edge some
  *other* operation happens to cross, so an exclusion no such edge reaches again was never
  re-examined — and `bumpPI` skips a reserved identifier, so accumulated exclusions
  eventually cover the space and stop QoS 1/2 delivery outright. The pass re-attempts the
  SAME proof on a bounded cycle while connected: `confirmSlotReleased` per bound
  subject, a
  lift only on `mqttReleaseEstablishedAbsent`, through the same
  `liftIdentifierExclusionLocked` and the same capture-before-read instance protocol. It
  writes nothing durable and acquires no authority. **The install is the arm** —
  `excludeIdentifierLocked` schedules it at its single exit, so all 27 install sites are
  covered by construction — and `addSession` re-enrols it, because nonresidency is NOT
  terminal and a restored object would otherwise come back with exclusions and no timer.

  **Three of the five disclosed classes are reached**: the landed delete whose
  confirmation
  failed, the handoff installed after its successor finished, and the drain that retired
  from a non-completing phase (once its record goes). The **withheld PUBREL** and the
  **retired source-completed register** are not, and that is the correct direction in
  both:
  the record stands, so no absence is provable. **The pass polls, with no eventual-success
  proof** — it cannot make a record go away — and the capped retry delay is not a capped
  reclamation latency. Both disclosed in `docs/CONFORMANCE.md`.

  **Two fences were proposed and refuted before the right claim was found**, and the next
  reader should not re-propose either. The instance counter is NOT a cross-broker fence: it
  advances only inside one session object. "Only the connection owner can create" is FALSE:
  `markInflightRegisterCancelledPending` (`inflight_register.go:4400`) is create-only
  and a displaced writer reaches it. What holds is narrower — a record's existence does
  not put an
  identifier in the client's hands, a TRANSMISSION does, and neither post-absence creator
  describes an outstanding client exchange (the tombstone is `Cancelled` by construction,
  the allocation create is untransmitted and `beginSendLocked` requires connection
  ownership). The three pre-existing lift routes inherit the identical window and are sound
  for the identical reason, which is why no HEAD defect was filed against them.

  **F-104's reclamation half, done as a defect fix rather than an enumeration.**
  `markInflightRegisterCancelled` now returns a classified `mqttRegCancelOutcome` with a
  refusing zero value, so each ending names its own licence instead of both callers
  asserting "the tombstone is on the register itself" — true of two arms of five. The real
  defect was next to it: `abandonForeignSharedInflightRegister` forgot the nonce verdict on
  claim INEQUALITY alone, while a same-nonce, non-cancelled record still stood. It now
  retires only against a **strictly newer** foreign claim and fails closed on older, equal
  and zero. **The decision lives inside that boundary**, covering all FOUR of its
  callers —
  two propagate the refusal, two discard it, and Go permits discarding an error.

  **Eighteen mutations were run and all eighteen kill.** Two are worth carrying: the naive
  single-exit rewrite of `excludeIdentifierLocked` (a `break` falling through to the
  unconditional append) duplicates the bind AND advances the instance twice, which the
  stale-instance test cannot see; and a cursor expressed as an INDEX into the sorted set
  first SURVIVED, because the fixture's set was smaller than the read budget and one pass
  visited everything whatever the cursor meant. The oracle now asserts where the next pass
  RESUMES. The install-side saturation guard was also a HEAD gap: the unique-bind tail
  incremented unguarded, so the installer could wrap where the lift could not.

  **F-105 dependence (audited 2026-10-05; corrected 2026-10-06 by WP-F W3):** the property
  this bullet closes is now enforced by F-105 code, so the closure is conditional on
  F-105's integrated gate. What replaced the mechanism its passages describe:
  `abandonForeignSharedInflightRegister` was deleted with 7b's foreign-claim arms, and
  F-104's strictly-newer rule now lives only in the slot-only claim contest. The custody
  ledger, not the exclusion lift, is the allocator's gate (`bumpPI` consults `holdsPI`;
  the lift still lifts on established absence,
  `TestF105CustodyAllocatorSkipsALedgerHeldIdentifier`), so accumulated exclusions no
  longer decide availability alone. The install-site count above is historical. See
  F-105's "Checked bullets resting on F-105".

- [x] **F-105: Removal of the last durable representation must not leave a peer entitled
  to serve the obligation.** **Filed 2026-09-14** by F-097 unit 2's attempt 2, on the
  ruling of escalation consult C2 after release-on-absence was refuted twice on two
  different premises.

  **The contract.** Establish that removal of the last durable representation cannot leave
  a peer entitled to serve the outstanding obligation, **or** preserve positive evidence
  sufficient to discharge that exact obligation. Narrowing the release authority to the
  unsent marker does **not** establish this, and neither does any observed absence.
  It must address **teardown, peer liveness, and removal/replacement ordering** together.

  **Completion of its SUPPORTED integrated contract is NECESSARY for the withheld F-097
  reconciliation, and is never automatic approval of it.** "Supported" is the same scope the
  integration gate below states and F-106's holistic-review condition preserves; it does not
  include the affine completion work chapter P now owns, and naming it does not
  manufacture a smaller independently shippable F-105 or F-106.

  **The two counterexamples, preserved verbatim so the premise cannot later be weakened.**
  `reapIncarnationRegisters` (`inflight_register.go`) deletes ordinary PUBLISH registers
  with neither terminal nor marker evidence, while `sess_teardown.go:929-936` states that
  "a peer may still be serving this incarnation and holding registers under these very
  subjects". F-036 does not gate it: its licence is for a **bound session record** and says
  absence alone is NOT the licence, and the adopted teardown deletes its targets **before**
  releasing the session record (`sess_teardown.go:1138`), so the record-absence conjunct is
  not even available. Separately, the incarnation completion lease (`sess_lease.go:33`)
  drains local owners and lives in **process memory**, so it cannot revoke a peer's
  entitlement.

  **"No peer entitled" must never be reduced to "no peer observed alive."** A renamed
  liveness check is the same refuted design.

  **Ticked in the W3 candidate; NOT accepted as closed until the integrated branch review
  passes.** WP-F, the last remaining work, is delivered in that candidate (W1 `868990c`,
  W2 `522d25d`, W3 this change; see "Delivered in the W3 candidate" below). The history
  follows. The design converged after four plan-review rounds and
  eight Codex consults in the PLAN lane, with the conversion lane's own numbered
  separately. The first substrate landed as commit `2992f9c`, which is a CHECKPOINT
  and explicitly not a delivery.

  **Cutover checkpoint state (row 3, the completion flip).** All three acknowledgement
  arrows that end a PUBLISH now advance the durable exchange in production: admission
  (`admitInflightPublish`), the SUBSCRIBE source-name claim (`opProvisionResource`, now
  re-attempting on authority CAS contention), and the acknowledgement — a PUBACK to
  Terminal, a refused PUBREC to Rejected, a successful PUBREC to PUBREC-observed —
  advancing the exchange in the same batch that re-phases its slot (`opAdvanceExchangeAck`,
  reached through `advanceExchangeAck` from the terminal arm in
  `attemptTerminalRegisterRelease` and from `intendPubRelSlot`). The durable core landed at
  WIP checkpoint `a759d23`, the PUBACK/refused-PUBREC wiring at `0bce8ed`; the
  successful-PUBREC arrow (in-line bounded re-attempt, Confirmed on an acknowledged batch)
  is the increment on top of it. One class gate (`ackExchangeFor`) serves every arrow:
  affine slots are slot-only (shared ones were too, until 7a made them ordinary), an
  ordinary slot advances with its exchange, and an
  ordinary slot WITHOUT its exchange is refused — the row-3 checkpoint's "explicit absence
  means a pre-F-105 register" route is deleted (consult 20: it served only test fixtures,
  which now admit through the authority). Because the PUBREC advance makes the standing
  authority a writer that a resuming CONNECT contends with, the CONNECT ownership steps now
  reconcile a lost or refused attempt from its transition handle before repeating it
  (`reconcileOwnershipStep`, consult 21), and the PUBREC advance itself re-attempts up to
  eight times with jittered spacing (consult 22).

  **The PUBCOMP arm (this checkpoint's increment).** PUBCOMP now ends an ordinary QoS 2
  exchange durably: `pubrel_ready -> pubrel_terminal` with `PubRecObserved -> Terminal`,
  or `-> TerminalPendingDrain` while a PUBREL action is live, whose holder's drain then
  clears the token and advances to Terminal in one batch (`opCompleteExchangeDrain`). The
  phase value PUBREL-owed is retired and refused (it stated nothing PUBREC-observed does
  not; consult 23). The canonical PUBREL is no longer acknowledged before its completion
  is recorded: an unconfirmed PUBCOMP hands the withheld ack to a completion worker that
  owns its record alone and is also the single owner of every unsettled terminal cleanup
  of an ordinary exchange (consult 24, a terminal fold against a frozen caller census).
  Recovery no longer finishes an ordinary `pubrel_ready` slot on an absence; it finishes
  one only beside a durably Terminal exchange — the shape MAXMSGSPER-REVIVAL leaves, now
  reproduced after a clean shutdown as well (consult 26; `docs/known-upstream-issues.md`).
  A drained PUBREC whose intent landed before its exchange advance is recorded on the
  exchange by recovery's route B, the drain resume and PUBREL admission before any PUBREL
  reaches the client (consult 25), which the prior checkpoint's "the direct arrow's
  publish origin is unreachable" had missed. **Row 2, the output boundary (the next
  checkpoint's increment).** Every ordinary acquisition — PUBLISH, and both PUBREL sites
  including the direct arrow — installs its register token and its action record in ONE
  authority batch, under the connection's own accepted entitlement (recorded by the
  CONNECT accept loop; after a takeover every acquisition of the displaced connection is
  refused and the delivery ends). The route is decided by the one class gate
  (`mqttDurIsSlotOnlyClass`), not a caller's flag. The write boundary requires the token
  and the record together in both directions, makes recognition of an acquisition
  mandatory, binds the packet to the register step, and refuses any authority write that
  clears or substitutes live custody other than the holder's own drain (consult 27; the
  atomic holder word of plan revisions 2-3 was deleted by consult 28). An ordinary PUBLISH
  or PUBREL frame is written through `writeAuthorizedFrame`, consumed once and only for
  its live holder (PUBREL: a conditional write entry under the session lock and a
  per-execution ticket), and an acquisition given up before writing is recorded ABANDONED
  for the local discharge oracle. **Row 4 is closed by census and its remainder reassigned
  (consult 30).** Every ordinary PUBREL emission — the ready-slot site, the direct arrow
  and each redelivery — is already one authority acquisition written through
  `writeAuthorizedFrame` (row 2). "Canonical PUBREL preserved while depended on" is NOT
  delivered by row 2: it belongs to row 8 and 7c-lc (resource-authorized deletion of the
  PUBREL consumer, recreate and inactivity protection), and re-materialisation of a
  vanished canonical PUBREL to 7b. The WP-A review is amended accordingly:
  resource-authorized cleanup and the teardown interleavings are out of its scope and it
  may not claim them. **The unsent ending (the next checkpoint's increment, the
  prerequisite consult 31 split off row 5).** An ordinary obligation destroyed without
  ever being transmitted is ended durably, inside the gated destructive door, by
  `opWithdrawUnsent`: the register becomes the cancellation tombstone and the exchange
  `Publish -> WithdrawnUnsent` in one batch, on whole-exchange evidence the write boundary
  re-checks itself (no output token, and the marker naming the allocation standing). Where
  the door destroys on a tombstone or nonce-verdict proof without that evidence, HEAD's
  behaviour is kept and the exchange stays `Publish` (consult 32); the custody ledger owes
  that population's charge and identifier. Beside it: admission is pinned to the
  connection's own accepted entitlement (a displaced connection no longer admits under its
  successor's tenure), and the admission ladder records the register's own sequence rather
  than the authority batch's, so the door no longer reads every admitted register as
  moved. **Row 5: capacity and identifier custody belong to the exchange (consults 30-32;
  plan rev 6, six plan rounds).** The custody ledger (`outbound_custody_ledger.go`) owns
  every ordinary exchange's Receive Maximum charge and identifier. An admission
  reservation is opened in the critical section that moves the tracked allocation out of
  the scheduling projection and held for the whole ladder; every submitted attempt is
  settled by resolve-abort-resolve and charges the lineage it wrote. An exchange entry is
  released only by an exact read of an ended phase (`mqttExchangeCharge`) or generation
  discard; hints from the ack advance, the drain, a withdrawal and a pair replacement
  schedule observations and never release. Capacity is two budgets (local scheduling
  against MaxAckPending, and the client's Receive Maximum over custody plus the entries
  the ledger does not own), with a separate resume replay allowance; `qos12InflightForCap`
  and `effectiveMaxAck` are deleted. The allocator requires no map entry, no ledger charge
  and no live send hold. A resume rebuilds custody from the generation's EXCHANGES before
  delivery or allocation opens; a failed census withholds new work and allocation, and an
  unreadable or vanished exchange is kept uncertain by subject until it reads or the
  generation is discarded. Every non-ended ordinary exchange — including one destroyed
  without evidence — holds its charge and identifier until a durable ending or the
  generation's discard (F-106's accepted cost). Shared-group QoS 1 capacity is the
  ledger's since 7a, retained capacity from its admission since 7c, and experimental
  affine capacity stays map-counted until chapter P converts it. A restored ordinary
  delivery DENIED on resume is
  abandoned durably (register state `delivery_abandoned`, an immutable exchange fact,
  charge released per Spec5 §4.9, identifier retained until valid protocol completion and
  cleanup or generation discard). Unit C preserves ACK routing and source settlement
  ownership through recovery; see its closure and residual above. A register-only
  replacement now keeps its `Acq` (a pre-existing defect the abandonment's veto exposed).
  At that point CONNECT authority activation, recovery re-enrolment, teardown release,
  rows 6-8 and the acquisition-release fence were still unwired; they have since landed
  (7b-7f, 8a — see the entries below). No supported
  production route calls
  `storeInflightRegister`; its sole remaining production caller is the explicitly
  experimental affine route, whose conversion and final writer deletion belong to chapter
  P.

  **No release blocker is carried by this checkpoint's own arrows.** A completed
  exchange reads a durable ending; one whose PUBREL output is still live reads "drain
  owed". The limits `docs/CONFORMANCE.md` stated then — a dead PUBREL holder's drain stays
  owed, a vanished canonical PUBREL holds its identifier, the completion worker is
  process-local — were gaps of the unfinished cutover. 7b and 7f since re-materialise a
  vanished canonical PUBREL in the supported absent-consumer case, and 8c lets each resume
  attempt to clear a dead holder's token on its connection's persisted certificate; what
  remains of the three was accepted as stated residuals by consult 125 (2026-10-05).
  Neither the anchor nor this checkpoint was release-ready on its own; the integrated
  candidate is accepted only by the integrated gate.

  An acknowledgement-cleanup latency is recorded in `docs/CONFORMANCE.md` (an ordinary
  acknowledgement's terminal cleanup takes one round-trip more than the slot-only path, so
  a packet identifier re-allocated in that window waits out AckWait). It was recorded as
  closing at 7a; 7a WITHDREW that promise (consult 82): converting the shared class does
  not delete the register inside the advance batch, and no carrier/settlement design yet
  shows that doing so is safe. Consult 125 (2026-10-05) accepted it as a stated residual
  rather than owed work. **The WP-A review
  (consult 80)** judged the ordinary QoS 2 slice as one composition over the row 1-5
  checkpoints. Its first round found a retained terminal slot whose source settled after
  the 30-attempt retained reconcile gave up: the register and the identifier exclusion
  stood until a resume. Consult 81 replaced the bound with one owner per retained slot,
  enrolled beside the exclusion, retried without a lifetime cutoff and woken by every
  confirmed source settlement (`retained_terminal_cleanup.go`). The review is a trigger,
  not acceptance; 7a follows it, and under consult 80's ruling the alpha requirement for
  `storeInflightRegister` is that no SUPPORTED route calls it — the experimental affine
  caller's conversion and the writer's final deletion belong to chapter P.

  **Unit 7a (the supported shared conversion, checkpoint).** Every supported non-affine
  shared route is converted, for the only QoS the supported route carries (1): a shared
  delivery is admitted through the authority beside an exchange that records the EXACT
  acquisition `{Group, Inc, Acq}` it depends on (consult 82's S1: the group consumer is
  held by acquisitions in the generation's ledger, not by a per-generation resource),
  selected from the session's memberships and re-validated against the ledger INSIDE each
  attempt, after the authority read, so the batch's authority CAS fences it. Output, the
  captured ACK, the custody ledger, withdraw-unsent and abandonment then apply to it
  unchanged; the slot-only class is the experimental affine one alone. A denied shared
  delivery is handed back, never acknowledged: consult 83 closed that class at ONE
  positive source-ACK gate with the abandoned-ACK binding set as the only HandBack
  definition, held until the exchange ended AND its register is proven deleted; consult 84
  accepted the shared QoS 0 check-to-write overlap as a stated duplicate-delivery
  residual. What 7a leaves: the release-side fence (7d, now 7e; designed in
  `docs/ARCHITECTURE.md`, "A shared exchange depends on its exact acquisition"; until then
  a release can retire an incarnation a non-terminal shared exchange depends on — a gap,
  not a residual), the ordinary arbitration replacement (7b, below), retained (7c),
  CONNECT activation and the tenure consolidation (8), and the affine route (chapter P).
  Consult 85 also corrected a PRE-EXISTING resident-resume defect the plan's resume test
  found: the close's shared purge dropped the source mapping of a retained
  possibly-transmitted delivery, so the group's redelivery to the resumed member arrived
  under a NEW identifier and the original stayed charged; the surviving-disconnect purge
  now keeps that mapping. Still open, stated in `docs/CONFORMANCE.md`: a source another
  member consumed while this one was away leaves this member's exchange charged until
  discard.

  **Unit 7b (row 6: resume restoration from the duty inventory, and ordinary ownership by
  entitlement; checkpoint).** Ruled by consults 86-90 (`docs/ARCHITECTURE.md`, "Resume
  restores from the inventory, and ownership is the entitlement's"). The resume's custody
  and ordinary obligations are rooted in the generation's duty inventory (the ledger's
  `Hi`, every ordinal, terminal entries included; uncertainty never a hole), which
  REPLACES the row-5 subject census; the register walk stays the occupant superset, and a
  register the inventory did not reach is refused. Ordinary ownership is the connection's
  accepted entitlement compared inside every owner-dependent authority batch:
  `opWithdrawUnsent` takes a licence, a register-only cancellation is the new
  `tombstone-register` transition, the disconnect and teardown void and every verdict
  staging carry the deciding connection's licence (`boundEnt`), and the register claim
  decides nothing — the resume's claim contest over ordinary registers, the Shared scope,
  the ordinary custody transfer and the admission ladder's foreign-claim arms are DELETED.
  Displacement is observed by two durable authority reads and apply's local binding. A
  pending projection recovery installs exists only while custody holds its lineage
  (install gate and lineage-reporting release under one lock). A vanished canonical PUBREL
  is re-materialised (`rebind-canonical`), and a ready PUBREL acquisition carries its
  delivered canonical sequence. Stated in `docs/CONFORMANCE.md`: the live-generation
  accumulation residual, the withheld re-materialisation while the PUBREL resource is not
  ready, and continuation 4 left for 7d (closed there).

  **Pending-entry structural closure (checkpoint after 7b; consults 89, 90, 95, 96, 97).**
  Census-first (a type-checked census of every `mqttPending` writer, selector, field and
  alias), then closed by construction (`docs/ARCHITECTURE.md`, "A pending entry's
  authority is its kind"): every entry has a KIND that controls eligibility (Hold, the
  zero value, acts for nothing; Ordinary exists only while custody holds its lineage;
  Retained and Affine are classified), its EVIDENCE comes from three producers only (a
  validated register, a landed admission, an acquired action), kind and evidence are set
  only by constructors and a promotion replaces the entry, and both maps are written by
  one function, `setPendingLocked`, whose every call states its expectation (empty, an
  exact captured pointer, or ANY at two recorded sites) and which refuses an Ordinary
  entry whose lineage custody does not hold; 7b's recovery gate still gates every
  recovered ordinary record whatever its kind. The round-5 population is closed: an
  acquired PUBREL action becomes an entry only through `bindActionToPubRelLocked`, which
  validates the resident, the handoff and custody before it mutates anything and builds
  the entry from the action's evidence, so the lineage's release takes it. The release
  removes delivery-phase entries only: terminal reservations end nonce-keyed, and a
  delivered PUBREL whose PUBCOMP this connection still owes becomes a non-acting Hold that
  its PUBCOMP settles and that is retired once its connection is no longer live (consult
  97). Also closed on the way: the ungated disposal reactivation, the acquisitions'
  unvalidated slot writes, the resident-inferred PUBACK settlement of a Hold, the
  shape-inferred retained admission, the PUBCOMP terminal conversion (its own
  pointer-checked conversion), and the occupant-route marker-opaque reservation (fenced on
  the ledger's holder epoch). A two-part structural test
  (`pending_entry_structure_test.go`, engine self-tested over a bypass fixture) and
  behavioural tests T1-T21 with their mutations verify it. Out of scope, stated in the
  architecture section: an UNPROMOTED lineage-less placeholder stays a Hold until its
  nonce-keyed release or the gate-driven pass, as before; affine behaviour (chapter P);
  retained through the manifest (7c). Nothing ships between these checkpoints; they remain
  CHECKPOINTS, not a delivery, and the integrated cutover holistic gate still requires the
  complete supported integration (next: 7c, 7c-lc, 7d, 8 in the approved build order).

  **Unit 7c (retained QoS 1/2 through the manifest) is IMPLEMENTED, not shipped** — a
  checkpoint like the ones above. A retained delivery is an admitted exchange with its own
  per-subscriber copy: a register union (the retained arm `payload`/`digest`, exclusive
  with the source arm), the copy reserved, chunked, sealed and ADMITTED in one batch that
  writes the manifest's `admitted`, the replay executed after SUBACK by a per-session
  replay worker, the frame judged (deny, then Maximum Packet Size) before acquisition, a
  PUBREC creating the canonical PUBREL `rel` on `$MQTT_inflight` in its advance (fenced on
  the unchanged READY manifest), unservable replays withdrawn when proven unsent and
  abandoned through the fact's new retained arm when possibly sent, material reclaimed by
  one restartable protocol whose close arm decides eligibility, CONNECT discovery and the
  recovery executor's pre-classification dispatch reaching a dead producer's copy, and
  recovery replaying from verified bytes with a fresh exchange read and an entitlement
  re-check before its exhaustion fallback (both arms). Four consults during
  implementation changed it: the abandonment's continuation is a worker's with durable
  ownership (103), the SUBSCRIBE name claim — which the retained workers' authority
  bursts were measured to defeat — got a spaced budget (104) and a foreground-priority
  handshake over the retained workers (105), and three holistic rounds re-cutting one
  seam (a failure path ending an obligation still owed) were closed by a PROPERTY with one
  exit door (106): every retained obligation stays owned until its settlement, a positive
  entitlement transfer or an effective connection close, the withdrawal is a retained door
  of its own, and an ownership oracle is asserted at the fault tests' quiescence points.
  Details in `docs/ARCHITECTURE.md` ("Retained
  delivery through the manifest"); its residuals in `docs/CONFORMANCE.md`. Next: 7c-lc,
  7d, 8. **7d's retirement set includes, for a DISCARDED generation, the outstanding
  exchange duties and the exchange, the retained register and the `rel` records**: 7c
  reclaims a discarded copy's material only (consult 100 condition 5; unfinished F-105
  work, not a residual).

  **Unit 7c-lc (the source consumer's lifecycle) is IMPLEMENTED, not shipped** — a
  checkpoint like the ones above. Consult 110 scoped it: the mechanism and the foreground
  SOURCE routes here; teardown, the PUBREL consumer's retirement producers, the discard
  disposition and the recovery executor's source/PUBREL cleanup stay 7d's. It first closed
  a prior-row omission: no source-replay or canonical-PUBREL edge was ever released, so
  every such resource that admitted anything was permanently unauthorizable. Now an
  exchange carries `holds` (the reverse index of those edges) and the batch that ENDS it
  releases them, enforced at the boundary both ways (no later release door); a logical-use
  consumer is retired only on a route's DISPOSAL decision written under the entitlement it
  decided with (zero edges is also an idle subscription); the deletion authorization
  retires the name in the same batch, both directions (consult 14 finding 1); and every
  route that used to delete a source consumer raw — UNSUBSCRIBE, the live and restored
  downgrades, the SUBSCRIBE rollback and failed filter, a fresh consumer's failed CREATE
  or local subscription, the drain's retirement, the resume repair's failed CREATE and
  undo — hands its decision to the session's retirement owner (generalized from the
  retained reclaimer), which authorizes, deletes, classifies and resolves outside every
  record fence. A failed claim is settled by arbitrating every submitted attempt's handle,
  never by a read. The PUBREL sweep's watermark gate was deleted (consult 14 finding 2:
  RETIRED ownership is its whole licence), admission checks the consumer's effective
  inactivity configuration, and the stable PUBREL name is sanctioned same-owner
  rematerialization. Consult 111 corrected a diagnosis mid-implementation (a fixture's
  fabricated ack reply, masked at baseline by the synchronous drain delete, not a
  production regression). Details in `docs/ARCHITECTURE.md` ("The source consumer's
  lifecycle"); residuals in `docs/CONFORMANCE.md` (the supported-configuration boundary,
  the offline retention cost, a cancelled transmitted obligation retaining its consumer).
  Next: 7d, 8.

  **Unit 7d (a discarded generation retires through its recovery owner) is IMPLEMENTED,
  not shipped** — a checkpoint like the ones above. Consult 113 scoped it to the
  retirement chain and split two pieces off it, both alpha and both before step 8:
  **7e**, the shared acquisition release fence (designed in 7a; the discard must keep
  progressing independently of `closeSharedGeneration`, which 7e may not change into a
  cycle), and **7f**, live-generation provisioning settlement and recovery disposal (a
  source or PUBREL consumer whose route crashed or lost its entitlement before its
  decision landed while the generation still serves, and the withheld PUBREL
  re-materialisation of `docs/CONFORMANCE.md`). What 7d found: every teardown deleted the
  generation's source, drain and PUBREL consumers raw and wrote nothing about them, so
  their resources stood LIVE with LIVE names for ever, the root never certified, a late
  CREATE of one of those names was never reaped, and no exchange of a discarded
  generation ever ended. Now the teardown TRANSFERS them; the licence of every act
  without an entitlement is the generation's committed discarding closure, checked at the
  write boundary; a new `Discarded` phase (`discard-exchange`) releases an obligation's
  holds and ends its duty without being an identifier ending, so no drain or discharge
  evidence is manufactured; a `generation-discarded` disposal carries no tenure; the
  recovery executor retires the consumers (decision, authorization retiring the name,
  classified delete — closing row 6's continuation 4) and a discard CENSUS over resolved
  exchange duties deletes what ended obligations left (`rel` first, then the register,
  which the teardown's reap now spares) before the root may certify; a bounded prompt
  drive calls the same visit at teardown; the release gate's root arm needs the discard
  (or an empty sealed inventory, the abandoned reservation's).
  Four plan rounds (5, 3, 2, 0 findings); the prompt worker's progress accounting was
  deleted after three rounds re-cut it, and a resource-side holder discovery was deleted
  as redundant with the census. Details in `docs/ARCHITECTURE.md` ("A discarded
  generation retires through its recovery owner"); residuals in `docs/CONFORMANCE.md`.
  Next: 7e, 7f, 8.

  **Unit 7e (the shared acquisition release fence) is IMPLEMENTED, not shipped** — a
  checkpoint like the ones above. Every shared release now reaches L1 through one route
  that, for an open ledger, CLOSES the acquisition on the ledger (a new `closed` list, which
  readiness refuses and the write boundary requires before L1), then FENCES the outbound
  authority with a new `fence-shared-release` transition — fenced only on a confirmed
  advance past the sequence read, never on a rejection, since a 10164 says nothing
  committed — then reads the generation's inventory and WITHHOLDS while an exchange naming
  the acquisition is still at `Publish` (or any subject is uncertain); a withheld verdict
  is re-run by the owner, from its stored record and bound to its generation, when a
  dependant leaves `Publish`, and by the verdict sweep. A sealed ledger and a generation
  under its discarding closure skip the fence (authorized retirement; consult 82's "every
  route runs (1)-(4)" was narrowed by plan review P-2). The plan review also found a
  PRE-EXISTING defect, fixed here: the clean-start handoff never ran a shared closing, so
  a displaced generation's acquisitions stayed listed and their incarnations held for
  ever; the recovery executor now closes a discarded generation's shared ledger and its
  root certifies only once that ledger reads sealed and empty. Five plan rounds (6, 5, 3,
  2, 0 findings). Details in `docs/ARCHITECTURE.md` ("The release fence: an acquisition is
  released only when nothing depends on it"); the stated cost — an exchange stuck at
  `Publish` by the 7a consult 85 residual now also holds its group for the generation's
  life — and the receiptless-teardown limit are in `docs/CONFORMANCE.md`. Next: 7f, 8.

  **Unit 7f (a serving generation settles its displaced tenures) is IMPLEMENTED, not
  shipped** — a checkpoint like the ones above. A persistent session's generation keeps
  serving across every resume under a fresh tenure, and a source consumer whose deciding
  route lost its in-memory custody (a crash, a takeover, a moved entitlement), or whose
  decided retirement was unfinished, stood for the generation's life. The licence the
  withdrawn 7c-lc rule lacked is the SUCCESSOR's accept: once tenure T' is accepted, T can
  write neither a claim nor a session record, so "provisioned under a displaced tenure and
  named by nothing T' holds" cannot be falsified. Resources now carry `prov_tenure`
  (proved equal to the serving tenure at creation, immutable); a resumed CONNECT runs a
  background settlement over the generation's resource records (paged, complete, retried
  while resident) that hands decided ones to the retirement owner and unnamed undecided
  ones of a displaced tenure as `tenure-superseded`, a reason the boundary admits only
  when the provisioning tenure differs from the deciding one; the owner keys its work by
  tenure, so a displaced item no longer swallows the successor's; the resume repair
  decides its predecessor `repair-replaced`; SUBSCRIBE and repair claims are made under
  the connection's entitlement; and the 7b PUBREL continuation recreates a consumer
  verified absent under its LIVE ownership. Eight plan rounds (6, 5, 4, 2, 4, 2, 1, 0) and
  two consults: the fork-reinstated decided-name seam ran five rounds until consult 116
  fenced the population out — fork repair reads whether each consumer it would reinstate
  still serves, after sampling the authority, and the accept's own handoff preparation
  CASes that sample (consult 115's attach-site predicate was superseded). Details in
  `docs/ARCHITECTURE.md` ("A serving generation settles its displaced tenures"); the
  settlement's per-resume cost, the operator-deletion boundary for the PUBREL consumer,
  the fork residual for an omitted owed-work drain and the fence's contention bound are in
  `docs/CONFORMANCE.md`. Next: 8.

  Row 9 (unit 8) is split, by consult 117: **8a** the CONNECT ownership lifecycle, **8b**
  the tenure and closing consolidations, **8c** durable certification. **8a done**: a
  proposal some acceptance left standing is never read as the CONNECT's own (identity is
  the CONNECT's own record of its intents, and the own verdict is read from its own slot);
  it is SETTLED — finalized as proposed, cancelled back, or closed, by its permanent
  verdict — at the top of the CONNECT before any restore, retirement or clean-start
  destruction, and again at every discard's boundary (`prepareClear`), whose final load is
  the only one `destroy` and the clean-start choice read. A resident object a committed
  proposal moved past is disposed as foreign; a retirement-pending one takes the existing
  join. The expiry dispositions are acted on, an own aborted proposal is abandoned on one
  path (in place, else the owed-abandonment worker on its captured target), the ownership
  deadline caps every request through a deadline view of the JetStream assistant, and
  every refusal that wrote no success CONNACK — and every teardown — certifies its
  connection closed. The two crash-window tests skipped since the cutover run un-skipped.
  Seven plan rounds (9, 4, 5, 4, 3, 2, 0) and two consults: 118 replaced a bespoke resident
  disposal with the existing foreign one, and 119 replaced a refusal at the discard
  boundary (which owed every discard caller a new recovery path) with settlement there.
  Details in `docs/ARCHITECTURE.md` ("A standing proposal is settled before anything is
  destroyed"); the boundary's receiptless residue under a store failure, the deadline's
  reach and the 8b/8c gaps are in `docs/CONFORMANCE.md`. Next: 8b, 8c.

  **8b done**: one tenure per acceptance — the ledger tenure's id IS the authority
  tenure the acceptance was finalized under, and its order is the stream sequence of the
  acceptance's own fact, read back by subject; EVERY acceptance installs it before
  CONNACK, and the installation is the only creator of a ledger (a join over absence and
  the abort's init fence are deleted). One closing driver (`closeRetiringGeneration`)
  runs the receipted discarding close first and seals only under a licence: an
  already-sealed ledger, a discarding closure, or admission fenced (a closure, or an
  authority proving the generation never served) with an empty ledger; anything else
  withholds with the record marked. Every adoption settles a standing proposal first,
  and a marker with no receipt over a still-serving generation is continued by
  committing the receipt with a CAS of the marker itself. `destroy` lost its
  `discardAuth` flag. A clean start decides once, before the detach, by a default-refuse
  disposition: it hands off naming the targets of the displaced record it proved,
  discards only where no receipt can follow, and refuses behind an unresolved boundary.
  The close fast path and episode no longer compose a second close while an attempt is
  unresolved. Four plan rounds (6, 4, 1, 0) and three consults: 121 chose the
  withhold-plus-continuation over sealing receiptless, 122 replaced a RESERVING-only
  close-admission rule (which ~35 fixtures contradict) with a ledger-emptiness licence
  and a sealed-resume rule, 123 settled the clean start's disposition after two holistic
  rounds cut the same seam. Details in `docs/ARCHITECTURE.md` ("One tenure and one
  closing driver"); the delayed-release residue, the clean start's refusal behind an
  unresolved boundary, the preserving-closure late-join residue and the per-CONNECT cost
  are in `docs/CONFORMANCE.md`. Next: 8c.

  **8c done**: a closure certificate outlives its process. A connection that attempted an
  acquisition registers where its certificate is persisted BEFORE the acquisition's batch
  is sent, and whoever records the certificate — the torn writer or the external certifier
  — persists it once, off the caller's path, as
  `$MQTT.inflight.f105.cert.<idHash>.<gen>.<run>.<conn>`. A persisted certificate is the
  one licence besides the holder's own that ends a register's live output token: the
  action at the register's own counter must name the certified connection and be of a kind
  whose token can stand in that state, and the write lands at exactly the sequence read —
  token cleared and nothing else, paired with the terminal advance in `pubrel_terminal`. A
  resume clears every certified token of its generation before its walk; an acquisition
  that meets one clears it once before refusing. Absence licenses nothing. The
  whole-exchange discharge proof, the local drain oracle and `mqttActionDrained` had no
  production caller and were deleted rather than wired (consult 124). Five plan rounds (5,
  2, 1, 2, 0): rev 3 replaced an in-walk reload with a pre-walk pass after two findings it
  caused. Details in `docs/ARCHITECTURE.md` ("A closure certificate outlives its
  process"); the one-attempt persistence, the per-resume cost, the PUBREL reconnect
  requirement, the corrected generation-tied charge rule and the narrowed abandoned-action
  residual are in `docs/CONFORMANCE.md`. Row 9 is complete; what remained of this bullet
  was WP-F, delivered in the W3 candidate, and F-105 is accepted only by the integrated
  gate below.

  Codex ruled there is no independently correct partial delivery, so nothing ships until
  the integrated change passes a holistic review (the integrated gate below; its result is
  never written into the tree ahead of it). That gate requires the COMPLETE
  SUPPORTED integration — the alpha list below — and is not satisfied by a subset; it does
  not re-import the affine completion work chapter P now owns.

  **The conversion lane reordered itself under a Codex ruling.** Consult 3 superseded an
  earlier instruction to flip the production route first: flipping before CONNECT can mint
  an authority would make every QoS 1/2 delivery refuse and hold the suite red for the
  rest of the conversion, destroying the regression signal. Prerequisites are built first
  and the cutover is ONE bounded integration phase. A runtime switch or a
  missing-authority fallback was refused again at the same time.

  **Boundary ruling of 2026-09-21, bearing on every F-108, F-109 and F-110 reference in
  this bullet.** Those three findings were moved out of §0 and recorded in
  `docs/known-upstream-issues.md` as upstream data-layer defects: JetStream is the data
  layer, and a store that misreports its own
  contents is never this broker's defect to compensate for. Where this bullet says the
  substrate "needs revalidation or repair under F-110's failure model", or that the shared
  transfer is "BLOCKED on F-110", it rests on the broker having to defend against a
  create-only CAS the store admits over a record it could not read; under the ruling that
  is the store's contract and the store's bug (F-108 filed as nats-server#8632, F-110's
  issue pending). **Decided 2026-09-22 by the shared-transfer run's consult 1 (Codex, with
  the advisor concurring): those obligations LAPSE and the shared transfer is UNBLOCKED.**
  The dependency disposition, verbatim: "The shared-group lifecycle transfer relies on
  JetStream honoring the create-only precondition (`Nats-Expected-Last-Subject-Sequence:
  0`), which is the data layer's contract; F-110 is tracked in
  `docs/known-upstream-issues.md` under the upstream reporting and correction process and
  is not a broker residual or an alpha blocker." No
  defensive mechanism, fork, `replace` or `docs/CONFORMANCE.md` residual follows from it.
  The two clauses in the next paragraph are retired as ACTIVE requirements and kept as
  history; the checkpoints stay checkpoints for their original reasons (admission unwired,
  supported integration incomplete, integrated holistic gate outstanding).

  **HISTORY, retired as an active requirement on 2026-09-22 (see the ruling above).** As
  filed on 2026-09-21: "F-110 QUALIFIES EVERY mutation-checked CLAIM BELOW, and does not
  withdraw any of them. Those checks were real and are preserved; what they did not
  establish is the admission contract F-110 names, which did not exist as a question when
  they were run. F-105's committed checkpoints `2992f9c`, `d5eced3` and `86b9b25` write
  `expect: 0` members — the admission batch's register, unsent marker and exchange
  (`outbound_admit_publish.go:189-191`), and the resources, duties, recovery roots,
  exchanges and directory shards enumerated in F-110's census — so the existing substrate
  needs revalidation or repair under F-110's failure model." Under the boundary ruling a
  create-only CAS admitted over a record the store could not read is the store's defect, so
  the substrate owes no revalidation for it; the checkpoints remain CHECKPOINTS for their
  original reasons, and neither F-105 nor F-106 is closed by any of this.

  **Built and mutation-checked:** the per-client outbound authority with permanent
  per-transition outcomes and per-generation closures; the create-only abort that makes "this
  never landed" a positive fact; the classification handle for a restarted caller; the
  exchange and action records, with the packet identifier an allocation belonging to an
  exchange rather than its identity; completion rules under which finishing an ADMITTED
  obligation never requires the current serving tenure; the immutable connection binding with
  a one-way output gate, writer barrier and closure certificate; resource records whose
  deletion authorization CASes against the transitions that would add a dependency; and the
  three-member frozen acceptance transaction carrying the handoff's metadata receipt.

  **Built since, and mutation-checked:** register replacement validated at the write
  boundary against the exact occupant it displaces; the in-flight slot decision as a pure
  classifier, with `storeInflightRegister`'s eleven inline predicates DELETED rather than
  duplicated; the action-consuming output boundary, where one logical frame is one
  execution and the binding must be the writing connection's own; captured acknowledgement
  bindings taken at DISPATCH, so a resumed client answering on a new connection still
  names the obligation it answers; the two capacity projections kept apart (since
  deleted: no production route reached them, and capacity is the custody ledger's two
  budgets); dependency edges carrying a fixed ROLE: six at first, of which four had no
  production writer and are removed by WP-F W3 (a persisted one is refused at the
  compatibility boundary, never migrated), leaving three: source replay, the canonical
  PUBREL and retained material (added by 7c), each released by its exchange's ending;
  provisioning status as an axis separate from
  retirement, gating deletion authorization; the generation ledger and duty inventory with
  contiguous ordinals allocated inside the admission's own batch; the sealed-inventory
  fence; the paged recovery directory and its four-field sweep cursor; the duty
  continuations; immutable retained replay material with a chunk fence that is a manifest
  CAS and the chunk creation in one batch; the source shape and canonical reference as
  validated tagged unions; and the ownership deadline anchored where F-051's timer stops,
  with its three expiry dispositions.

  **Documented** under "The outbound obligation authority" in `docs/ARCHITECTURE.md` and in
  `docs/CONFORMANCE.md`. Since rows 1-5 and units 7a-8c, production routes admit, acquire,
  acknowledge, restore and retire ordinary, shared QoS 1 and retained obligations through
  it. The parts of both documents that still described the state before any route
  admitted are rewritten by WP-F W3 (this change; step 6 below).

  **The shared-durable enrolment landed (d5eced3) and the RECOVERY PASS now landed on top
  of
  it.** A closing transition allocates a REACHABLE RECOVERY ROOT in the same atomic batch
  that
  writes the closure, seals the inventory and enrols the durables, so a closed generation
  is
  discoverable by construction rather than by a later write a crash could lose; the
  closure
  RECORDS that root's address, so the release gate reads it exactly rather than by an
  enumeration
  that can come back short. An account-manager pass walks the directory on the periodic
  sweep's
  cadence under an explicit budget of WORK UNITS — not store calls; a unit is charged
  where work is dispatched and the helpers it calls carry nested reads — with capacity
  reserved for its own
  progress
  writes, and executes the affine cleanup family end to end — the readiness assertion, the
  provisioning seal, the deletion authorization, the physical delete, and the classified
  outcome
  with the duty's resolution in ONE batch. A generation whose duties are all terminal is
  retired:
  its completion certificate is written and CONFIRMED first, and only then is its root
  deleted. The readiness WALK alone sits outside that budget — it is account-wide and
  unfilterable, so charging it per record starves it — and is capped at ONE attempt per
  pass, cancellable between record units rather than at every store operation, with the
  pass ENDING after the root that spends the attempt so a permanently failing root cannot
  hold it against its siblings. The walk takes an AUTHORITATIVE horizon and re-reads the
  stream's end to confirm it did not move — a fixed horizon alone is unsound, since
  MaxMsgsPer:1 lets an unrelated contender rewrite a claim above it — with at most two
  extensions, and withholds readiness rather than asserting it when the tail keeps
  moving. That leaves a stated LATENCY residual in `docs/CONFORMANCE.md`, and it does not
  establish that the shared maintenance goroutine cannot hang: its pre-existing orphan
  scan walks the same stream unbounded.

  The gate's third fact changed with it: it asks whether the generation is REACHABLE, not
  whether
  its sweep is unfinished. A finished sweep is not a retirement, and making the gate
  depend on a
  cursor position would make one goroutine's liveness its safety condition.

  **Ownership is per TARGET, and the selector is the closure's own discard disposition.**
  A
  DISCARDING closure enrols its affine QoS 2 consumer and that deletion becomes the
  pass's; a
  preserving closure enrols none and its affine consumer stays the teardown's, exactly as
  today.
  The teardown classifies BEFORE its first deletion and carries that classification to the
  release — the release gate's own question runs after the deletions, which is right for a
  release
  decision and useless for a deletion one.

  **THIS IS A NON-SHIPPING CHECKPOINT, and its claim is CONDITIONAL.** The pass
  establishes
  readiness and completes the affine cleanup from persisted evidence alone, with no owner
  teardown — **given an admission closure that accounts for every previously admitted
  producer**. That premise does NOT hold today, and the unconditional claim that affine
  recovery is demonstrated is WITHDRAWN. `beginQ2AffOp` admits a move against LOCAL session
  state, and the claim it then creates is a subject-only CAS that never contends with the
  generation's closure, so a mover paused before that CAS can resume after the pass's final
  verification read and install a fresh UNDISCARDED claim. The existing restart tests
  demonstrate the executor for the populations they exercise; they do not supply the missing
  premise, and neither do the green suites nor the mutation results. See **F-107**, which
  this depends on and which is **chapter P's** — it was relocated there on 2026-09-20,
  unresolved, because the premise it supplies is consumed only by this AFFINE half,
  which is an EXPERIMENTAL obligation and not an alpha prerequisite. (The sentence that
  stood here, making the shared-consumer lifecycle work and sound F-108/F-109 evidence an
  alpha prerequisite of cutover step 2, is retired: the shared lifecycle landed on
  2026-09-22, and the F-108/F-109 clause fell to the boundary ruling above.)
  **There is a SECOND missing premise, and it is independent of the
  first**: the walk has no authoritative completeness evidence either. A stale non-direct
  GET
  can be answered by a partitioned former leader, and a failed store read is indistinguishable
  from an empty suffix (**F-108**), so "the executor is demonstrated conditional only on
  F-107" would itself be too strong. What is demonstrated is execution over the exercised
  fixture populations, conditional on BOTH premises. This half may not be presented as an
  independently safe production cleanup transfer, and if enrolled production generations
  carrying an AFFINE cleanup duty become reachable before F-107 lands, this split stops being
  acceptable. Clearing that population is chapter P's promotion item's business, not an alpha
  obligation.

  **The SHARED transfer LANDED on 2026-09-22, as ONE item: option A, the per-group holder
  authority plus physical-incarnation naming.** Its design was settled on 2026-09-20 (that
  run's consult C1), stopped by consult C2 on the F-110 premise, and unblocked by the
  boundary ruling above; the second run's plan converged after nine Codex review rounds
  (9 → 8 → 5 → 4 → 2 → 2 → 1 → 1 → 0 findings) and three consults. What landed
  (`docs/ARCHITECTURE.md`, "A group durable is held by arbitration, not counted by a scan"):
  a **group record** per logical group on the dedicated `$MQTT_shared` stream
  (`grp.<qkey>`: live incarnation
  and holder count, retiring incarnations, monotone ordinal), an **acquisition ledger** per
  session generation (`shl.<idHash>.<gen>`: the acquisitions the generation holds, an
  ORDERED tenure naming which CONNECT of the generation may acquire, and a permanent seal),
  **release verdicts** (`AcqReleases`) in the session record written by the transaction
  that removes a membership and executed by any reader, and incarnation-qualified names
  (`$MQTT_SHARED_<qkey>_<inc>`, `$MQTT.qsub.<qkey>_<inc>`, the logical queue unchanged).
  The join is J0 admission for the whole filter → J1 acquire (one ledger+group batch under
  the tenure, a settlement loop over one acquisition identity, an abort fence that EXISTS)
  → J2 the eager membership save as the acquisition's DISPOSITION → J3 create → J4
  subscribe; the release is a verdict in the record then L1 (ledger entry out, count down,
  retire at zero) and L2 (classified delete, retiring entry out); the teardown seals the
  ledger, releases everything listed and gates the record's release on the empty seal, on
  both routes; a resume installs its tenure after its accept save, executes verdicts,
  cancels uncommitted joins by a SUBSEQUENT record CAS and refuses on group inconsistency;
  the orphan sweep classifies shared consumers by ordinal against the group record, finishes
  retiring entries, and executes verdicts on two stable observations in a pass of its own.
  Every duty the earlier text listed is met by it — joining fenced against closing (the
  seal); a failed join owning its acquisition (the abort table and the compensation's
  verdict); a last-reference rule for several keys reaching one consumer (one acquisition
  per key, count discipline); broker death not being holder death (the ledger and the
  verdicts, recovered by the adopted teardown and the verdict pass); `retiring(X)`'s duty
  surviving the move to `live(Y)` (a member of a record a join mutates but never clears);
  delayed creates and deliver-subject reuse (I-NAME and the ordinal derivation); the orphan
  sweep obtaining retirement AUTHORITY from the record; and the synchronous leaving and
  rollback paths losing their scan-authorized route (the walk, its five delete arms, the
  shared enrolment, the shared resource kind and the release gate's enumeration inputs are
  DELETED). Storage accepted and the stated residuals are in `docs/CONFORMANCE.md`.
  **The old numeric `shared_subs` record shape is REFUSED**, never migrated.

  **The two records live on a dedicated `$MQTT_shared` stream, not on `$MQTT_inflight`.**
  The ported `TestMQTTMaxAckPending` exposed an upstream store defect (`docs/known-upstream-issues.md`,
  MAXMSGSPER-REVIVAL): a `MaxMsgsPer:1` in-flight register that is limit-evicted and then
  has its survivor deleted REVIVES on a raw-block (crash / stale-index) recovery when a
  neighbour keeps its storage block non-empty. A permanent lifecycle record on
  `$MQTT_inflight` is such a neighbour, so an early form of this transfer regressed even
  plain single-session accounts, which used to leave that stream empty at restart. Moving
  the records to `$MQTT_shared` (`DenyDelete`, so no tombstone and no revival shape; the
  tenth broker-owned stream, F-066) kept the shared transfer from adding a neighbour to
  `$MQTT_inflight`. It did not fix the multi-session exposure, which was HEAD's and is the
  upstream defect above; that is a residual in `docs/CONFORMANCE.md`, not a broker repair.
  **That rationale no longer makes `$MQTT_inflight` collapsible (corrected 2026-10-06,
  consult 133 Q3):** since the cutover, F-105's own permanent authority and outcome
  records are on that stream already by an account's first accepted CONNECT, and
  revival-eligible registers follow only after an obligation is admitted; WP-F step 6
  above states the disposition and the per-defect exposure. `$MQTT_shared` still keeps
  the shared lifecycle records off it.

  **Cutover obligation, recorded here for the CONNECT/closure step of the integration:
  DISCHARGED by 8b's ordering, not by a merge.** As recorded: the per-generation closure
  record and the ledger's seal were to be consolidated, as were the ledger's tenure and
  the authority's tenure, preserving the exclusion of delayed writes to
  `shl.<idHash>.<gen>` — a closure written elsewhere with this seal then deleted would
  reopen the create-only address — so it changes the arbitration or keeps the fence, and
  is never a licence to reclaim. 8b consolidated the tenures (the ledger tenure's id IS
  the authority tenure its acceptance was finalized under) and ORDERED closure and seal:
  one closing driver, `closeRetiringGeneration`, runs the receipted discarding close first
  and seals only under a licence. The seal stays its own record, so the `shl`
  delayed-write fence is kept (`outbound_close_enrol.go`, `closeRetiringGeneration`).

  **The affine recovery EXECUTOR is demonstrated, conditionally; shared release is the
  lifecycle transfer's rather than this pass's, and full P3 is not met.** There are TWO
  conditions, not one: given an admission closure accounting for
  every previously admitted producer (**F-107**, now chapter P's) AND authoritative
  enumeration completeness (**F-108**), the pass completes the enrolled cleanup from persisted
  evidence alone. Both conditions bear on the EXPERIMENTAL claim; neither is an alpha
  prerequisite. Full P3 remains unproved. The
  witness exercises the executor over its fixture population and supplies NEITHER premise.
  The witness
  restarts
  the broker against the same JetStream state and requires a FRESH account manager, with
  none of
  the first one's memory, to complete the enrolled cleanup from persisted evidence alone —
  and its
  oracle is the terminal duty and the certificate, because "the consumer is gone" is
  equally
  satisfied by a synchronous delete that ran anyway.

  **Two unresolved populations are stated in `docs/CONFORMANCE.md`**, both accumulation
  and neither client-visible: a delayed affine create landing after certification, which
  reduces to F-036 course E's accepted residual; and a creation over a CLOSED predecessor,
  which is PRE-EXISTING and identical at HEAD. (A third — roots that never retire while the
  shared duty was untransferred — is CLOSED by the transfer: the duty no longer exists.)

  **Delivered in the W3 candidate, ALPHA (re-audited against the code on 2026-10-05,
  consult 125; WP-F completed 2026-10-06).** Every route the earlier list named is
  converted: admission and output (`admitInflightPublish`, `writeAuthorizedFrame`),
  retained sends (7c), retransmission and PUBREL (rows 2 and 4, 7b), the acknowledgement
  arrows (row 3), restoration and recovery (7b), retirement and deletion (7c-lc, 7d, 7f),
  custody and capacity release (row 5) and the CONNECT ownership lifecycle (8a-8c). The
  recovery executor performs every retirement a discarded generation owes; the verdicts it
  withholds — deliver, release, drain, provision — are withheld by stated rule and owed by
  no supported population, because production writes a preserving closure only over a
  reservation that never served. The clause asking for sound evidence on the decisions
  F-108 and F-109 affect is retired by the 2026-09-22 ruling above. Partial adoption is
  still worse than none: a path that ignored the authority could create work after
  closure. **WP-F** (the cutover plan's verification, mutations and documents) is ONE
  acceptance unit with ordered steps, delivered by W1 (`868990c`), W2 (`522d25d`, which
  needed F-113's fix `c374099`) and W3 (this change). Each step's evidence follows; none
  of it is the gate's result, which this tree does not record:

  1. **The output boundary, restated.** The five actionless emitters (CONNACK, the server
     and shutdown DISCONNECTs, SUBACK/UNSUBACK, the displacement notice) move to
     `writeActionlessFrame`. The structural check then requires the recorded bypass set to
     EQUAL the two mixed sites `mqttEnqueueProtoLocked` and
     `mqttEnqueuePreparedPublishMsgTo`, with their per-site counts, and tests show that
     supported QoS 1/2 traffic never leaves through them. Their remaining raw affine
     output is chapter P's; the plan's empty-set requirement is restated, never claimed
     met, and B2 is re-run against the restricted set. *Delivered by W1 (`868990c`):* the
     five moved onto `writeActionlessFrameBy`, with
     `TestF105OutputBypassSetIsTheTwoMixedSites` and
     `TestF105SupportedObligationsNeverLeaveThroughAMixedSite`.
  2. **The plan's amendments recorded:** `storeInflightRegister` keeps only its
     experimental affine caller (consult 80); the custody ledger, not the exclusion lift,
     is the allocator's gate (`liftIdentifierExclusionLocked` still lifts on absence;
     witness `TestF105CustodyAllocatorSkipsALedgerHeldIdentifier`); 8c's certificate
     clearance replaces the deleted discharge oracle. *Delivered by W1 (`868990c`):*
     recorded, with the restricted bypass set, in the cutover plan the gate judges
     against; W3 adds Amendment 9 (consult 133, step 6 below).
  3. **An obligation-to-evidence crosswalk** over the plan's structural properties as
     amended, its test census and its behavioural and mutation lists. The census is read by
     assertion: `TestMQTTInflightReservationReleaseIsOrderIndependent`,
     `TestMQTTInflightReleaseRequiresSerializedVerdict`,
     `TestMQTTInflightAbandonReleasesProvenReservation` and
     `TestMQTTExclusionReclaimLiftsOnConfirmedAbsence` carried their pre-F-105 names,
     and the first three asserted scheduling-map removal while describing an identifier
     release the custody ledger now owns (the first is the test F-106 names for violation
     1): each is replaced or narrowed, keeping its valid checks, and mapped to the custody
     test that covers it; the F-098 action assertions are read and recorded too. Every
     behavioural scenario and every named mutation is mapped to its evidence — for a
     mutation, the effective mutation, fixture and assertion, tree and recorded result — or
     run now (C1-C9, A1, B1, B2, D2b, D2c, D3b and D4b appeared in no unit's ledger); a
     retired mechanism names the invariant that supersedes it; C9 and D2c keep their direct
     boundary submissions. *Delivered by W2 (`522d25d`):* the crosswalk is the lane file
     `crosswalk-wpf.md` (outside the tree; its path goes to the gate), every mutant run on
     one frozen candidate tree and recorded as observed (consult 130's contract); the three
     inflight tests are renamed to what they assert (scheduling-entry removal for a
     never-admitted allocation, with the custody ledger asserted empty; the first is now
     `TestMQTTInflightSchedulingEntryDropIsOrderIndependent`), the RM 1 behaviour lives in
     `TestF105CustodyMetadataRemovalReleasesNothing`; the seven sweep tests that asserted
     the pre-ownership sweep are re-expressed over production retirement; the dead
     `checkDestructionAuthorized` and `ConsumerInactiveThreshold` are deleted. W3 runs its
     own mutants (the refused former roles, the lineage derivation and the capacity
     charge) on its frozen tree and adds them to the crosswalk.
  4. **The client-visible witnesses,** through the assembled production route with real
     clients on two frontends sharing one JetStream, asserted separately: a PRESERVED
     obligation - a peer TENURE takeover that keeps the generation (production writes no
     closure for a generation that served and is resumed) - keeps its completion evidence
     and resources until protocol completion, and the client sees the completed exchange
     on the peer; an obligation of a DISCARDED generation - a peer clean start, or a peer's
     session expiry, that closes and discards the predecessor (consults 128, 129) - is ended
     only under that discarding closure.
     `TestF105ObligationSurvivesRetirementAndCompletes` builds the transitions directly and
     satisfies neither. The clean-start arm was unreachable until F-113's fix (`c374099`).
     *Delivered by W2 (`522d25d`):* `outbound_peer_witness_e2e_test.go` -
     `TestF105PreservedObligationCompletesOnThePeer`,
     `TestF105DiscardedObligationEndsUnderTheDiscardingClosure` (clean start),
     `TestF105ExpiredObligationEndsUnderThePeersDiscardingClosure` (expiry),
     `TestF105AnOverdueSessionIsNotResumedOnThePeer` and
     `TestF105AnUnconfirmedSourceAckStillNeverResendsThePublishOnThePeer`; the preserved
     witness withholds the PUBCOMP for three AckWaits and requires that the original
     PUBLISH never returns ([MQTT-4.3.3]); deletes are judged at request time and every
     delete route is checked behind a per-broker queue barrier.
  5. **Verification on the exact candidate tree:** every command the plan lists, reported
     by exact command and named red test, with no unexplained failure or race;
     `TestMQTTMaxAckPending` run and reported with its assertion intact. W3 runs the chain
     on its exact tree, alone, and reports it in the commit proposal, comparing reds by
     name with W2's chain; no result is written into the tree.
  6. **Documents complete BEFORE the review** (this change): `docs/ARCHITECTURE.md`'s
     F-105 status and its "not yet true" list, its JetStream stream table and storage
     discussion; `docs/CONFORMANCE.md`'s storage disposition and F-105 section;
     `docs/known-upstream-issues.md`'s exposure paragraphs; F-068's dossier, whose affine
     remainder moves to chapter P; this bullet and the superseded passages of the checked
     bullets listed below; F-118 filed in §0.4; and F-105's and F-106's ticks,
     provisional. **The storage disposition, corrected (consult 133 Q3).** F-105's
     permanent records are on `$MQTT_inflight` ALREADY BY an account's first accepted
     CONNECT, not only once an obligation was admitted: every accepted CONNECT runs at
     least two authority transitions, each writing a create-only permanent outcome, so at
     least two authority writes and two distinct permanent outcomes (a CONNECT that
     committed a reservation or preparation and then failed can leave them too); each
     admitted obligation adds its exchange, outcome and dependency records. Retained
     subjects are counted apart from write volume; nothing reclaims these records within
     the recovery epoch, so storage grows with the epoch's accumulated activity, and no
     fixed ceiling is claimed. Registers eligible for MAXMSGSPER-REVIVAL exist only after
     admitted work, and revive only where the rewrite, delete and recovery conditions also
     hold; the permanent neighbours remove whole-stream emptiness as an escape but
     establish no shared-block placement. Exposure to the upstream defects is per
     operation: MAXMSGSPER-REVIVAL (occupancy from the first CONNECT, single-session
     accounts included), F-108 (false enumeration exhaustion in the supported suffix
     scans, `scanPubRelRecordsCharged` among them), F-109 (false point-read absence in the
     CONNECT ownership reads and record loads), F-110 (a create-only CAS over an
     unreadable occupant: the outcome, ownership and certificate writes). This corrects
     the exposure statement; it reopens no upstream prerequisite and adds no compensation.
     **W3's code, which makes those documents true (consult 133 Q1):** the minting
     executors and the capacity projection, which no production route reached, are deleted
     (CONNECT drives ownership through `mqttDriveOwnershipRun`); the four resource roles
     no production route writes are removed, so a persisted one is refused at the
     compatibility boundary, never migrated; and a replacement's inherited lineage is one
     derivation, `mqttLineageForWrite`, with two agreement refusals (the replacement
     composer's, and `opAdmitPublish`'s for a create).

  **The integrated gate.** One `adversarial-review --scope branch --base 9310151` — the
  parent of the first checkpoint, `2992f9c`; the span also carries the F-108, F-109 and
  F-110 filings, the nats-server 2.15.0 pin and F-111, which the review is told about and
  does not rework — over the complete candidate tree, judged against the cutover plan's
  coherence property and its nine stopping invariants as amended in step 2. It passes when
  a completed review leaves no finding unresolved, each fixed (re-running the verification
  it touches) or refuted with the code, the spec or `docs/ARCHITECTURE.md`, and the accepted
  tree's fingerprint equals the committed tree with no later edit, a checkbox included.
  F-105 and F-106 are ticked together in the W3 candidate, provisionally; they are
  accepted as closed only when this gate passes, and a required change reopens them.

  **Gate round 1 (on `672a6a3`): one required change, made in the following commit.** The
  frame guard kept one entry per executed or abandoned acquisition in a per-connection map
  that nothing deleted, so a long-lived subscriber's memory grew with every transmission.
  The consume-once record moved onto each acquisition's holder (`mqttExecCell`, shared by
  the holder's copies and its entry tickets, never reset), so it lives exactly as long as
  the acquisition can still be presented, and no connection or session structure retains
  execution history (`docs/ARCHITECTURE.md`, "The consume-once record lives on the
  holder"). The fix's long-lived test also measured F-116's debt on an entered PUBREL
  refused by the client's Maximum Packet Size; that open finding is widened, not fixed
  here (consult 134).

  **Gate round 2 (on `31e2537`): round 1's fix accepted, one further required change, made
  in the following commit.** The identifier-exclusivity walk every admission runs read one
  stream-info page of the generation's exchange subjects and refused when the server
  reported more, and nothing deletes an exchange record, so a generation stopped admitting
  after 100,000 exchanges, even with every identifier free. `streamSubjectsComplete` now
  pages to the reported total and refuses only on a page it cannot vouch for; the same
  walk serves the obligation, resource and close-certificate enumerations, whose
  history-size ceilings go with it (`docs/ARCHITECTURE.md`, the admission invariants;
  `docs/CONFORMANCE.md`, the live-generation accumulation residual). The ticks stay
  provisional, and the gate re-runs on the corrected committed
  tree; F-105 and F-106 are accepted only when a completed round leaves no finding.

  **Checked bullets resting on F-105 (audited 2026-10-05, consult 125).** Conditional on the
  gate, because the property each closes is now enforced by F-105 code: F-111
  (row 5 unit C code); and, each with passages describing a mechanism F-105 replaced,
  corrected in step 6 (this change) to point at what replaced it:
  "`sharedDurablesHeldByOthers` fails OPEN" and F-082 (the shared holder scan, deleted by
  the shared-group lifecycle transfer), F-087 (its quarantine, gone with 7b's claim
  contest), F-091 (7c carries a retained identifier across a restart), F-092 (custody
  transfer is slot-only since 7b; its retained exception went with 7c), F-098 (7c gave
  retained sends a register), F-100, F-101 and F-102 (the charge is the custody ledger's)
  and "Prompt reclamation of a held identifier exclusion" (its F-104 paragraph names a
  function 7b deleted). Sound closures with a superseded phrase: F-076 and F-077 (the
  deleted shared holder scan) and F-099 (Arm B's shared contests). The row 5 unit C entry
  at the top of §0 is a checkpoint on the same terms. Every other checked bullet was found
  independent of F-105.

  The general recovery directory, the inventory, the resource authority and the retirement
  machinery are **alpha infrastructure**, implemented and on production paths since 7b-7d;
  WP-F is delivered in the W3 candidate, they await only the gate above, and the split
  below moves no part of them.

  **Remaining, CHAPTER P:** the affine cleanup family the recovery pass executes, and its
  F-107 premise. See "F-105's affine completion work" in chapter P, which carries both missing
  premises and the acceptance it still owes.

  **Virgin session creation is CLOSED**, and not by manufacturing a predecessor from
  session
  metadata. A claim is a `reserve-creation` transition installing a RESERVING authority that
  serves nothing, over an absent predecessor or a CLOSED one; its metadata half commits on
  the same permanent slot a handoff uses, with a receipt carrying a mandatory closed `kind`;
  and
  `finalize-creation` makes it servable only against that committed verdict. There is
  deliberately no kind that installs a serving authority in one step — the two that did,
  `admit-tenure` and `readmit-tenure`, are deleted, because each WAS the bootstrap gap.

**F-107 was relocated, unresolved, to chapter P on 2026-09-20**, under the ruling that
session-affine shared-subscription QoS 2 is not an alpha release feature. It is
experimental-only: not alpha-selectable, and not an alpha prerequisite. Its defect stands
and its evidence is preserved intact at
[F-107, in chapter P](#f-107-affine-claim-admission-is-fenced-only-in-memory).

- [x] **F-106 — ABSORBED INTO F-105.** Exchange-owned custody, capacity release,
  identifier-release authorization, both metadata-release bypasses, background exclusion
  reclamation and their recovery and test coverage are delivered and validated by F-105, not
  as separate work. Codex's design ruling (F-105 consult 5, D6) is that no part of F-106's
  mechanism work remains outside it, and that inventing a smaller item merely to preserve
  the
  original ordering would be wrong. This box is ticked together with F-105's, on the same
  terms.

  **Ticked in the W3 candidate; NOT accepted as closed until the integrated branch
  review passes.**

  **Status (F-105 row 5):** for the ORDINARY class both violations are closed — the
  custody ledger owns the charge and the identifier, and the metadata routes release
  nothing they do not hold; since 7a the same holds for shared-group QoS 1 deliveries, and
  since 7c for retained QoS 1/2 ones. They remain live for experimental affine ones until
  chapter P converts them; that remainder is chapter P's and is not this box's condition,
  which stays F-105's integrated gate.

  The original filing follows; it is the same item, ticked with this one.

- [x] **F-106: capacity and identifier release have no single authority, and metadata
  handoffs still discharge them.** **Ticked in the W3 candidate; NOT accepted as closed
  until the integrated branch review passes.** Filed from F-097 unit 2's holistic round
  12, which fired
  that lane's declared stopping rule on the sixth consecutive completion-evidence finding.
  **Do not patch per site; this needs the shared release design.**

  *The contract that must hold*, established across four design rulings in that lane:
  three
  facts stay separate — **completion** (this obligation ended), **occupancy** (a subject
  currently holds a record), and **observation finished** (a continuation has no useful
  work
  left). Neither occupancy nor the end of observation establishes completion. Identity
  checks
  limit WHICH obligation a release affects; they never supply release authority.

  *The two live violations round 12 named:*

  1. **Verdict retirement releases custody.** `forgetCancelledReg` deletes a parked
     `pendingPublish` entry merely because its verdict retires, and
     `tombstoneCancellationsOnStream` calls it after landing a tombstone with no delete
     completed. A peer may already have transmitted that allocation — a tombstone proves
     neither non-transmission nor completion. Removing the entry loses ACK custody and
     decrements `qos12InflightForCap`, admitting another delivery at Receive Maximum 1
  while
     the first is still outstanding. `releaseSettledVoidReservationsLocked` has the same
     metadata-to-capacity release. **Verdict persistence and retirement must be
  independent of
     custody**, with obligation-bound completion evidence required for every capacity
  release,
     including both reservation sweeps and callback settlement.
  2. **Background exclusion reclamation bypasses the evidence requirement.**
     `runExclusionReclaim` records empty subjects and calls
  `liftIdentifierExclusionLocked`
     directly, whose checks require only occupancy clearance and the local exclusion instance.
     It never obtains completion evidence and never passes through
     `settleRecoveryDropExclusion`. So an exclusion deliberately retained after an unconfirmed
     drop is lifted on the next poll once incarnation cleanup removes the register, although
     the client may still hold the exchange. **Completion evidence must be mandatory at the
     SHARED exclusion-release boundary**; polling may establish occupancy clearance only.

  *A test licenses the first violation:*
  `TestMQTTInflightReservationReleaseIsOrderIndependent`
  expects release on verdict retirement, so a green suite PRESERVES the contract violation.
  Replace tests that license release from verdict durability or from absence with end-to-end
  assertions that custody, quota and identifier restrictions survive without completion.

  *How each closed (F-105, supported classes; affine is chapter P's):*

  1. `forgetCancelledReg` and `releaseSettledVoidReservationsLocked` still remove the
     scheduling entry of an allocation that was never admitted; for an admitted exchange
     the custody ledger holds the charge and the identifier until its durable ending or
     the generation's discard, so removing the metadata releases nothing
     (`TestF105CustodyMetadataRemovalReleasesNothing`). `qos12InflightForCap` is deleted:
     capacity is the ledger's two budgets.
  2. `runExclusionReclaim` still lifts an exclusion on established absence through
     `liftIdentifierExclusionLocked`, but the allocator's gate is the custody ledger
     (`bumpPI` consults `holdsPI`), so an un-ended exchange's identifier is not handed out
     again (`TestF105CustodyAllocatorSkipsALedgerHeldIdentifier`).

  The test named above was renamed by F-105 WP-F W2 to what it asserts,
  `TestMQTTInflightSchedulingEntryDropIsOrderIndependent` (scheduling-entry removal for a
  never-admitted allocation, with the custody ledger asserted empty).

  *Audit boundary, which is finite:* every consumer that removes a capacity charge,
  lifts an
  identifier exclusion, forgets a verdict, or deletes a durable register.

  *Accepted cost when taken:* a peer-deleted register, or a delete whose own result was
  ambiguous, reserves one identifier and one Receive Maximum charge until the generation is
  discarded; occurrences accumulate and at Receive Maximum 1 a single one stops delivery.
  Convergence is conditional — successful authorized cleanup converges, missing completion
  evidence may permanently reserve resources within the generation.

- [x] [F-097: Terminal-completion evidence and reconciliation across allocation
  replacement](findings/ha-arbitration.md#f-097-terminal-completion-evidence-and-reconciliation-across-allocation-replacement).
  **Alpha blocker, filed 2026-09-09. CLOSED 2026-10-11 with F-088.** The joint change
  landed as `a4a01a1` (lane `~/.claude/lanes/f-097-after-f105/`, plan revision 20,
  consults C1 to C10; holistic round 6 approved tree `4737156d` with its residuals and
  coverage gaps disclosed). The close-out (lane `~/.claude/lanes/f-097-f088-close/`)
  witnessed the open evidence on production schedules, which reached two F-088 contract
  failures, and fixed both in the same change (see F-088). F-088 consumes this contract
  and lands with it. F-105 and F-106, accepted 2026-10-08, supply the identity and
  custody it rests on: lineage-keyed exchange records, and the custody ledger holding an
  exchange's charge and identifier until it durably ends.

  **The current contract (consult C1, after F-105).** F-105's lineage-keyed exchange
  records ARE the identity; the register is the durable carrier, preserved; a client
  acknowledgement is positive protocol evidence, but it carries a packet identifier, not a
  lineage, so its binding to the exact exchange is captured before any asynchronous work,
  kept through retries and reconstructed on resume from durable evidence; register
  disappearance contributes no authority. **Closure statement (C1):** durable
  lineage-bound completion survives allocation replacement, never releases another
  exchange, and drives durable and memory convergence after finite storage failure
  without another offer or resume.

  **C2's correction.** Plan-review round 2 finding 4 (R2-F4) demanded that a finite
  marker-read or reread failure during recovery be repaired on the LIVE session, so fresh
  allocation reopens without another resume. Consult C2 withdrew that: it conflated an
  exchange's RELEASE (kept, mandatory: the closure statement above) with reopening
  ALLOCATION after an incomplete recovery, which keeps its pre-existing, stated
  next-complete-resume bound (`piSpaceIncomplete`, `docs/CONFORMANCE.md`). R2-F4 and the
  findings against the live-recovery design (R3-F1, R4-F1, R4-F2, R5-F1, R5-F2's pause
  parts, R5-F3, R5-F4) are superseded with that design, not refuted; the live-recovery
  machinery was never landed.

  **The supersession matrix (WP1).**

  | Obligation | Disposition | Owner |
  |---|---|---|
  | `.done.<lin>` and its producer, resolver, recovery and sweep arms | RETIRED | this bullet |
  | F-098 dependency | RETIRED | this bullet |
  | "terminality in place is refused" | RETIRED for ordinary registers (the ACK advance re-phases the register in the exchange's batch) | this bullet |
  | Lineage versus suppression consult | SETTLED by lineage | this bullet |
  | Different-source arm (the HEAD defect below) | MET; regression `TestF097DifferentSourceArmReservesAPossiblyAttemptedOccupant` | T3, T3b |
  | Takeover stranding | durable half F-105; memory half the custody dispatcher | T1 |
  | Clause (e), memory release after a failed observation, and a peer's later completion | DELIVERED by the custody dispatcher | WP4 |
  | D4, `condemnSendLocked` first-wins | DELIVERED behind its witness (W6b) | WP6 |
  | Old D5 | SUPERSEDED by F-105's `releaseDeferredAfterCustody` | none |
  | F-088 counterexamples 1-3, pre-landing verdict, N1 | identifier harm superseded by F-105 custody; liveness and service delivered by WP2/WP3, W5b's population by WP5 | F-088 |
  | Counterexample 4 | DELIVERED for ordinary | none |
  | Interleavings (1)-(3) | witness-first: (3) witnessed by W5b and delivered for the witnessed row | WP5 |
  | R-B | narrow residual | `docs/CONFORMANCE.md` |
  | Recovery completeness after a finite failure | pre-existing residual, next complete resume (C2) | `docs/CONFORMANCE.md` |
  | Refuted proofs | preserved verbatim below | this bullet |

  **What landed** (mechanism in `docs/ARCHITECTURE.md`: "A late acknowledgement is bound
  before it arrives", the custody dispatcher under "Capacity and identifier custody belong
  to the exchange", "An admission that may have landed keeps its source", and the D4 rule
  in the send-section description; limits in `docs/CONFORMANCE.md`):

  - **WP2, a late acknowledgement is bound before it arrives.** Every scheduling-entry
    removal states its reason; an unanswered removal of a transmitted entry installs an
    AckRoute binding (built from the entry) in the same critical section, routed to the
    existing completion owners; the resolver answers `StillOwed` for an AckRoute's owed
    successful PUBREC while the acquisition is unchanged; an ended-lineage veto refuses
    AckRoutes for ended exchanges. An AckRoute's successful PUBREC over a source a drain
    still claims records the drain's PUBREL intent before the exchange moves, as the
    live-entry path does (T29 `pubrec-ackroute` found the PUBREL withheld for ever and the
    drain left claimed without it).
  - **WP4, the autonomous custody dispatcher**, replacing the hint-only observer: bounded
    units, round-robin with eligibility backoff, a tracked tick, a demand latch, a
    rate-limited census, and owed withdrawals and continuity records as its items.
  - **WP5, admission continuity, for the witnessed row only** (see the witnesses).
  - **WP6, D4:** the client's answer (`AckComplete`, `PubRecFailed`) replaces a parked
    non-answer in `condemnSendLocked`, never the reverse.

  **Witness results** (real-server, `~/.claude/lanes/f-097-after-f105/witnesses.md`):

  - **W5a** (`TestF097ContinuityW5aDefiniteFailureAdmitsOnce`): a definite failure, the
    batch never leaving; the re-offer is admitted once. **No defect.**
  - **W5b** (`TestF097ContinuityW5bLandedThenUnreadable`): **REACHED.** The first
    admission lands, its response is lost, every readback fails and its custody
    resolution stays unresolved; the ladder abandoned the entry and the source mapping,
    and the re-offer of the same delivery was admitted under identifier 2: two un-ended
    exchanges for one source delivery (interleaving 3). The continuity record
    (`outbound_admit_continuity.go`) landed for the witnessed row: with it the re-offer is
    withheld, the first admission is settled and withdrawn on its standing marker, then
    the source is re-offered and admitted once, also across a cold peer resume. The other
    rows (possibly attempted, past PUBREC, abandoned, an action token, unreadable) were
    NOT reached by any witness and were NOT built: there the refusal stands, conservatively
    (`docs/CONFORMANCE.md`).
  - **W5c** (the inherited replacement's production producer chain): **not separately
    witnessed.** T17 reaches the register-only replacement through the real ladder from a
    seeded same-delivery occupant; that is the boundary form only.
  - **W6b** (`TestF097D4W6bDrainOwnedDenyReleaseThenPubAck`): the plan's order (write
    returns, then PUBACK) completes through the AckRoute, no defect; the
    **ack-during-write** order **REACHED** D4: first-wins kept the drain-owned deny's
    `DenyRelease`, the consumed PUBACK completed nothing and the exchange stuck at
    `Publish`. The fix landed: answer kinds replace parked non-answers. Both orders pass.
  - **W6a** (a recovery drop over a live entry): **not run.**

  **What closed the box (2026-10-11).**
  - The holistic review of the whole change ran (round 6 approved `4737156d`, committed
    as `a4a01a1`).
  - `TestF105CustodyExhaustionRequestsObservation` passes: it was re-expressed under the
    implementation ruling R-impl-2, with the fault held through the window, the exhausted
    allocator's request starting the observing pass, and every release assertion kept.
  - The close-out's production witnesses and its fix are recorded under F-088.

  **Stated limits, not owed work.**
  - T2g attributes dispatcher requests by synchronous isolation, not by an explicit
    call-path context.
  - The `claimRecoveredInflight` reap of a slot-only tombstone on the `Cancelled` bit
    alone is a chapter P question (F-088 records it).
  - W5c and W6a are disposed under F-088.

  **History, condensed.** Unit 1 (the exchange lineage) LANDED before this run: a
  mandatory `Lin` on the in-flight register naming
  the CLIENT-FACING obligation, distinct from the nonce that names the allocation. It is
  minted fresh by every create-only write and by the proven-unsent replacement arm, and
  **inherited** by the register-only replacement arm — the one whose occupant is
  possibly-attempted and whose own comments say the client may already hold it. The
  value is decided by each attempt's WRITE MODE, because the store's retry loop resets
  to a create-only pair without rebuilding its record. A whole-record rebuild carries it
  with no code of its own, and `TestMQTTLineageWriterCensus` **scans the source** rather
  than enumerating known writers, so a writer added later fails it. An absent or
  malformed lineage is refused, never defaulted. Mechanism in `docs/ARCHITECTURE.md`
  under "An exchange's LINEAGE is carried, where its nonce is deliberately re-minted".
  At the time it changed no behaviour beyond that refusal; F-105's exchange records are
  now keyed on it.

  **Unit 2 as first conceived, now RETIRED by consult C1**, was blocked on F-098: the `.done.<lin>`
  completion record, its producer at PUBACK and failed PUBREC, the resolver that reloads the
  current subject, recovery's consumption, memory reconciliation for an owner that cannot
  be reached by another broker's resolver, retention to generation retirement, and the sweep
  and retirement arms for the new subject class. An attempt on 2026-09-10 ran **three
  plan-review rounds and three consults and shipped no code**: rounds 1, 2 and 3 dismantled
  three successive proofs that a broker which does not own an exchange may release its
  identifier, and the second escalation consult ruled the missing piece a **primitive that
  does not exist at HEAD**. That is F-098, which carries the three refuted proofs and their
  schedules. **Do not re-attempt them here**, in any wording — in particular "the evidence
  is the proof and the absence is only a check", which is releasing on absence by another
  name.

  **The lineage guarantee originally claimed for unit 1 is SUPERSEDED**, and the dossier's
  "The design shape, ruled" section now says so. Keying evidence on the lineage stands, but
  the claim that "old evidence cannot name a genuinely fresh exchange" does not:
  [MQTT-4.3.3-9] lets a receiver that sent a FAILED PUBREC accept a subsequent PUBLISH under
  that identifier as a NEW Application Message, and the client cannot see the lineage. **The
  lineage is correlation, not proof of one continuing obligation, and inheritance alone
  authorizes no release.**

  **What that loop SETTLED** (much of it about the retired `.done` design; kept so none of
  it is re-derived): the representation and lineage keying; the **latch** (evidence is
  owed when this owner's cleanup ends without a FIRST-ATTEMPT confirmed delete, and
  owed-ness is never withdrawn by a later observation); `deleteInflightRegisterFor`
  returns `true` for three non-deleting outcomes and `deleteInflightRegisterConfirmed`
  (`inflight_register.go:1405`) both swallows missing-sequence errors and retries
  internally, so neither layer means "this call deleted it"; a create-only rejection
  proves occupancy, not validity; the evidence ordering is against the **terminal source
  disposition**, not `sendAck` (the failed-affine arm disposes
  inside `mqttFinishQ2AffineDelivery` — **EXPERIMENTAL**, chapter P's, since that arm is
  reachable only on the affine path — while the drained arm reaches
  `finalizeDrainSource` as a switch condition and bypasses `releaseRegister`, which is
  supported); the teardown cannot reach a
  completion-only generation because `reapIncarnationRegisters` returns early on an empty
  register enumeration (`:1182-1184`); `collectInflightCandidates` would log every completion
  record as unattributable on every sweep tick; and two **pre-existing defects** are
  prerequisites — `condemnSendLocked`'s unconditional first-wins (`:5532`) and `endSendWith`
  never draining deferred NAKs (`:5470-5518`) where the acknowledgement handler does
  (`mqtt.go:19270`). (The first-wins precedence is now D4's: an answer replaces a parked
  non-answer, behind W6b.)

  **A HEAD DEFECT THE LOOP FOUND, now MET** (the matrix's different-source row; regression
  `TestF097DifferentSourceArmReservesAPossiblyAttemptedOccupant`).
  `storeInflightRegister`'s different-source arm (`inflight_register.go:4127-4162`)
  deletes a register whose source is provably complete **without examining the unsent
  marker**, and `mqttSlotDeleteComplete` proves only that the slot is in `publish`.
  Recovery says the opposite about the same record (`:7207-7216`, `:337-343`). A consult
  ruled recovery correct and allocation the defect. Counterexample, needing no completion
  record and no crash: a QoS 2 PUBLISH reaches the client, its PUBREC is lost, the source
  expires by its own Message Expiry TTL while its consumer still holds interest, and a
  later allocation reuses the identifier while the client still holds the exchange. **This
  was a source-supported counterexample and a required regression, not an observed
  failing test**; the regression now exists (above). The two rules interlock, which is
  why it had been invisible: recovery's
  reservation is what stops `bumpPI` from ever offering that identifier to the allocation
  that would reap it.

  Earlier carry-forward in `~/.claude/lanes/f-097/plan-item2.md`; the retired unit-2
  plan, at revision 3, in `~/.claude/lanes/f-097-unit2/plan.md`, with the ledger and the
  three consults beside it.

  **It has independently reachable defects at HEAD, with no cancellation involved**, which
  is what makes it a prerequisite rather than F-088's remainder. F-072 already prevents an
  ordinary marker-absent, source-complete PUBLISH register from being consumed by recovery
  (`inflight_register.go:7110` onward) — credited, not re-proved, and it is a protection
  against premature release, **not** a claim that cancellation safety has landed. The
  defect is what that protection leaves standing, and the schedule is concrete: an exchange
  completes and the acknowledgement handler correctly releases its live custody in memory
  (`mqtt.go:19244` untracks before any durable work, and the Receive Maximum slot is freed
  with it); the register's confirmed delete is then **lost, indeterminate, or aimed at a
  sequence a rewrite has moved**, so the durable record survives; and a later resume's
  recovery reads that marker-absent register, cannot prove the exchange terminal, and
  **reserves the identifier**. Nothing then releases it — there is no completion evidence
  to consult, and a resume is not a release mechanism, because retaining it is exactly what
  `:7110` does. The reservation is durable residue, not stranded live memory, and it costs
  **outbound** QoS 1/2 capacity for the incarnation's life; at Receive Maximum 1 that
  stalls the session's outbound QoS 1/2 traffic. (That was the schedule as filed. Under
  F-105 the acknowledgement's advance re-phases the register in the exchange's own batch,
  which is the matrix's retired "terminality in place is refused" row, and the custody
  ledger releases the charge on the exchange's ended read, not on the register's delete.)

  **F-088's D1 (RETIRED with the `.done` record by C1; kept as recorded) settled the
  REPRESENTATION and nothing else**: an immutable, create-only
  sibling `$MQTT.inflight.<idHash>.<gen>.<PI>.done.<nonce>` carrying no `Claim`/`ClaimSeq`,
  so a custody rewrite cannot invalidate an acknowledgement. Terminality in place on the
  register is refused — it reopens F-087's frozen five-writer set. **Exchange identity and
  lifecycle integration are UNRESOLVED**, and two identity failures are established.
  Nonce-only lookup loses completion across an N1 -> N2 replacement: N1 is acknowledged,
  its register survives because cleanup was delayed, a same-source replacement installs N2,
  and reconciliation asks about N2 while the landed sibling is `.done.N1`. And
  `(PI, Dur, Seq)` — proposed and **refuted** — wrongly joins **distinct** exchanges,
  because the QoS 1 / failed-PUBREC arm releases the register and then sends a
  **fire-and-forget** source acknowledgement whose own comment (`mqtt.go:19630`) reads "a
  lost ack here costs one duplicate PUBLISH, which QoS 1 permits": so a completed source
  **is** re-offered, and after identifier reuse a later exchange takes the same triple and
  matches the old evidence. The premise "a completed source is not re-offered" is **false
  at this HEAD**. The re-offer itself is existing, intended behavior, and `.done.N1` is not
  implemented — this is a counterexample against the *proposed* design, not a defect of
  current behavior. Retained sends are a second collision population, all sharing
  `(PI, "", 0)` within an incarnation. **Two open design shapes, neither chosen:** stable
  exchange lineage across replacement, or a proved suppression rule preventing a completed
  source from starting another exchange while its old evidence can still match. (Since
  SETTLED by lineage: the matrix above.)

  Not to be re-derived: the acknowledgement handler untracks memory **before** its durable
  release work (`mqtt.go:19244`), so a new allocation and a send can begin before the old
  acknowledgement's durable and source work finishes; `retainAndReconcileTerminal`
  (`pubrel_action.go:640`) is a per-terminal-**reservation** retry whose scheduling and
  admission pattern may inform a design but whose terminal-reservation semantics may not be
  imported unchanged, since a pending cancellation with no landed slot cannot enter it; and
  generation scoping works only if every delayed writer and reconciliation task uses the
  **captured** generation. R-B is carried narrowly — a crash before any durable
  consequence
  of the acknowledgement leaves no recoverable proof — and it excuses neither losing landed
  evidence nor omitting production retries. Evidence retention, cleanup and the storage and
  latency costs of whichever design is chosen are **unresolved and not pre-accepted**.

  **The deliverable contract as filed:** genuine durable completion evidence survives
  replacement of the **same** exchange, **never releases a different** exchange, and
  drives durable and memory release after a finite storage failure **without requiring
  another offer or a resume**, including at exhausted Receive Maximum. It is restated as
  C1's closure statement above, with C2's correction for allocation after an incomplete
  recovery. Full finding in `docs/findings/ha-arbitration.md`; earlier planning record in
  `~/.claude/lanes/replacing-allocation-lifecycle/` (its `plan.md` revision 2 was never an
  approved design).

**The replacing allocation's lifecycle destroys or abandons an inherited exchange —
CONSOLIDATED into F-088, whose bullet carries its current state.** Deliberately not a
checkbox: this is a pointer, not an item. Filed 2026-09-07 out of the exchange-QoS item
and carried here until 2026-09-09, when a planning loop's consult ruled that its
obligations and F-088's are one seam and must not be planned apart. Its three
interleavings, its acceptance matrix, the refuted in-memory "inherited" bit and its
seven-reader starting census now live in **F-088's bullet**, which owns cancellation
proof together with continuous custody; no obligation was dropped in the move.

- [x] **A closed-but-present delivery subscription under a live `cons[sid]`.** Filed
  2026-09-06 by the delivery-readiness item's plan-review round 1; **CLOSED by making a
  deferred QoS downgrade REVOCABLE within its packet.** One SUBSCRIBE packet carrying the
  same plain filter at QoS 0 and then at QoS > 0 used to leave the client granted QoS 1 on
  a subscription that delivered nothing for the rest of the connection, with the record
  naming a durable the packet then deleted. Widening the existing-consumer arm's predicate
  was refuted and is not what landed: the deferred delete was still scheduled, so
  reinstalling delivery would have raced a teardown the packet had committed to.

  **What landed.** The packet's deferred-delete list became the SINGLE authority — each
  entry carries the barrier its staging lowered and the verdicts it recorded, replacing two
  parallel slices whose three per-filter boundaries over three cardinalities could not
  express "withdraw the staging for this sid". A later occurrence re-establishing QoS > 0
  revokes the staging: barrier, terms and un-void in ONE critical section (it is the SPLIT
  that is the defect — a delivery between a reopen and a later refresh goes out at the
  PRE-PACKET QoS), with a READINESS REFUSAL (`0x80`) rather than a grant the broker cannot
  serve. The reviver's own failure re-stages through the same helper, never from the saved
  snapshot, because the reopened interval had live delivery whose new registrations belong
  in the packet's own membership-removal write. The at-most-one-live-entry invariant also
  removed a pre-existing double `CONSUMER.DELETE` for `[{foo,0},{foo,0}]`. Two test seams
  were added — a live-SUBSCRIBE per-filter failure seam (the existing one was restore-only,
  so the re-stage could only be asserted, never proven) and a reply-bearing observation of
  the closed-arm skip — plus a packet-scoped pause and a deferred-teardown counter, the
  last because that teardown's observable effects are idempotent.

  **Not claimed:** atomicity across duplicate occurrences of one filter; a prompt re-offer
  of the deliveries the barrier skipped (each waits out `AckWait` and can be overtaken);
  `DUP=0` on their first transmission, which is F-072's single derivation site and not a
  second marker here. The early-shared-save window (a void verdict made durable beside a
  still-pre-packet membership) is PRE-EXISTING and unwidened, asserted in both directions.
  Residuals in `docs/CONFORMANCE.md`; mechanism in `docs/ARCHITECTURE.md` under "A deferred
  QoS downgrade is revocable within its packet".

- [x] **A destroyed QoS 0 downgrade left the record naming its consumer.** Filed
  2026-09-06 by the revocable-downgrade item's plan-review round 2 as
  "`buildPersistedSessionLocked` keys `Cons` on membership PRESENCE, not positive QoS", and
  **CLOSED by releasing the reference at the packet's final-update boundary instead.**

  **THE BULLET'S OWN DIAGNOSIS WAS REFUTED, and the plan-review loop is what refuted it.**
  The defect was real: a QoS 1 -> 0 downgrade whose consumer is DESTROYED rather than
  converted persisted `Cons[sid]` in its own confirmed save and then deleted the durable,
  leaving the record naming a consumer that does not exist — and three recovery surfaces
  read that record as live. But attributing it to the builder's membership-**presence** test
  is wrong, and the obvious positive-QoS predicate **introduces a live-exchange loss**.
  Presence is what protects a durable that is live and OWED at a QoS 0 membership: an
  adoption moves a drain's durable out of BOTH drain maps into `sess.cons` at once while
  `sess.subs` still says QoS 0 until the final update, so a later shared filter's early
  persist writes a record in which a positive-QoS predicate names that durable **nowhere**,
  and an orphan sweep on any broker deletes it with the client's unacknowledged exchange
  inside it. That was measured, not argued: applying the predicate as a mutation kills
  `TestMQTTAdoptedDrainIsNamedInAMidPacketRecord`.

  **What landed.** `releaseDowngradedConsumersLocked` runs at the packet's final-update
  boundary, one step after `convertDowngradeCandidatesLocked` and inside the same `sess.mu`
  section that applies the membership — the asymmetry `d554f13` closed for the converted arm
  and left open for the destructive one. It does **not** require `dd.convert`, because a
  stream inactivity limit stops a descriptor from ever reaching the conversion and that is
  the route that can hold started exchanges. The `inLock` hook reports the release, so a
  packet that changes no membership still writes (a QoS 0 re-SUBSCRIBE over a filter already
  at QoS 0). A definite save failure un-releases first, filling only an absent sid, or the
  rollback's `sess.cons` walk would miss a durable this packet created. The builder, its
  predicate and its comment are **unchanged**.

  **Three refuted designs, not to be re-proposed:** the positive-QoS builder predicate
  above; releasing at the filter's own staging, which would let a mid-packet save
  describe a positive-QoS membership with no consumer and make resume reconciliation
  mint a second
  durable; and restoring the entry on the `saveMaybeLanded` arm, which would recover the
  prompt cleanup path and the charge refund but re-persist `Cons` under the QoS 0 membership
  through the unchanged presence predicate — the defect again.

  **Not claimed.** `:3217` is untouched, both halves: its prior-QoS-0 exemption and its
  restore-side residual stand, and no adoption timing changed. Two residuals are stated in
  `docs/CONFORMANCE.md` — the sweep window (accepted, because every route into it is
  destructive by construction, and **not** extended to adopted or converted durables) and
  the ambiguous-landed route's loss of both prompt cleanup paths. The `MaxAckPending`
  charge that route leaked was filed as a residual too, and **holistic round 3 refused
  it**: a charge nothing gives back costs a subscription slot for the life of the session,
  so a **refund-only landed continuation** now gives it back at the resolution. Refund
  only — restoring the `Cons` entry is the third refuted design above, and the durable
  remains the sweep's. Cross-broker convergence is proven as far as a broker that is
  GONE — the fixtures stop the owning broker and await its shutdown lifecycle before the
  peer sweeps, in both orderings — and an ABRUPT process death is not exercised, because
  an in-process harness cannot construct one. Mechanism in `docs/ARCHITECTURE.md` under
  "UNSUBSCRIBE drain".

- [x] **A quarantined packet identifier has no live-session release.** Filed 2026-08-30 by
  F-087's plan-review loop. **CLOSED 2026-10-11 by census: the mechanism no longer exists,
  so there is nothing to release.** F-105 7b (`e35961d`) deleted `quarantineForeignInflight`,
  the Shared claim contest, its source-keyed decline and the five Quarantine tests
  (`TestMQTTInflightQuarantineHoldsUntilResume` among them) for EVERY class, the
  experimental one included — not narrowed to it. Today a live same-delivery occupant is
  replaced with no claim comparison (on the authority-backed route the batch compares the
  connection's accepted entitlement); a replacement that fails or is refused, and an
  occupant naming another live delivery, give the allocation up to identifier exclusion,
  which has its own live reclaim; recovery runs no claim contest either, and every
  remaining `regForeign` reservation is installed by recovery and released by its staged
  release (which hands the identifier to exclusion), by reactivation or by a terminal or
  exclusion handoff. *History kept so it is not re-derived:* two live-release designs were
  refuted on one seam — an observed ABSENCE (round 10, a rival's late create-only CAS) and
  a cancellation TOMBSTONE (round 11, the rival's reap-and-retry consumes it) — because a
  fence any writer may lawfully consume fences nothing; the population was decided
  SUPPORTED, not chapter P (2026-09-06). Two settled conclusions, kept so a future live
  release design neither re-derives nor over-trusts them: F-088 does not revive
  release-by-tombstone (its lifecycle in `docs/findings/ha-arbitration.md` keeps
  "`Cancelled` and provably never transmitted → reap and free", which is exactly the
  tombstone), and reusing recovery's `ClaimSeq` ordering on the live path is EXAMINED AND
  NOT ESTABLISHED — ordering two stored snapshots gives no current right to take custody,
  which the tree's live custody transfer (`pubrel_action.go`) pairs with a current
  acceptance proof and an exact CAS.
  *The census is qualified, not a claim that every supported hold releases live.* Three
  holds with no live release remain, none of them this mechanism, each stated in
  `docs/CONFORMANCE.md`: a marker-opaque reservation installed mid-connection, which
  withholds all new allocation until a resume (stated by this change, in "An unaccountable
  identifier space withholds new QoS 1/2 deliveries"); a shared QoS 1 exchange whose source
  another member consumed ("A shared delivery another member consumed while its member was
  away stays charged"); and F-121's never-sent exchange read as possibly attempted. This
  change retired the quarantine's residual and mechanism text from `docs/CONFORMANCE.md`
  and `docs/ARCHITECTURE.md` and corrected the comments naming the deleted mechanism; no
  behavior changed. *Side findings, not worked here:* the "Shared-register takeover
  arbitration excludes unreadable records" residual in `docs/CONFORMANCE.md` and the "LOST
  arbitration" reservation passage in `docs/ARCHITECTURE.md` still describe the deleted
  resume claim contest (7b fallout); and mutation M26 (`sharedRecordUnsupported` reports
  every record supported) no longer fails
  `TestMQTTInflightUnsupportedSharedShapeIsRefusedBeforeTheContest`, because the 7b
  inventory gate refuses the same fixture first.

- [x] [F-072: Derive the outgoing DUP flag from MQTT
retransmission](findings/release-investigations.md#f-072).
  **DONE (2026-09-09): validation and four holistic rounds passed; round 4 approved with
  no
  finding after a sweep of every identifier-release path.** The sibling unsent marker is
  implemented, and the crash window holistic round 1 found in it is CLOSED by a
  user-authorized redesign (2026-09-08): the marker is now consumed — a
  CONFIRMED delete — as the record of **send intent**, after the frame is admitted and
  BEFORE the socket write, and nothing durable happens after the write. A surviving
  marker means **proven-unsent** (DUP=0, discardable as never sent); an absent one means
  **possibly-attempted** (DUP=1, identifier HELD, expiry and withdrawal refused, released
  only by the client's terminal acknowledgement or session destruction). The irreducible
  intent→write crash window is answered DUP=1 by design — an attribution no protocol can
  make exactly, accepted as the conservative reading (N-1.2 and A-1 of the plan revised
  by the user; T1/T2 unchanged and passing). Also landed: admission split from
  transmission with size decided after Topic Alias selection; the unresolved-intent hold
  (ambiguous delete → no write, no release, re-attempt never readback); retention of a
  recovered possibly-attempted exchange on an oversize refusal; the confirmed register
  delete before terminal cleanup's marker reap; and, separately, the reclassification
  lineage fix (`resumeLineage`). **This does NOT license a release: F-088 remains an
  unconditional alpha blocker.**
  **The diagnosis and design that follow are HISTORICAL — the state before the closure,
  kept
  as the record of what the fix had to satisfy. They are not current behaviour and not
  current obligations.** As diagnosed: `trackPublish` set DUP from the JetStream delivery
  count (`dcount > 1`) and it reached the wire through `mqttPubFlagDup`. MQTT-3.3.1-3 ties
  DUP to whether **the outgoing PUBLISH** is a retransmission, and a copy offered then
  refused before `trackPublish` produced no PUBLISH at all — so the next offer is a FIRST
  transmission and must carry DUP=0. F-071's single-publisher capture shows seven of eight
  messages arriving DUP=1 on their first client-visible PUBLISH. **An eleven-round plan
  loop (2026-08-29, `~/.claude/lanes/f-072/`, stopped at a pre-registered gate) settled
  the design, and it bound the attempt that LANDED it:** the durable fact is a sibling
  **unsent marker** (`$MQTT.inflight.<idHash>.<gen>.<PI>.unsent`) created with the
  register in ONE atomic batch (`$MQTT_inflight` accordingly now requires
  `AllowAtomicPublish`) and DELETED at its immortal exact
  sequence to record the transmission — because eight rounds proved the seam is the
  register **rewrite**: an in-place `Sent` field moves the record's sequence and every
  exact-sequence operation racing it is wrong (five cover mechanisms were built and each
  failed review; do not re-propose it, nor the plan's other refused designs — in-memory
  only, tombstone-style reaping of an unproven register, register-after-send, holding
  `sess.mu` across the socket write, dropping the send reservation, a durable terminal
  verdict, re-homing the durable half, and a per-session watermark).
  **The paragraph that follows is the PRE-CLOSURE statement of what was owed, retained as
  the record of what the fix had to satisfy. It is not a list of current obligations** —
  the "Delivered" paragraph below is authoritative for what landed, and what genuinely
  remains of the same-delivery contested-store lifecycle is now **F-088**'s, with its
  completion half **F-097**'s. As stated then: what remains open is narrow and
  F-087-shaped: the same-delivery contested-store lifecycle, and two one-sided recovery
  states (register present + marker authoritatively absent; register absent + orphan
  marker). **TWO deterministic tests were required and the second forced the durable
  fix** — both now pass, deterministically. (1) a pre-`trackPublish` claim/materialization
  refusal, then DUP=0 on the first PUBLISH; (2) a seam after the pair confirms and before
  the send, then restart, recover, re-offer, and DUP=0 on the first client-visible PUBLISH.
  Test (1) alone is satisfied by transient state while the crash path still emits DUP=1, so
  a fix passing only (1) has NOT closed this. Neither may be load-sensitive. Changing this
  acceptance was ruled the user's alone, and the user declined (2026-08-29).
  **Delivered:** the register and a sibling unsent marker on
  `$MQTT.inflight.<idHash>.<gen>.<PI>.unsent` commit in one atomic batch
  (`$MQTT_inflight` now requires `AllowAtomicPublish`, refused at startup if absent); the
  transmission is recorded by deleting the marker at its exact sequence; DUP is
  `mqttPending.transmitted`, one derivation site, the six `sess.deferred` DUP claims
  deleted (N-7.1); one operation-aware boundary `mqttDecideMarker` decides every marker
  disposition. Both acceptance tests pass, deterministic. Residual: a transmitted message
  re-offered under a FRESH identifier (a drain completion whose save failed after the
  PUBACK freed the original) sends DUP=0 -- honest per identifier, `docs/CONFORMANCE.md`.
  Every refused design is recorded in the finding.

- [x] [F-088: A transmitted delivery's cancellation must not free its packet
  identifier](findings/ha-arbitration.md#f-088-a-transmitted-deliverys-cancellation-must-not-free-its-packet-identifier).
  **Alpha blocker. CLOSED 2026-10-11 with F-097.** The joint change landed as `a4a01a1`
  (lane `~/.claude/lanes/f-097-after-f105/`). The close-out (lane
  `~/.claude/lanes/f-097-f088-close/`) witnessed PS-1, PS-2, W5c and W6a on production
  schedules. That reached two contract failures, both fixed in the same change ("The
  close-out" below). It lands
  with F-097, whose completion contract this item consumes; F-105 and F-106 (accepted
  2026-10-08) supply the custody this item builds on: the ledger holds an ordinary,
  shared or retained exchange's charge and identifier until the exchange durably ends.

  **The finding, as filed.** A pure `Cancelled` was written over **transmitted**
  deliveries too (via `purgeConsumerPendingLocked` and the `voidRegistersOnStream` ladder),
  and recovery freed the identifier for any `Cancelled` register without distinguishing,
  against [MQTT-2.3.1-4]. F-105's custody ledger had already removed the identifier harm
  (the ledger holds the identifier whatever the register says); what remained was
  liveness and service: the door destroyed a possibly-transmitted register on a tombstone
  or verdict proof, so a late acknowledgement found nothing to complete and the exchange
  was held to the generation's discard.

  **Closure statement (C1).** Cancellation does not falsely establish non-transmission,
  destroy required completion authority, or permit conflicting delivery; legitimate later
  acknowledgements can complete the supported exchange across resume.

  **What landed** (mechanism in `docs/ARCHITECTURE.md`: the release authority's arm 4,
  "One door", "Readers", "A known-false cancellation is cleared", "A late acknowledgement
  is bound before it arrives"; limits in `docs/CONFORMANCE.md`):

  - **A cancellation is not evidence of non-transmission.** Arm 4 of
    `mqttSlotReleaseAuthority` is marker-only; the evidence loader reads no verdict.
    Recovery classifies a cancelled register by its marker: standing, the genuine
    cancellation; absent, possibly attempted and restored acting; unreadable, opaque.
  - **The door refuses where it destroyed.** `endUnsentBeforeDestroy`'s no-marker and
    missing-exchange rows refuse; `tombstoneBeforeDestroy` is deleted; an unresolved
    withdrawal is recorded as owed (`mqttWithdrawOwed`) and retried by the custody
    dispatcher.
  - **Provenance (C4).** A cancelled register carries `cancel_origin` (`pending-create` |
    `occupant`), refused at decode when missing or unknown (no migration); the create-only
    pending fence has ONE disposal rule (`mqttSlotDeletePendingFence`), by provenance alone,
    and its retry owner is the exclusion-reclaim continuation.
  - **Known-false clearance** at the write boundary (`mqttClearsCancellation` +
    `mqttRefutedCancellation`), and an all-class marker probe in the admission classifier.
  - **`confirmInflightRegister`'s cancelled arm over a landed register-only replacement**
    (edit 5, STAGED) destroys and tombstones nothing; the placeholder goes, the identifier
    stays excluded. Acting-entry reconstruction and drain re-enrolment stay behind W5c.
  - **A transmitted drain-owned deny** (`releaseTransmittedIdentifier`) installs an
    AckRoute and the door preserves its register, so the late PUBACK or PUBREC completes;
    a successful PUBREC over the still-claimed drain source records the drain's PUBREL
    intent first, so the canonical PUBREL is emitted and the drain released (T29).
  - **Recovery exempts no cancellation**: an occupant cancellation whose exchange a
    complete census did not reach is reserved like any register (P-d2).

  **Split off: F-119** (the bullet below). A paused admission landing after its pending
  fence was disposed of, with no cancellation evidence surviving, is preserved and follows
  ordinary recovery, so the cancellation can be lost. Consult C6 ruled that a
  cancellation-durability gap, not this item's custody seam; HEAD's verdict lifecycle is
  kept unchanged here.

  **Witness results.** T36 (`TestF088PendingFenceDisposalPreservationMatrix`, rows 1-18d)
  witnesses fence disposal and preservation per route; row 17 is F-119's schedule. T15
  witnesses every door entrance: marker standing ends then deletes, marker absent always
  preserves. **W6b** reached a D4 defect and its fix landed (see F-097). PS-1, PS-2, W5c
  and W6a were open at `a4a01a1`. Their dispositions are below.

  **The close-out (2026-10-11).** Two brokers on one JetStream with real clients
  (`f088_cross_tenure_witness_e2e_test.go`). Each ordering is forced by an existing seam:
  the reserved send window, the certificate persist hold, and the eviction pause.

  - **PS-1 and PS-2 are REACHED, through a cross-tenure schedule neither plan listed.**
    1. Broker A acquires the output token.
    2. The client resumes on broker B.
    3. B's recovery reads the marker standing and the register carrying A's token.
    4. B's teardown treats the entry as proven unsent.

    When A's eviction is delayed, A really writes. The old connection read
    `PUBLISH pi=1` in every run. Ordering is a RACE, never an invariant. A-first and
    B-first were both observed.
  - **Two contract failures were witnessed and are FIXED.**
    - (I) After B's stale removal and tombstone, the client's legitimate PUBACK on B
      found no entry and no AckRoute. The exchange stayed at Publish with the identifier
      held: `TestF088CrossTenureLegitimateTransmissionThenCancellation`, 4 of 4 before
      the fix.
    - (II) A verdict-covered register that recovery restored acting could never
      retransmit. `beginSendLocked`'s nonce fence refused every send, so the client
      never received the message until the session ended: the cold PS-2 witness, 2 of 2
      before the fix.

    T5e had missed (II), because it acknowledges without needing a retransmission.
  - **The fix.**
    - A recovered ordinary register that still carries an output token is POSSIBLY
      ATTEMPTED. Its standing marker is kept as deletion debt, so the next sender
      confirms its delete before writing.
    - The classification's consumers ask one predicate, `mqttRecoveredProvenUnsent`.
    - A same-allocation resident is reclassified monotonically.
    - A successful acting restoration retires the superseded nonce verdict
      (`retireSupersededVerdictLocked`): on a fresh install and on the resident arm,
      again after a fork-repair resurrection.
    - `beginSendLocked`'s fence stays unconditional for a genuine cancellation.
    - The entry evidence is renamed `mqttSendEvidencePossiblyAttempted`.

    Mechanism in `docs/ARCHITECTURE.md`, under "A late acknowledgement is bound before it
    arrives". The fix rests on one invariant: ordinary acquisition is fenced by the
    accepted generation and tenure, so every token a displaced holder can still write
    under is visible at the successor's recovery read. The slot-only class is excluded.
  - **Witnesses after the fix.** Each passed 10 of 10 in the flake screen:
    - W-LEGIT: the legitimate PUBACK completes on B, with no re-offer or resume.
    - Acknowledgement before the holder's drain.
    - The persisted verdict over an absent marker: retired, retransmitted with DUP=1,
      completed.
    - The same with a standing marker and a recovered token.
    - The marker debt after a no-intent drain.
    - The resident repeat recovery.
    - The fork-repair resurrection.
    - A genuine marker-standing cancellation that still cannot send.
    - Both routing predicates.
    - The drain completing without a recovered `jsAckSubject`.
    - The AckRoute vetoes.

    The PS-2 cold witness is now a recovery regression over persisted state: under the
    fix B no longer produces that verdict. The F-121 B-first ordering is preserved
    (enrolled).

    **Mutations.** Ten of twelve were killed by their paired witnesses:
    - the token arm ignored;
    - either routing predicate reverted;
    - either retirement call removed;
    - the marker debt zeroed;
    - the AckRoute install disabled (boundary credit);
    - the verdict revision bump suppressed;
    - the resident's monotone update removed;
    - the standalone tombstone refused.

    Removing `markerSeq != 0` from `mqttRecoveredProvenUnsent` SURVIVED as an
    equivalent mutant: recovery never derives a decided, non-opaque, untransmitted state
    without a marker, so that conjunct changes no reachable outcome. Refusing `actionLive`
    in `provenUnsentReleasableLocked` is OBSOLETE under the fix: a recovered token is
    already classified transmitted. Per-mutation results are in the lane.
  - **Coverage limits, stated.**
    - The routed late acknowledgement after an UNANSWERED removal of a recovered possible
      attempt is BOUNDARY coverage only: none of the real non-drain routes removes such
      an entry.
    - No live-session service was established by the pre-fix synthetic probes. Those
      failed and are recorded in the lane.
  - **W5c is UNREACHABLE for the supported ordinary chain.** The custody ledger holds the
    identifier of every un-ended exchange, and `bumpPI` refuses it. The observed gate is
    `TestF105CustodyAllocatorSkipsALedgerHeldIdentifier`. The conditional edit-5 work is
    not owed. The affine class is excluded (chapter P).
  - **W6a is REACHED.** The resident reconnect's RecoveryDrop parks over the open section:
    `TestF088D4W6aRecoveryDropOverALiveSection`, with its `WithdrawnUnsent` end state
    asserted. No PUBLISH left the sealed gate, so D4 precedence was not exercised there.
    M16's credit stays with W6b.
  - **Filed rather than fixed, none gating this box.**
    - F-121: intent recorded after certification.
    - F-122: a displaced session's admission churn, which also starves the successor's
      advances while eviction is delayed.
    - F-123: a pre-existing dispatcher-fairness flake.
    - F-124: a drain's removal marked under a zero entitlement.

  **Open, outside this item's landing.** `claimRecoveredInflight` still deletes a
  slot-only matching-nonce tombstone on the `Cancelled` bit alone: a chapter P question
  (experimental affine), reported, not changed here.

  **History.** Before 2026-10-08 the seam was planned four times without an approved
  design (revision 7 of the second run; the joint revision 2 of 2026-09-09, ruled not
  ready), and on 2026-09-09 the former "replacing allocation's lifecycle" bullet was
  consolidated here (durable cancellation safety must not be split from memory custody).
  The item was reframed as a PROPERTY over a mechanically frozen close set rather than a
  list of release edges. F-088's D1 (the `.done.<nonce>` sibling) is RETIRED with F-097's
  `.done.<lin>` record (consult C1). The refuted proofs and counterexamples below are kept
  verbatim so none returns. Under the landed change, counterexample 1, the pre-landing
  verdict schedule and the N1 counterexample all relied on a cancellation licensing a
  release, which the marker-only arm 4 no longer allows, and counterexample 2's identifier
  is held by the custody ledger (F-105 row 5) whatever the scheduling map says; the rest
  are kept as stated, not re-judged here.

  **WHY IT STOPPED, and the counterexamples are recorded HERE rather than only in a lane,
  so losing that lane does not erase them.** One arm's disposition —
  `storeInflightRegister`'s same-source replacement, where a second allocation under nonce
  N2 takes over a transmitted exchange's slot at its exact sequence — has been designed and
  refuted **six** times: two dispositions in the first planning run, three plan revisions
  in the second, and two nonce variants a design consult refused outright. The four
  surviving counterexamples:

  1. **A landed replacement carries N2, and the matching-nonce writer can set `Cancelled`
     on it.** `markInflightRegisterCancelledPending`'s rebuild arm matches by nonce once the
     replacement has landed, and a **crash before confirmation** lets recovery free the
     outstanding exchange through the record's own `Cancelled` bit. **Removing the
     `CancelledRegs` nonce disjunct from recovery's classifier does NOT remove this route**,
     and an exact-sequence CAS arbitrates competing *writes* while proving nothing about
     transmission. There are **two** writers of that bit — `markInflightRegisterCancelledPending`
     and `markInflightRegisterCancelled` — and neither's fail-closed arms cover a matching,
     readable, PUBLISH-phase record, which is exactly the inherited case.
  2. **Custody is destroyed before any settlement point can hand it off.** `stageDrain`
     cancels a pending registration and calls `untrackPublishEntry`, removing **both** the
     pending entry and its source mapping — so `bumpPI` sees the identifier as available and
     Receive Maximum accounting excludes it. **Ordinary, uncancelled retry exhaustion** also
     reaches `abandonInflightRegister`'s live path and untracks entry and mapping. Later
     restoration cannot repair an allocation gap that already happened.
  3. **Persisting verdicts does not retry tombstone writes.** `persistVoidedRegs` persists
     the **session record** and returns immediately when the saved revision matches;
     `secureRegCancellations` attempts tombstones only for its own supplied batch. So a
     **finite** storage failure can cause **permanent** loss of O3 convergence — a
     never-transmitted allocation whose identifier is never freed — which refutes the
     "reduced promptness only" claim a revision made for that trade. Two further
     verdict-based release consumers need an actual record-proof delivery mechanism:
     `voidVerdictProvenLocked` and `releaseSettledVoidReservationsLocked`.
  4. **A past-PUBREC release with no PUBCOMP**, from combining an older PUBREL scan with a
     newer ready register. Recovery scans PUBREL records **before** registers, so between
     the two scans another broker's outstanding PUBREC continuation can store the canonical
     PUBREL, advance the slot to ready and complete the source acknowledgement; recovery
     then reads `!exact && sourceComplete` as finished and reaches
     `advanceFinishedSlotToTerminal`. **Source completion plus a snapshot absence can
     authorize release without PUBCOMP, and an exact-sequence CAS does not validate that
     cross-stream inference.** (An earlier explanation — "the PUBREL was never stored" — is
     WRONG: `phaseSlotToPubRelReady` reads back and validates the canonical PUBREL before
     advancing.)

  **Also retained, each verified against the code and each still owed:**

  - **The pre-landing verdict schedule.** A teardown stages N2, its tombstone attempt
  reads
    the *occupant's* N1 record and correctly refuses, the session-record fallback durably
    saves `CancelledRegs[N2]` — and *then* the replacement lands. **No post-landing teardown
    pass is needed**; the saved verdict simply becomes applicable, and recovery's nonce
    classifier frees it. So "PUBLISH-phase and uncancelled" is not sufficient for recovery
    safety.
  - **The N1 counterexample**, which refutes preserving the occupant's nonce instead of
    taking the slot: **N1 can itself carry cancellation authority**, because the replacement
    arm expressly admits an occupant whose cancellation exists only in `CancelledRegs`. If
    the replacement preserves N1 and then transmits, that verdict matches the **transmitted**
    exchange. Preserving N1 moves the hole rather than closing it.
  - **Two further source-completion reapers** beyond the main recovery drop:
    `resolveRetiredSource` and recovery's final retirement pass both remove suppression
    **and** reap the register with `mqttSlotDeleteComplete` after source `Complete`. The
    phase gate admits that disposition from PUBLISH, and **retirement proves PUBREC — a
    custody transfer, not PUBCOMP**. Each needs the suppression/register split with its own
    test and mutation.
  - **The non-shared source-completion hole — the RECOVERY route is REFUTED, the principle
    stands.** As originally filed: gating only `$MQTT_SHARED_`-prefixed durables is a proxy
    for the wrong fact, so after a denial or a Maximum Packet Size rejection preserves an
    identifier and acknowledges the non-shared source, the next resume's AckFloor
    classifier says `Complete` and recovery consumes the register. **That recovery route is
    no longer live**: F-072 routed the source-complete drop through the marker phase
    (`inflight_register.go:7110` onward), which retains a possibly-attempted,
    source-complete register instead of consuming it. What survives is the **principle** —
    *source completion is not exchange completion, for every durable class* — and the
    source-based reapers that are not recovery's, which is the retirement row above.
  - **The drain finalizer's deletion — the oversize route is REFUTED.** As originally
  filed:
    a started, unacknowledged drained delivery could fail Maximum Packet Size and reach
    `finalizeDrainSource` → `releaseInflightRegisterByIdentity`, deleting a PUBLISH register
    with no terminal client acknowledgement. **That route does not reach**:
    `mqtt.go:20930` returns through `oversizeDisposalAllowed` first, retaining a
    possibly-attempted entry whole and acknowledging nothing, and only a **proven-unsent**
    entry is disposed. The general statement — "the drain completed" is not itself a release
    proof — is retained, but **a reaching route must be demonstrated before it is treated
    as a defect** (`feedback_fixture_must_reach_its_route`).
  - **`purgeSharedPending` frees transmitted shared identifiers on a plain DISCONNECT** —
    no teardown, no crash. Its own comment states it: the group durable survives, "but the
    identifiers are freed". **CORRECTED 2026-10-08: this no longer holds.** The purge runs
    `purgeConsumerPendingModeLocked`, which RESERVES a possibly-attempted entry
    (`provenUnsentReleasableLocked` false: the entry stays, no register sequence is
    collected, no settle is parked) and, on a surviving disconnect, keeps its source
    mapping (`mqttPurgeSurvivingDisconnect`) so the group's redelivery is a retransmission;
    and a shared QoS 1 exchange is charged by the custody ledger since F-105 7a, so
    `bumpPI` does not re-mint its identifier.

  **Binding constraints on any next attempt.** Neither nonce equality, `regPending`,
  PUBLISH phase, nor sequence freshness proves never-transmission across inheritance, and
  **both writers' outstanding cancellation continuations matter**. Preserved durable state
  does **not** guarantee continuous memory custody, source mapping or Receive Maximum
  accounting. Source disposal is not client completion, and **PUBREC transfers custody
  without authorizing identifier reuse**. Completion reconciliation must be delivered
  **with** its dependent preservation changes. Failed and ambiguous stores need
  **separate**
  schedules, each followed by an actual subsequent offer and a recovery pass, and
  **production retry convergence cannot be demonstrated by manually writing the missing
  tombstone**. **Do not restart this seam under a new item name or with a stand-in marker**,
  and do not re-propose: an in-memory "inherited" bit (its close set was wrong — seven
  readers treat `regPending` as never-transmitted evidence, not three), the abandonment
  inverse, or terminality in place on the register.

  **F-072's contract is what this item consumed** (F-072 LANDED 2026-09-09): the
  register/`unsent`-marker pair in one atomic batch; send intent as a CONFIRMED marker
  delete after admission and before the socket write; marker present = proven-unsent,
  marker absent = possibly-attempted. F-072 deliberately left the cancellation route
  alone; this change made the marker the only non-transmission evidence there too.

  **The inherited allocation's lifecycle, consolidated here.**

  Filed 2026-09-07 out of the exchange-QoS item and consolidated here on 2026-09-09;
  independent of QoS. Its three interleavings and what this change does with each:
  (1) *cancellation after adoption, before confirmation* (`stageDrain` cancels on
  `ack.regPending` alone, and `confirmInflightRegister`'s cancelled arm reaped or
  tombstoned a register-only replacement): the cancelled arm now reads its `transmitted`
  argument and destroys nothing (edit 5, STAGED; drain re-enrolment stays behind W5c,
  which was not separately witnessed); (2) *cancellation before adoption* (the placeholder
  retains the allocation's nonce, so nonce equality is not proof of a live allocation):
  unchanged, and recorded; (3) *failed store after adoption* (retry exhaustion reaches
  `abandonInflightRegister`, which drops the entry and its source mapping, so the next
  offer is admitted under another identifier): witnessed by W5b and closed for the
  witnessed population by F-097's admission continuity.

  **One design is already refuted and must not be re-proposed.** An in-memory "inherited"
  bit on `mqttPending` honored at three cancel sites: its close set was wrong — **seven**
  production readers treat `regPending` as evidence of never-transmitted, not three — and
  `purgeConsumerPendingLocked` has **no transmitted arm** to fall back to, since it removes
  `cpending` before its loop, so skipping its cancellation branch falls through to
  `delete(sess.pendingPublish, pi)` and confirmation still finds no entry. This does **not**
  prohibit memory state derived from durable evidence inside a demonstrably complete
  custody design.

  **The 2026-09-09 census of `regPending` readers and `Cancelled` classifiers** (a
  starting inventory, with five corrections) was replaced by this change's structural
  tests: every entry removal states its reason (`TestF088EveryEntryRemovalHasAReason`),
  every cancelled reader carries an evidence disposition
  (`TestF088EveryCancelledReaderHasAnEvidenceDisposition`), and the cancellation-register
  producers are a frozen close set (`TestF088CancellationRegisterProducersAreFrozen`).
  `releaseCancelledReservation`'s pre-settlement release is recorded under F-119 as HEAD
  defect P-14#1. `abandonInflightRegister` is still the store's terminal return; a
  repair that merely retained a `regPending` entry would stall the session, which is why
  continuity is a record beside it, not a retained entry.

  **Three mechanisms were proposed on 2026-09-09 and REFUTED. Do not rebuild them.**
  (i) A condemn-then-read-then-act fence over the send reservation is **unsound**:
  `condemnSendLocked` only *parks* a continuation and `recordSendIntent` consumes its
  captured token without checking cancellation, so a sender can still delete the marker and
  transmit between the proof read and the destructive CAS — and `endSendWith` untracks for
  *every* nonempty parked settlement, which is itself a release edge a repair must change.
  (ii) Treating `transmitted = true` as a prohibition on sending is **backwards**:
  `beginSendLocked` requires `transmitted || markerSeq != 0` as **send authority** and uses
  the bit to set DUP. (iii) `(PI, Dur, Seq)` as a universal exchange identity is refuted —
  see F-097, which owns it.

  **Two routes previously listed here are DISPROVEN and are not defects**, recorded so
  they
  are not re-filed: the main recovery non-shared source-completion hole is **already
  marker-gated by F-072**, and the alleged oversize route to `finalizeDrainSource` is
  **blocked by `oversizeDisposalAllowed`**, which retains a possibly-attempted entry whole
  and does not acknowledge its source. What survives of that family is the general
  statement — *source completion is not exchange completion* — plus the two retirement
  reapers, where retirement proves PUBREC and not PUBCOMP.

  **Acceptance, as it now stands.** The failed-store and ambiguous-store schedules are
  distinguished (W5a: a definite failure, admitted once; W5b: landed then unreadable,
  withheld by continuity, then admitted once). The UNSUBSCRIBE-between-replacement-and-
  confirmation schedule is covered only in T17's boundary form (seeded occupant, real
  ladder). W5c's production chain is unreachable for the supported ordinary class, and
  PS-1 and PS-2 are reached and served, as recorded in "The close-out" above.
  The rest of the original acceptance list (a mutation per census site, the stalled store
  across a cancellation and a reuse, a route witness and positive control for every
  negative assertion) is carried by the change's test and mutation plan (plan revision 20,
  section 6) and is judged by the holistic review. The three false comments this item
  owed (`Cancelled` "NEVER TRANSMITTED", the `qos` field's "no durable 'was transmitted'
  fact", `drainClaimCancel`'s "nothing was transmitted") are corrected in the tree.

  Prior analysis: `~/.claude/lanes/f-088/` (the six refuted dispositions and every review
  round), `~/.claude/lanes/replacing-allocation-lifecycle/`,
  `~/.claude/lanes/qos2-exchange-downgraded-by-resubscribe/`, and the 2026-10-08 run in
  `~/.claude/lanes/f-097-after-f105/` (`plan.md`, `DECISIONS.md`, `review-ledger.md`,
  `witnesses.md`). Behaviour and residuals in `docs/CONFORMANCE.md` ("Capacity and
  identifier custody belong to the exchange", the replacement arm's deviation, and F-119's
  residual).

- [x] [F-119: Cancellation must survive pending-fence
  disposal](findings/ha-arbitration.md#f-119-cancellation-must-survive-pending-fence-disposal).
  **DONE 2026-10-12 for the supported classes; the experimental affine remainder is filed
  in chapter P.** Allocated 2026-10-08 by consult C6 of the joint F-097 / F-088 run (lane
  `~/.claude/lanes/f-097-after-f105/`, `DECISIONS.md`), which split this window off rather
  than close it inside F-088. What landed is at the end of this bullet; the history below
  is kept as it was analysed, with the C5 candidate design superseded. **F-119
  does not reopen F-088's custody seam:** the late landing below frees no identifier,
  destroys no custody or completion route and admits no conflicting delivery, so F-088's
  custody, identity and subsequent-offer obligations stay F-088's.

  **The schedule, at HEAD, and unchanged by F-088.** An allocation's admission pauses after
  its ladder's entry check, before its write lands. A teardown cancels the allocation while
  its store is still in flight: `markInflightRegisterCancelledPending` writes the create-only
  pending fence on the empty subject (`cancel_origin: pending-create`), and
  `tombstoneCancellationsOnStream` then forgets the nonce verdict at once
  (`forgetCancelledReg`, right after the fence lands). The fence is disposed of by its
  provenance alone (`mqttSlotDeletePendingFence`, through the one door). The paused
  admission then lands its register, unsent marker and exchange on the now-empty subject,
  and the process crashes and resumes with no surviving verdict.

  **The consequence: a cancelled delivery can later be served.** Nothing remembers the
  cancellation, so recovery classifies the late landing by the ordinary rules and, where the
  delivery is recoverable, may restore it acting and transmit it. This is distinct from a
  false completion (nothing is recorded complete, the exchange stays at `Publish` with its
  custody) and from a conflicting delivery (one lineage, one identifier, one exchange): it
  is a lost cancellation.

  **Two populations, with different settlement evidence.** The authority classes (ordinary,
  shared, retained) hold ledger custody from `openAdmissionCustodyLocked` until the ladder
  has returned and every attempt it submitted is settled (`tryRemoveAdmission`), so their
  settlement is observable. The experimental affine class (`storeInflightRegister`,
  chapter P) is slot-only: it opens no ledger custody, and after an ambiguous return it holds
  no abortable handle. Consult C5 gave two affine counterexamples to keying fence disposal
  on ledger settlement: an affine admission paused after its `stillOurs` check while the
  cancellation fences, so a disposer that sees an empty ledger deletes the fence and the
  create-only pair lands after it; and an ambiguous affine pair that commits after its
  placeholder was abandoned, including (P-15) an ambiguous attempt followed by a successful
  retry. T36 rows 5 and 6 observed, in their construction, the late affine pair cleaned up
  whole on its standing marker by the surviving cancelled placeholder's settle path, and the
  retry arm unreachable there (one write attempt; the ladder's ownership check refuses a
  retry once the cancellation landed). That is one construction's observation, not a
  guarantee.

  **P-14#1, a HEAD defect.** `releaseCancelledReservation` deletes the nonce from the
  verdict set (`cancelledRegNonces`) and the cancelled placeholder from the scheduling map
  before the admission's deferred settlement. Its one caller is `runInflightAdmission`'s
  entry check (`!stillOurs`), reached when the allocation was cancelled between tracking and
  the ladder's first read, before the ladder issues any write. For the authority classes it
  runs while the admission's custody reservation is still open (`finishAdmissionCustody`
  runs from a `defer` after the ladder returns); the affine entrance reaches it with no ledger
  custody at all; and unlike `releaseSettledVoidReservationsLocked` and `forgetCancelledReg`
  it installs no identifier exclusion. Because the ladder writes nothing after this exit,
  the deletion does not by itself let a late landing occur; it is a verdict retirement
  outside the settlement owner, which any design that keys retirement on settlement must
  route through that owner. The retention wedge of plan revisions 12 to 14 was a proposal
  that never landed and is not HEAD behaviour.

  **A candidate design, not an accepted one.** Consult C5 proposed one token-guarded
  cancellation-obligation record per (generation, nonce): an immutable instance token owning
  the cancellation state, its coverage revision, the exact fence identity, the admission's
  lifecycle and settlement and the disposal result, with `CancelledRegs` as its persisted
  projection, instance-bound callbacks, retirement only by proof and a named retry handoff.
  Plan review P-15 found six defects in it, and a revival must answer all six:

  1. its retirement transition E13 treated accepted ownership as never-landed proof, so it
     retires the verdict a landed, unmarked register still needs;
  2. E9 misses an earlier ambiguous affine attempt followed by a successful retry;
  3. `unvoidRegNoncesLocked` (a rolled-back packet, or a revoked QoS downgrade, withdrawing
     its verdicts) is a membership writer the record does not route, and needs a rollback
     transition of its own;
  4. it has no operation-start or attachment transitions (an attempt already writing, an
     admission that predates the record);
  5. E5's early return on a cancellation before opening leaves a pending cancelled hold
     permanently unsettled;
  6. a restored bare nonce carries no class or tenure provenance to rebuild an instance
     from.

  **HEAD semantics F-088 preserves.** F-088 keeps HEAD's verdict lifecycle exactly:
  `CancelledRegs` persistence on the session record with its save and credit paths
  (`proveVoidedRegs`, `creditVoidedRegsLocked`) and the opportunistic `persistVoidedRegs`
  backstop; retirement on a confirmed delete; the `unvoidRegNoncesLocked` rollback; the
  fork-repair union (`sess_fork_repair.go`); and the early `forgetCancelledReg`. The verdict
  still decides where only it can: recovery's genuine-cancellation arm reads
  `rec.Cancelled || cancelledNonces[nonce]` on a standing marker, so an admission that landed,
  whose cancellation was saved but whose occupant marking failed before a crash, is still
  withdrawn.

  **Acceptance, amended 2026-10-12 (lane `~/.claude/lanes/f-119/`, `DECISIONS.md` D1, a
  Codex ruling under the supported-scope rule).** A cancellation survives every relevant
  outstanding attempt and every crash boundary without false completion, custody loss,
  stranded reservations or unsafe retirement, for the ordinary, supported shared and
  retained classes and the supported recovery paths; retry ownership and retention costs
  are stated explicitly. "Both populations" no longer applies: the experimental affine
  population is chapter P's, and its counterexamples are NOT claimed fixed. **The old
  witness was a fixture that did not reach its route:** T36 row 17 injected the late
  landing with `landAdmission`, a FRESH authority read after the disposal, so it never
  witnessed a previously prepared batch; it now runs the schedule through the production
  path and asserts the late batch refused. Prior analysis: lane
  `~/.claude/lanes/f-097-after-f105/` (`DECISIONS.md` C5 and C6, `review-ledger.md` P-12
  to P-15, which C6 superseded for F-088 and carried here, and `witnesses.md`).

  **What landed (2026-10-12).** Read against F-105 7b, the supported schedule was mostly
  closed already: the pending fence is written by `opTombstoneRegister`, an authority
  transition, and every admission batch CASes the authority sequence it read, so a batch
  prepared before the fence can never land after it; every accepted CONNECT mints a fresh
  tenure, so a predecessor connection's admissions are refused. What remained was the
  fence's DISPOSAL: a plain confirmed delete with no ordering against an attempt prepared
  AFTER the fence and still outstanding (unsettled, or held by a live ladder). Now, at
  both door entrances, an ordinary pending fence of the serving generation is deleted only
  after (1) the deciding licence names the serving generation and tenure, (2) the custody
  ledger, being that generation's, shows no admission of the fence's nonce whose ladder
  has not returned, and (3) a confirmed `fence-pending-disposal` authority advance
  (revision +1, nothing else) has landed under that licence; a fence of a generation the
  stored authority no longer serves needs none of it, and preparing, reserving, absent or
  unreadable authority keeps it (`pending_fence_disposal.go`; mechanism in
  `docs/ARCHITECTURE.md` "The pending-fence disposal rule goes through the same door").
  The exclusion-reclaim continuation passes its current connection's licence — the sole
  current-binding exception to the licence-provenance rule (D2), matched by its exact
  shape in the licence census and stated in `docs/ARCHITECTURE.md` — and admits the
  advance as a background authority attempt. Five plan rounds (Codex rejected a
  local-ledger gate as defeated by a displaced broker's disposal, then refuted a
  scheduling-entry quiescence test with drain removal). Witnesses
  (`f119_fence_disposal_test.go`): W1 a paused pre-fence batch loses its authority CAS
  alone; W2 an unsettled post-fence batch is refused after the disposal's advance; W3 a
  displaced ladder cannot dispose of a serving fence; W4 the gated entrance converts and
  fences; W5 and W5b disposal refused while the nonce's ladder runs, including after a
  drain removed its entry (W5b's fence is planted: no production producer); W6 shutdown
  and foreground priority; W7 the store-decided generation branch; W8 a resident rebind
  keeps the old ladder visible to the proof; row 17 converted. Mutations M1, M2, M2b, M3,
  M4, M5, M5b, M6, M7a, M7b and the census mutations MX1 and MX2 each failed their
  witness; under M2b and M5 the cancelled allocation's batch COMMITTED, so the harm was
  observed on the production path. One contract change in an existing witness: T36 row 2's
  cancelling ladder no longer disposes of its own fence while it runs; the fence is
  disposed of once it returns. **Cost** (in `docs/CONFORMANCE.md`): one authority revision
  per disposal; a fence stays, identifier excluded, while its advance cannot land, its
  ladder runs, or its object has no current connection. P-14#1 is orthogonal (that exit
  submits no write) and F-088's verdict lifecycle is unchanged.

- [x] [F-071: Preserve same-topic ordering for affine QoS 2
deliveries](findings/release-investigations.md#f-071).
  **PARKED, NOT FIXED: post-alpha shared-QoS-2 work in chapter P.** It is a demonstrated
  defect rather than a flake, but NOT a spec violation, and the
  basis is weaker than three earlier drafts of this bullet claimed.** MQTT-4.6.0-5 is
  conditional on an *Ordered Topic*, MQTT-4.6.0-6 covers only **Non-shared** Subscriptions,
  and §4.8.2 does not extend it, so the specification permits reordering here.
  `docs/CONFORMANCE.md` makes no global ordering promise either. What exists is a **stated
  implementation property** in `docs/PORTING.md` — "each topic-filter+QoS stream rides a
  single ordered carrier ... on a single dispatch goroutine" — which
  sits under *Testing strategy* justifying why three upstream ordering tests were REWRITTEN,
  and is therefore load-bearing for them. The affine path breaks that mechanism by delivering
  copies through a `$MQTT_Q2AFF_*` consumer on `$MQTT_out`: a SECOND carrier. Before the
  2026-09-02 scope decision, its blocker status rested on that property plus the judgement
  that silently reordering one client's same-topic QoS 2 messages was unacceptable here
  regardless of the spec. It is now unambiguously parked outside the alpha in chapter P.
  That reclassification preserves the evidence and the assertion that found the defect;
  it does not call the behavior fixed or acceptable.
  Found by F-034's
  verification on 2026-08-22 and reproduced from a SINGLE publishing Client on one
  connection, each QoS 2 handshake completed before the next: 2 inversions in 120 attempts,
  each transmitting one arbitrary message first with DUP=0 while the rest followed in order
  with DUP=1. (An earlier capture used eight different publishing Clients, which §4.6 does
  not order; that claim was withdrawn.) The broker's own
  packet identifier — minted immediately before the synchronous register gate and enqueue,
  on a subscription `nats.go` dispatches with one goroutine — shows the ordering, so the
  harness is not the cause. Load-sensitive. **Not a spec violation** — see the basis in the
  bullet header; §4.6 permits reordering on a shared path.
  **Three of my four explanations were refuted, all by evidence already in the tree**, so
  read the dossier before proposing a fourth: an AckWait redelivery cannot do it (the
  fixture records FIRST arrivals and ignores duplicates); the fixture is exonerated by the
  identifier allocation; and a `regPending` register-store race is impossible, because
  `shim_engine.go` depends on `nats.go` serializing a subscription's callbacks. Since the
  callbacks are serialized, the reordering must already exist in the order JetStream
  OFFERS the copies — copy store or re-offer order, still untested.
  **Do not weaken the ordering assertion — STRENGTHEN it.**
  `TestQ2AffineOrderingPreserved`
  publishes from a fresh Client per message, so it asserts an ordering the spec does not
  require and would catch a single-publisher violation only by coincidence; a correct fixture
  publishes from one Client. What is also owed is instrumentation correlating source
  sequence, copy sequence, claim phase, callback entry and enqueue.

- [x] [F-035: Bound every courtesy DISCONNECT
write](findings/runtime-correctness.md#f-035).
  **Closed by RESTORING a bound this port dropped, not by the watchdog the bullet asked
  for, and the difference is the finding.** The per-site design was reviewed twice and its
  ENUMERATION faulted five times — arming sites went 3 to 7 to 9 — while its shape was never
  faulted. That is the enumeration being the defect
  (`feedback_close_the_class_with_a_property`). The property those sites approximate has one
  home: upstream nats-server bounds every client write with `opts.WriteDeadline`, clears it
  after, and closes a missing client as `SlowConsumerWriteDeadline`
  (`DEFAULT_FLUSH_DEADLINE` = 10s). This port lost it when the shim replaced upstream's
  `writeLoop` and outbound buffer with a synchronous write under `wmu` and a no-op
  `writeLoop`, leaving `writeSocket`'s deadline parameter with one caller.

  **What landed.** `writeSocket` installs `mqttWriteDeadline` itself on every write and
  CLEARS it before returning; `queueOutboundBy` (since F-105 WP-F W1,
  `writeActionlessFrameBy`) carries the shutdown drain's absolute cap
  (`min(drain deadline, now + mqttWriteDeadline)`, an already-past cap installed as-is);
  and `writeDirect` is deleted. No per-site arming, no licence audit, no source guard, no
  timer, no new goroutine anywhere. The slow-consumer disconnect is `queueOutbound`'s
  existing close-on-error path, so there is no new close path and no new `ClosedState`.

  **What was planned and DROPPED, by a Codex scope ruling during implementation.** The
  plan also moved the notification into the close goroutine at the three displacement
  tails that own both (`discardTakenOverClient`, `disposeForeignSessionLocked`, the
  remote-persist eviction tail), to take the notification's write off those tails
  entirely. Implemented, it failed **nine** existing tests; reverting only that move
  turned all nine green. Eight read the DISCONNECT claim immediately after the displacing
  call returns and merely race the notification, but the ninth
  (`inflight_rewrite_test.go`, "endSend must issue the deferred register delete") asserts
  a durable register delete, and 5/5 runs failed with the move against 5/5 passing
  without — so it is not the `TestMQTTInflight*` intermittent population filed below. The
  move is not load-bearing for the claim above, since the deadline already bounds those
  writes; it is a scheduling change that needs its own analysis and its own oracle
  strategy, not a rider on a bounded-writer repair. Two mechanisms stating one fact, and
  the seam between them costing nine tests, is `feedback_delete_redundant_mechanisms`. Not
  filed as a roadmap bullet: nothing is owed. The inline form is correct and bounded.

  **The clear is not tidiness**, and its absence was the one real defect a review round
  found in the replacement: `c.nc` may be a `*tls.Conn`, and a TLS 1.3 KeyUpdate reply
  is written from `Read` without passing through `writeSocket`, so a deadline left in
  place
  expires through idle time and gives that write a PERMANENT outbound error no later MQTT
  write can repair.

  **What is claimed, and only this:** the deadline removes indefinite blocking caused by a
  NON-READING MQTT socket, under ordinary transport and scheduling assumptions. It puts no
  figure on the ownership gate's hold and does not bound work that is not a socket write —
  one `c.mu` section can contain many bounded writes, and a gate-held Will phase can
  wait on `subsBarrier.wait`, which has no elapsed-time bound (its extension by later
  SUBSCRIBEs, filed below, was closed on 2026-10-12).

  The bullet's own three "do not repeat" shapes remain correct, and none of them is this
  fix: it is not a synchronous write on the readLoop, not a goroutine that writes then
  closes, and not `writeDirect` with a deadline — the bound is installed inside the single
  writer, so it covers what a caller-supplied deadline never could.

  **Owed, and explicitly deferred by the same ruling** rather than dropped: the plan's T6
  (the nine gate-held routes driven through their actual producers) and the TCP-specific
  half of T9 (a real socket-buffer backpressure observation). Both are route and transport
  coverage around a property that is itself covered and mutation-tested at its single
  home; the slow-consumer lifecycle chain — blocked write, deadline failure, raw closure,
  ordinary teardown — ships here over a real readLoop instead.

- [x] **A delayed Will's `FireAt` is anchored to CONSUMPTION, not to the close.** Filed
  2026-09-06 by F-035's plan review, which ruled it separate work rather than absorbing it.
  **CLOSED for the ANCHOR and the deferred-outbox interaction; the takeover half was SPLIT
  to the bullet below by an explicit plan-review ruling** after 7 plan rounds
  (findings 9 → 10 → 6 → 5 → 3 → 2 → 0).

  **The finding.** `mqttHandleWill` computed `FireAt` from `time.Now()` at CONSUMPTION,
  and
  between the close and that moment sit the session gate, a clean-session discard and two
  JetStream round trips. So a Will consumed late fired late — and, the sharper harm, the
  window in which a reconnect could still suppress it was WIDER than the client
  configured, so an acceptance dated after the delay had elapsed from the true close
  could still cancel a Will that was already owed. **The suppression predicate was never wrong:**
  `mqttAcceptFactQualifies` rejects an acceptance at or after `FireAt`, and its own comment
  states the rule. Its INPUT was wrong. F-035 made it more reachable — a write timeout
  is a new producer of a late close — but did not create it.

  **What landed.** One field on the CONNECTION next to the disposition, stamped by
  `mqttOweWill` on **both** owed arms, consumed with the Will and its disposition by
  `mqttTakeWillLocked`, cleared on a discard, and **restored by a refused
  deferred-disconnect park** (that restore was later deleted, the take moving into the
  park's commit — see the takeover-settlement bullet below) — the three are one fact, and
  a rollback restoring two of them would leave an owed Will with no anchor that no later
  `mqttOweWill` can repair, because the owed disposition makes that call a no-op. On the
  connection rather than the session because a takeover rebinds the session before the
  displaced connection finishes closing. One helper, `mqttWillFireAt`, states the deadline
  for both producers and takes the expiry **by value** because every caller holds `c.mu`.
  A Will consumed still ARMED has had no close observed separately, so the consuming
  route's own instant is the anchor; an OWED Will with no anchor is an **invariant
  violation**, classified caller-side because the two zeroes are indistinguishable to the
  arithmetic, and its degraded boundary is a residual rather than a claim.

  **And the deferred-outbox interaction, which the bullet's own text required.**
  `dischargeDeferredWill`'s arm no longer diverts an already-due delayed Will to a direct
  publication: that skipped the acceptance-fact scan entirely, so a reconnect that genuinely
  occurred during the Will Delay — possibly on a broker this one never observed — was
  ignored. Anchoring the deadline makes that bypass MORE reachable, which is why the two
  are one item. `enqueueDelayedWill` now classifies its outcomes **per EXIT, never by error
  value**: six of them, exactly one retainable, and the ordering that forces the
  distinction is an exhausted attempt whose fail-safe refuses the destination — it returns
  the same sentinel a pre-effect refusal does, but its create may already have landed, so
  retaining on the value would write a second record for one Will. An unavailable or
  mismatched destination is RETAINED on its own queue and its **enqueue re-attempted**,
  carrying the original `FireAt` and an immutable `WillID`/hash so every attempt is the same
  create-only CAS — the identity is therefore allocated **before** the retainable refusal.
  Not the immediate-Will queue, whose recovery PUBLISHES, and with its own budget so a
  delayed backlog cannot starve the Wills that are due now.

  **What it does NOT claim.** Two pre-existing exits still publish a delayed Will without
  arbitration (writer-validation refusal, and store exhaustion) — the delay is sacrificed
  rather than the Will, at a cost of four things including a possible DUPLICATE
  publication, and this change routes MORE expired actions through them. Immutable identity
  is **not** durable deduplication. Retention is in memory, so a restart, its capacity
  bound and a never-returning account still lose the Will. Deadline correctness is not
  delivery promptness: the interval between the close and the consumption is unbounded, and
  `c10f049`'s write deadline does not cover it. The sibling `:3903` is **untouched** and
  anchoring does not fix it — a Will held behind that barrier still publishes late, it
  merely no longer has its delay restarted. Residuals in `docs/CONFORMANCE.md` under
  residual 8; mechanism in `docs/ARCHITECTURE.md` under "Why dated".

- [x] **A takeover before the enqueue destroys a delayed Will whose delay had already
  elapsed.** Filed 2026-09-07 by the `FireAt`-anchor item's plan-review round 2 and split
  out of it by an explicit ruling that the seam is **obligation ownership and completion**.
  **DONE 2026-10-12** after 5 plan rounds (findings 4 → 3 → 2 → 2 LOW → 0) and one scope
  consult.

  **The finding.** The takeover tail's `mqttDiscardWillOnTakeover` destroyed every Will that
  was not owed immediately, with no date test. A v5 connection whose close had been observed
  (`mqttOweWill` → owed-delayed) and whose teardown then lost the gate to a same-client-ID
  CONNECT exited through `mqttPublishOwedWill`, which publishes only an owed-now Will, and
  the tail then discarded the owed-delayed one: a delay that had already run out was lost,
  and so was the Will of a connection whose replacing CONNECT was refused before its
  acceptance batch committed.

  **The refuted fix stays refuted.** Promoting an elapsed owed-delayed Will to owed-now in
  the tail can run after the only publisher returned (silent loss), and an elapsed local
  deadline says nothing about an earlier qualifying acceptance on a broker this one never
  observed. Having the displaced teardown enqueue at its lost-owner exit was rejected too:
  the session it would sample is already the successor's, so the baseline would sort above
  the successor's acceptance fact.

  **What landed.** The takeover destroys only an ARMED Will. `mqttSettleWillOnTakeover`,
  applied by the tail (and by T1b's `applyWill`, which runs before it) to the connection
  it displaces, discards an armed Will, leaves an owed-now one to the teardown, and
  consumes an owed-delayed one into the outbox through the existing parked-action producer
  (`mqttTakeWillIntoActionLocked` + `dischargeDeferredWill`) with its close-anchored
  deadline and the displaced incarnation's baseline — the retiring `mqttWillFence` on a
  clean start, the session sample on a resume, both before this CONNECT's acceptance
  batch. Suppression is then the acceptance-fact predicate alone. The displaced side's
  rebound arm applies only the armed half (`mqttDiscardArmedWill`). Plan review found the
  handoff was not total while the ungated deferred-disconnect park TOOK the Will before
  knowing it would commit and RESTORED it on refusal: a takeover or a disposal consumed
  the empty slot, then the restore stranded the Will, and the park could install against a
  successor's uncertainty. The restore is **deleted**: the take moved inside the park's
  commit (`parkClose`, one `sess.mu` section with `c.mu` nested, checking uncertainty, an
  existing action and the binding before sampling the baseline and taking), so every
  refusal names its consumer (table in `docs/ARCHITECTURE.md`). The existing-action
  refusal while still bound — a parked action of an EARLIER connection, reached by a
  drain-timeout repark and a resume — used to take and drop the closing connection's Will;
  it now discharges it from that connection's own baseline (consult C1).

  **Witnesses** (`will_takeover_owed_delayed_test.go`, all interleavings forced by seams):
  the elapsed Will published once through a confirmed record with the teardown already
  returned, and with the takeover deciding first; not-elapsed suppression on a resume and
  on a clean start with the record's origin identity asserted; an elapsed Will suppressed
  by a planted earlier acceptance; the ungated park against a settling takeover, a
  successor's uncertainty and a consuming disposal; the occupied park; T1b. Mutations
  M1-M10, each paired with a failing test.

  **What it does NOT claim.** The Will is in memory only between the take and a confirmed
  create, and the enqueue's fail-safe exits still publish without the delay (residual 8).
  On the occupied-park arm the closing connection's lifetime obligations are still not
  recorded. A disposal meeting both a parked action and a live client still completes only
  one of the two Wills — **F-125**, kept as a `docs/CONFORMANCE.md` residual rather than a
  roadmap item (rare: needs a drain-timeout repark followed by a resume). Residuals in `docs/CONFORMANCE.md`.

- [x] **`subsBarrier.wait` has no elapsed-time bound and its generation can be EXTENDED by
  later entrants.** **DONE 2026-10-12: the generation extension is gone; no elapsed-time
  bound is claimed.** The barrier is now a set of entries, one per `processSubs` run, each
  with its own completion channel, and `wait` snapshots the live set and waits only for
  those entries, which is the contract its comment had always stated. So overlapping later
  SUBSCRIBEs can no longer hold a QoS 1/2 store or a Will phase's gate. The guarantee is
  unchanged: interest a sampled `processSubs` establishes is registered before the store.
  There is deliberately no timeout, because the fan-out precedes the wait and abandoning it
  trades a delay for an interest-retention LOSS. **Residual** (`docs/CONFORMANCE.md`, the
  barrier entry and the lease-duplicate boundary): a wait lasts as long as the slowest
  SUBSCRIBE in flight at its sample, a duration set by that processing's own JetStream
  work. A double `leave` now panics rather than releasing another entrant. Tests:
  `TestMQTTSubsBarrierWaitIgnoresLaterEntrants` (unit, ordered by a new sampled seam
  `mqttTestSubsBarrierSampledHookV`, not by sleeps) and
  `TestV5PublishWaitsOnlyForSubscribesItSampled` (two SUBSCRIBEs held in the barrier on a
  real broker). Mutations, each killing its paired test: M1 waiting until the barrier is
  empty (the old semantics), M2 returning at once (the wire test then LOSES P1, and
  `TestV5SubsBarrierHoldsAPublishUntilInterestExists` fails), M3 `leave` not closing, M4
  ignoring abort (also `TestMQTTWillOutboxShutdownUnblocksBarrier`). Plan review: 4 rounds.
  Original filing follows. Filed 2026-09-06 by F-035's plan review; a confirmed liveness defect
  found by that review, not created by it, and not closable by it. `enter`, `leave` and
  `wait` share one count and one channel, and only a transition to zero closes it — so,
  contrary to the barrier's own comment, later subscribers extend a generation an earlier
  waiter already sampled. Continuously overlapping SUBSCRIBE operations can therefore
  keep a QoS 1/2 Will waiting indefinitely, and with it the gate that Will phase is held
  under, reached via `mqttHandleWill` → `mqttInitiateMsgDelivery` →
  `mqttInitiateMsgDeliveryJSA`. CONNECT restoration also enters the barrier. Shutdown's
  abort channel is not a normal-operation bound. Fixing generation membership needs its own
  correctness argument about subscription interest and message retention, which is why
  F-035 did not absorb it.

- [x] [F-041: Persist the MQTT 5 DISCONNECT expiry
override](findings/session-and-will.md#f-041).
  **Medium. DONE — the override is an OBLIGATION of the accepted connection, persisted at
  the close and applied on every park and every discharge branch.**

  **What was wrong.** A v5 DISCONNECT's Session Expiry Interval reached
  `sess.expiryInterval` and `c.mqtt.sessionExpiry` in memory and **never** `$MQTT_sess`,
  and it moved the interval without `clean`. Two defects, opposite directions, one cause —
  which is why the fix is not conditioned on the override being zero. Lowering to 0 left a
  record saying `Clean=false` with the pre-override interval, so a discard that did not
  complete let a reconnect resume a session the DISCONNECT ended and report Session Present
  true (Spec5 [3.1.2.11]). Raising to `mqttSessionNeverExpire` left a **finite** record with
  `DisconnectedAt=0`, which `sweepSessionExpiries` stamps and arms a timer for —
  deleting a session the client asked to keep forever. **Defect B was found by
  plan-review round 1 and is in neither the bullet nor the dossier**, and it is why
  persisting the interval alone
  would have been strictly worse than persisting nothing: the resume decision reads `Clean`,
  `overdue` needs a positive interval, and the sweep skips zero, so such a record is both
  resumable and unreapable.

  **What landed.** Three facts kept apart, which is the whole design: the session's
  **confirmed** terms, a CONNECT's **requested** terms (speculative, refusable, and which
  must never reach the close), and an accepted connection's **DISCONNECT override** — a new
  `mqtt.lifetimeOverride`. The packet records the third and **no longer touches the session
  at all**, so it needs no ownership test; a displaced connection's late DISCONNECT records
  an obligation on its own client struct and its close finds the session rebound. The close
  applies it through the CONNECT's own helper under a new `mqttLifetimeFinal` finality,
  in its own record transaction, re-checking uncertainty **with the fence held**, and
  handles all five classifier outcomes **per outcome** rather than through one shared
  fall-through — a disposing verdict is ownership, not persistence, so continuing would run
  `purgeSharedPending` and a second save against a record that is no longer ours. Under
  `Final` a definite failure does **not** restore the pre-override terms: no refusal is
  possible, so restoring would silently ignore the client's instruction. `clean` is derived
  by one helper, `mqttSessionIsCleanForExpiry`.

  **Both discharge branches apply it — when the action's interval IS a lifetime.** The
  holistic review caught the last instance of this item's recurring defect class, one
  representation carrying two facts: `action.expiryInterval` is the terms a close ends on
  for a park built from a connection, and a bare discard **instruction** for the action
  `reparkRefusedDiscard` synthesises — which is how **ordinary expiry recovery** retries,
  for a session whose real lifetime is FINITE. Persisting that zero would have written
  `Clean=true`/`ExpiryInterval=0` over the finite record, and the sweep skips interval-zero
  records, so a later refused `clear()` left it **unreapable** where before it had a
  deadline the sweep could retry — strictly worse than not writing.

  The first fix drew that boundary at "carries a lifetime" and **widened the defect while
  claiming to narrow it**: an ordinary parked close with no override then also ran the
  backstop, adding a record store to every one and letting an AMBIGUOUS store repark a
  discard HEAD simply attempts. The gate is `lifetimeOverridden` — only an override creates
  an obligation, and only an obligation is written.

  Otherwise the rule is stated as a property over every branch rather than
  patched per arm — rounds 3, 4 and 5 each found exactly one unfixed arm. The surviving
  branch writes the pair beside its stamp in one save; the discard branch writes the pair
  **and the action's stamp** in a transaction of its own before `clear(true)`, because
  `clear()` can fail without deleting anything and a drain timeout reparks with no
  uncertainty armed. The stamp is not optional there: both resume checks need a **positive**
  timestamp, which is what makes the `expiredBy` half of that clause work.

  **A whole sub-programme was DELETED rather than shipped**, and that is the most useful
  thing this item records. Revisions 7-9 also anchored the close's expiry deadlines at an
  earlier close observation. Rounds 7, 8 and 9 produced four blocking findings and every one
  was the same defect: **six** production sites derive an expiry decision from one
  seconds-precision `disconnectedAt`, so an earlier anchor corrupted a different consumer
  each round — round 9's being `mqttProcessConnect`'s own resident `overdue` check, the
  resume decision this item exists to make correct. Two successive enumerations of those
  consumers named three, then five, of the six. The programme was deleted by an explicit
  scope ruling; F-041 now changes no clock source, converts no unit and rewrites no deadline
  formula. **Do not re-propose anchoring the expiry deadlines earlier** without first
  enumerating all six consumers and proving the ordering against a nanosecond `FireAt`.

  **What it does NOT claim.** Round 2's P1 4 — a park's stamp dated late by the gate
  spin — is **not** closed. The extra record write adds expiry lateness with no demonstrated
  sub-second bound, and it can change Session Present at a one-second interval. The
  pre-existing coarse-deadline-versus-`FireAt` inversion survives on the resident, parked
  and record-driven routes. No cross-broker ownership semantics for `Clean`, which is a
  resume predicate — destructive arbitration stays in `claimTeardown`.

  Three more exclusions, stated here rather than left to the other two documents: a
  **definite failure** of the lifetime write leaves the pre-override terms durable, so the
  old deviation stands for that intersection and the close reports it at warning level; a
  **crash before the first write** is not covered, because no durable state can precede
  it; and a **refused discard still leaks** the record, its consumers and the generation's
  registers with no resident object — what the backstop changes is that the leak is no
  longer also a resumable session. Residuals in
  `docs/CONFORMANCE.md` under "The DISCONNECT lifetime override is durable, and expiry
  TIMING is not repaired"; mechanism in `docs/ARCHITECTURE.md` under "The DISCONNECT
  lifetime override is a connection's OBLIGATION".

  **Shape:** 10 plan-review rounds across two runs (blocking 3, 5, 2, 1, 1, 0, 2, 2, 2,
  0),
  zero relaunched, plus two scope consults. Rounds 1-6 were planned against `a8635fe` and
  never approved; the re-anchoring to `2231651` cost rounds 7-10 and one deletion.

- [x] [F-039: Restore shared membership during SUBSCRIBE
rollback](findings/runtime-correctness.md#f-039).
  **CLOSED by restoring both maps, which is one of the two courses the bullet offered.**
  "Make the release walk both" is refuted — not as useless, but as leaving a **permanent
  hole**: a broker restart or a record-derived expiry has no second map to consult, because
  the close save has already removed the record's claim and group configs are excluded from
  the persisted `Cons` by construction. Restoring the membership closes it at the source,
  and a union would have been a second derivation of a fact `sharedSubs` already states.

  **What landed.** `rollbackSubscribePacket`'s **definite-failure arm only** restores
  `sharedSubs`, `rapSubs` and `subIDs` for the shared keys it dropped, captured immediately
  before the compensation — the same sample `rollbackSharedSub` takes for one filter. The
  pre-packet snapshot is the wrong value because the eager per-filter membership write goes
  to the record **before** its group consumer exists, so the packet already made a durable
  attempt the final save's failure did not undo. Without the restoration the next routine
  save — the disconnect stamp, or `clear()`'s teardown scan — dropped a holder whose group
  durable was live, and **no sweep reclaims that**: `$MQTT_msgs` is interest-retained and
  the only route that deleted a consumer from a candidate listing there is gone. "Small"
  described the diff, not the blast radius.

  **The producer survey found `readd()` the only site producing a PERSISTENT mismatch**
  across an enumerated close set — every membership mutation and every `cons` insertion,
  with the transient ones listed and their owners named. **No exhaustive sole-producer
  proof is claimed**, and the sweeps, unions and derivations that reasoned from this defect
  as a tolerated fact are all **kept**: only their justifications are corrected, because
  closing one producer says nothing about the rest.

  **Three details of the restoration are load-bearing.** The capture is **filtered by
  membership and copied** — `rapSubs`/`subIDs` carry plain filters too, so a wholesale
  copy leaks a plain filter's option into a record whose subscription was rolled back. The
  restoration **writes through the `snap.shared` alias**, which is safe because the
  survivor set is copied before the alias is installed and the only other reader of that
  snapshot is unreachable on this arm — structurally, not by timing. And it is the
  **definite arm only**: an ambiguous rollback's bytes may be what landed, and the
  resolver settles them by re-attempting them.

  **The downgrade lifecycle is orthogonal by DISPATCH, not by survey.** Both
  `stageDeferredDel` call sites sit inside the non-shared branch, so no shared filter can
  ever become a QoS 0 conversion candidate and the un-conversion machinery cannot meet this
  fix; its own critical section also closes before the rollback is entered.

  **One claim this item proposed and then withdrew:** a "pre-existing"
  successor-connection
  alias race on the ambiguous arm. It is **not established** — CONNECT resolves an uncertain
  resident session before proceeding — and the aliasing comment it was inferred from
  establishes the aliasing, not that interleaving.

  **What it did NOT fix, at the time:** unreclaimable group durables across every crash
  window — the release ran after record removal, an indeterminate last-holder scan failed
  closed, and the scan and the delete were separate operations (history: all three CLOSED
  by F-105's shared-group lifecycle transfer, 2026-09-22). Residuals in
  `docs/CONFORMANCE.md`, including a **truth correction** to one whose "cleanup falls to the
  orphan-consumer sweep" was false for a `$MQTT_msgs` durable; mechanism in
  `docs/ARCHITECTURE.md` under "A SUBSCRIBE rollback restores the shared state it
  attempted".

- [x] [F-045: Release stranded shared pending state at membership
removal](findings/runtime-correctness.md#f-045).
  **Closed by TWO items, and the joint provenance is the point — neither one changed both
  sites.** F-045 tracked one defect at two removal paths: their purges walked only
  `cons`-derived durables, so a record-restored session (membership without configs)
  stranded a
  TRANSMITTED delivery's `cpending`/`pendingPublish` entries until the close, holding packet
  identifiers and Receive Maximum slots meanwhile — under a Receive Maximum of 1, a
  session that
  can no longer deliver at all. **UNSUBSCRIBE's no-`cons` branch was closed by F-083**; the
  **3.1.1 forced leave** by the forced-leave item in §0.2, which derives its pending
  purge from
  the pending maps at both its staging and its commit half. `purgeSharedPending` remains the
  backstop for a session that simply disconnects.

- [x] [F-050: Refuse or retry an incomplete shared-subscription
restore](findings/shared-qos2.md#f-050).
  **Closed 2026-08-23 by refusing, not retrying.** CONNECT no longer reports a resumable
  session whose delivery path was not restored: subscription restoration runs BEFORE the
  success CONNACK, and a filter it cannot install refuses the CONNECT through the abort
  discipline that already discards the Will and leaves the requested lifetime ungranted —
  which is what the F-064 review's objection to "fail the CONNECT" was actually about.
  **Filed twice**: this entry and §0.2's unnamed bullet are one defect, and both are
  ticked. **Five reachable exits**, each with its own fixture, because a remedy shown on
  one says nothing about the others: the PUBREL consumer, the session-affine QoS 2
  consumer, the raw subject subscription, the plain-or-group JetStream consumer, and the
  level-up companion of the last two. The verdict is `mqttFirstUnrestoredFilter`, reading
  a value `processSubs` was always required to set, and it is named **local failure
  aggregation** rather than arbitration: it proves no step reported failure, not that
  ownership is still current. **The reorder needed exactly one new fact**, and three
  revisions of the plan were spent discovering it cannot be per-subscription — a
  per-subscription flag cannot be armed before the subscription exists (a bare
  `c.processSub` makes the NATS handler live before it returns, so a stored PUBREL can
  enter its callback with no metadata at all), and a released gate is indistinguishable
  from one that never existed, so a refusal cannot express "woken and still not allowed
  out". So transmission suppression is ONE connection-scoped verdict on the session,
  consulted by all four delivery callbacks before they touch `sub.mqtt`, armed
  unconditionally on every accepted CONNECT and deliberately left installed after a
  refusal. `mqttSub` gains no field, `processSub` is unchanged, and the reconciliation
  phase's adoption and sweep are untouched. Two orderings are load-bearing: the grant
  point moves after restoration (its own rule is "after everything that can refuse"), and
  the 3.1.1 forced leave stays after the CONNACK, so the pre-CONNACK restore now excludes
  shared filters for a non-v5 connection **explicitly** where the old code relied on
  ordering. A restore-path failure also stops being destructive: it no longer deletes a
  plain durable the record still names, and it refunds `tmaxack` per filter from the
  amount each join **reported**, because the config comes back whether a call charged it
  or reused a resident entry. **Residuals in `docs/CONFORMANCE.md`**: a permanently
  unrestorable filter refuses every resume (clean start is the recovery), a shared QoS 0
  message can be swallowed during the restore window, **a withheld QoS 1/2 delivery can
  lose its never-transmitted marker to a rebind racing the refusal and then waits out
  AckWait rather than being NAKed forward — the lost prompt NAK is this item's own residual
  while the DUP derivation was F-072's, since CLOSED 2026-09-09**, the same marker is lost
  for a shared durable even without that race because the ordinary close purges it, a
  timed-out affine wait fell into that same closed population, and the displaced-broker
  window has no upper bound. **A
  malformed persisted shared key now refuses the resume rather than being skipped**, which
  holistic round 4 found was the same accepted-but-dead state in miniature.
  **Refuted designs, so none returns:** failing the CONNECT after the
  CONNACK, retrying in place, removing the filter from the record, duplicating the
  provisioning into the reconciliation phase, installing the raw subscription ungated in a
  final pre-CONNACK sub-pass (it makes a QoS 0 PUBLISH the first packet), proving the group
  durable by re-issuing its recorded config (shared configs are not persisted, and
  `ActionCreateOrUpdate` with a stale one rewrites a live durable for every other member),
  moving the 3.1.1 forced leave before the CONNACK (it makes an unacknowledged CONNECT
  destructive), and carrying the restore state on each subscription.

- [x] [F-028: Preserve deferred disconnect state during foreign
disposition](findings/session-and-will.md#f-028).
  **ACCEPTED RESIDUAL, not fixed (2026-10-12 convergence triage).** Since F-084 every
  foreign route discharges the parked Will against its original deadline rather than
  dropping it. What remains is narrower and rare (an ambiguously parked close later
  disposed as foreign): the foreign discharge performs no session save and no expiry
  scheduling, so the original session-expiry obligation is not enforced by that path, and
  retained `uncertainLanded` continuations stay reachable while the old object is
  referenced. Stated in `docs/CONFORMANCE.md` in the ambiguity path's F-084 resolution
  bullet ("What the discharge does NOT carry is the session's expiry").

- [x] [F-037: Make the rejected finite-disconnect stamp
crash-safe](findings/session-and-will.md#f-037).
  **ACCEPTED RESIDUAL, not crash-safe (2026-10-12 convergence triage).** Reached only by a
  finite close whose save is rejected followed by a crash before any settlement write
  lands. Stated in `docs/CONFORMANCE.md` beside the rejected-close residuals.

- [x] [F-038: Prevent takeover races from publishing or dropping the wrong
Will](findings/session-and-will.md#f-038).
  **ACCEPTED RESIDUAL for arm (a) (2026-10-12 convergence triage); arms (b) and (c) were
  closed by F-084.** A remote takeover racing an ordinary finite close can cause one
  unwanted immediate Will from that close (an extra notification, never a lost Will).
  Retracting it needs a durable Will intent, which is Will-outbox scope. Stated in
  `docs/CONFORMANCE.md` under "A rejected close still publishes its Will".

- [x] [F-047: Keep affine assignments when a SUBSCRIBE packet
fails](findings/shared-qos2.md#f-047).
  **PARKED, NOT FIXED: chapter P.** Previously Medium.

- [x] [F-048: Arbitrate affine cleanup before discarding
assignments](findings/shared-qos2.md#f-048).
  **PARKED, NOT FIXED: chapter P.** Previously Medium to large. Design and test F-047 and
  F-048 as one transactional-cleanup
  cluster.

- [x] [F-059: Arbitrate retained-index application by stream
sequence](findings/runtime-correctness.md#f-059).
  **DONE (2026-10-12).** Both mechanisms landed in `internal/mqtt/retained_index.go`.
  Ordering: every index writer (observer add, tombstone and decode-failure or marker
  deletion; local seed; SUBSCRIBE-time load, which now also INDEXES what it reads) goes
  through one arbitrated apply against the entry's sequence and the observer's watermark
  W, and a deletion seen ahead of the observer leaves a placeholder. Loss: the observer
  tracks the consumer delivery sequence; a gap, an idle heartbeat naming a later delivery,
  or three missed heartbeats resets it, and a reset re-creates the same-named consumer on
  a fresh per-generation deliver subject from W+1 and flushes the cache. Markers: enabling
  `SubjectDeleteMarkerTTL` was evaluated and declined (it needs `AllowRollup`, which the
  shared stream shape refuses); a marker sourced in from an upstream is recognised before
  decoding and applied as a deletion. Residuals — a superseded value served from the cache
  in three interleavings, bounded by the cache TTL; repair bounds; the sourced-topology
  coverage gap — are in `docs/CONFORMANCE.md`, "The retained index is ordered by stream
  sequence, and a lost application is repaired". Nineteen mutations each kill their test.
  The finding as filed: the volatile retained index applies stream events and local seeds
  without comparing sequences, so a delayed or lost application can leave a stale
  or absent entry. Two mechanisms, not one: a watermark orders the writers, and a
  replayable consumer or a reconciliation is what covers a lost `AckNone`
  delivery — the watermark alone cannot. Scope it by arm: a stale entry on an
  indexed subject is already reconciled by the next matching subscription after
  cache expiry, so the work that has no existing repair is the ABSENT entry.
  Evaluate `SubjectDeleteMarkerTTL` as the expiry signal in this SAME change:
  recognize `Nats-Marker-Reason: MaxAge` before MQTT decoding and order the marker
  by stream sequence, while keeping explicit empty-body MQTT tombstones. Markers do
  not replace replay/reconciliation and require an `AllowRollup`/purge ACL review.
  See the [platform audit](findings/nats-platform.md#subject-delete-markers-for-retained-convergence).

- [x] [F-060: Report a failed retained store to the
publisher](findings/runtime-correctness.md#f-060).
  **DONE (2026-10-12).** An accepted publication whose retained update the store did
  not confirm (refused, reply lost, or a fence) is never given final success:
  `mqttHandlePubRetain` returns the error and `mqttInitiateMsgDeliveryJSA` joins it
  with the `$MQTT_msgs` store's. QoS 0 closes the connection (DISCONNECT 0x80 under
  v5); QoS 1 still stores `$MQTT_msgs`, withholds PUBACK and closes; QoS 2 withholds
  PUBCOMP and keeps the hold; a delayed Will stays `publishing` for its lease to
  re-fire; an immediate Will is logged. The client re-sends; the broker never
  re-attempts a retained store on its own, because a retry at a newer sequence can
  overwrite another publisher's newer value. The retained update now also runs when
  the live send failed, which used to skip it while QoS 1/2 was still acknowledged.
  Mechanism: `docs/ARCHITECTURE.md`, "A retained update the store did not confirm
  (F-060)"; residuals (recovery needs a surviving session, repeated live fan-out, a
  re-send can overwrite an intervening value, delayed retained Wills re-fire per
  lease): `docs/CONFORMANCE.md`, "A retained update the store did not confirm closes
  the publisher's connection".
  The finding as filed: a PUBLISH whose RETAIN side failed to store is reported as
  successful, contrary to [MQTT-3.3.1-5]. Decide the contract for QoS 0 —
  which has no acknowledgement to withhold — alongside QoS 1 and 2.

- [x] **Reclaim CONNECT acceptance facts, or bound them.** **DECIDED (2026-10-12): an
  accepted residual, neither reclaimed nor bounded.** Research for this item found the
  premise did not hold. The fact is one of at least four permanent records every successful
  CONNECT leaves — the fact and the create-only handoff receipt on `$MQTT_sess`, and at
  least two F-105 transition outcomes on `$MQTT_inflight`, the last already an accepted
  residual — so reclaiming facts cannot bound the reconnect-loop growth this item was filed
  for. And the reclamation rule below is UNSOUND, with or without an amendment keeping the
  current acceptance's fact: `mintAcceptanceTenure` and the uncertain-acceptance resolver
  read an acceptance's OWN fact back (the resolver's readback was deleted on 2026-10-19,
  leaving `mintAcceptanceTenure`), and `L` reaps exactly that fact; and a delayed Will
  held in memory by the retention queue (`will_delayed_retry.go`) is enqueued later with
  its ORIGINAL origin, below facts a reaper would already have taken, so the Will fires
  although the client reconnected within its delay. An admission bound was not taken
  either: a rate limit bounds only the slope over an unbounded recovery epoch, and a
  ceiling without reclamation eventually refuses legitimate CONNECTs; both need an
  availability policy nobody has decided. Codex consult (course C) decided; the exposure —
  cheap authenticated storage exhaustion — is stated in `docs/CONFORMANCE.md`, "Per-CONNECT
  permanent records accumulate for the recovery epoch, and a reconnect loop can exhaust
  storage", with the worked interleaving. General reclamation of these classes is a §2
  audit item, not an alpha precondition. Do not implement the rule below.

  **The finding as filed, preserved as history; its proposal is REFUTED.** Every accepted
  CONNECT writes one permanent record on a unique `$MQTT.accept.>` subject, and
  `$MQTT_sess` carries no `MaxMsgs`, `MaxBytes` or `MaxAge` (it cannot: those limits
  would evict live session records). `MaxMsgsPer: 1` does not bound it either, because
  each fact has its own subject. So any credential permitted to CONNECT grows the
  account's disk permanently through a reconnect loop, and nothing in the broker
  refuses or throttles it. Raised as a **no-ship** by the holistic review of the
  acceptance-fact change (F-031); the change landed with it stated as a residual in
  `docs/CONFORMANCE.md`, which is a disclosure, not a bound.

  The latency half of the same complaint is weaker than the storage half, but only
  WITHIN one incarnation, and the difference matters to scoping. For a Will whose
  origin epoch is the current one, the walk starts at `OriginSessionSeq + 1` and stops
  at the cutoff, so it reads only facts written after the connection that enqueued that
  Will — and every reconnect that inflates the history also advances the origin of any
  Will enqueued afterwards, and licenses a cancel for one already pending. For a Will
  from ANOTHER incarnation the walk starts at the beginning of the stream, because that
  origin sequence names a store that no longer exists, so there the scan depth is the
  whole current history.

  **A candidate reclamation rule, and the epoch condition that a first draft of it got
  wrong (REFUTED 2026-10-12, see above: its claims that facts at or below `L` are unusable,
  that Wills not yet enqueued are safe, and that storage becomes O(clients + pending delayed
  Wills) are all false, and the "supported coupled recreation" in its counterexample is no
  longer a supported operation after F-066).** Within ONE incarnation a fact at stream
  sequence `S` can only ever qualify
  for a Will whose `OriginSessionSeq < S`, and a Will's origin is the session record of
  the connection that enqueued it. So for client `C`, with every non-terminal Will of
  `C` in the CURRENT incarnation, let

      L = min( C's current session-record sequence (absent = +inf),
               min OriginSessionSeq over C's non-terminal $MQTT_willq records )

  and every fact of `C` at or below `L` is permanently unusable — including for Wills
  not yet enqueued, because a future connection's record necessarily sits above every
  fact that exists now, and a fact always commits below the record of its own batch.
  Since `$MQTT_sess` is `MaxMsgsPer: 1`, `C` has exactly one record, so this reclaims a
  client's whole history on each reconnect and leaves storage at O(clients + pending
  delayed Wills) rather than O(accepted CONNECTs).

  **That rule is UNSOUND the moment any non-terminal Will of `C` carries a different
  origin epoch**, and a review round caught the first draft of this bullet asserting it
  unconditionally. `mqttAcceptFactQualifies` deliberately SKIPS the sequence comparison
  for a cross-incarnation Will — a current-incarnation acceptance supersedes it outright
  — so such a Will qualifies against a current-epoch fact at ANY sequence. Worked
  counterexample: an old-epoch Will has origin 100; after the supported coupled
  recreation a fresh acceptance fact lands at sequence 1 and its record at 2; the rule
  computes `L = min(2, 100) = 2` and deletes the fact that would have suppressed that
  Will, which then fires. The rule must therefore retain EVERY current-epoch fact of
  `C` while any non-terminal Will of `C` has a foreign origin epoch, and may apply `L`
  only when they all match.

  Also settle before implementing: where the reaper runs (the claim pass already walks
  `$MQTT_willq` per account, which is where `L`'s second term is cheapest to compute);
  what a *refused* outbox record does to `L`, since its origin cannot be read and it
  must therefore force `L = 0` for that client rather than be skipped; whether a partial
  or failed `$MQTT_willq` walk may reclaim anything at all (it must not — the same
  conclusively-completed rule the timer sweep already obeys); and how the rule interacts
  with `an absence is not a fact`, since this deletes suppression evidence and a wrong
  `L` silently un-suppresses a Will. The alternative, if the proof is rejected, is an
  admission bound — a per-account acceptance-fact ceiling that refuses CONNECT — which
  trades a storage vector for an availability one and needs its own decision.

- [x] **F-112: a live generation's outbound history is never reclaimed, and every resume
  reads all of it.** **DONE (2026-10-13):** a live generation's ledger carries a proven
  contiguous floor `Lo` and a deletion watermark `Del`; every position below `Lo` is
  retired — RECLAIMED on absorbing, record-bound proof (a Terminal duty; an exchange at an
  identifier ending with every edge released and its register subject no longer naming it;
  a resource at CleanupComplete), or RELOCATED unchanged to a representative above `Hi`
  (the PUBREL consumer's, live subscriptions', abandoned and stuck deliveries' duties) —
  by one atomic `advance-floor` batch whose write boundary re-decides every position; the
  records below `Del` are physically deleted. Every reader (resume, custody census, shared
  release fence, identifier exclusivity, obligation enumeration, resource-duty lookups,
  the recovery sweep) reads `[Lo, Hi]` and restarts only when the floor crosses a position
  it could not read; exclusivity moved off the paged subject walk onto the inventory its
  own ledger CAS fences. An absence is classified (`classifyExchangeAbsence`: a typed
  landing proof plus a complete traversal without a representative), so admission
  continuity no longer NAKs a reclaimed admission; a retired position's uncertainty waits
  for a complete census; the ended-lineage veto got its exit; a custody-dispatcher floor
  unit (latched, periodic, reopen-aware, background-priority, bulk resolution) drives it,
  resolving exchange duties from the inventory. Residuals in `docs/CONFORMANCE.md` ("A
  live generation's history is retired behind a floor"): total storage stays unbounded
  (outcomes, actions, resources), and never-ending obligations are carried rather than
  reclaimed. Mechanism: `docs/ARCHITECTURE.md`, "The inventory floor". Original finding
  follows. Filed 2026-09-30 by F-105 unit 7b (consult 87, course (i)). No live
  generation resolves an exchange duty or deletes an ended exchange record — only the
  closed-generation recovery pass resolves duties, and nothing deletes
  `mqttExchangeSubject` — and `$MQTT_inflight` has no size or age limit, so every QoS 1/2
  delivery a persistent session ever made leaves an exchange and a duty entry until the
  generation is discarded (Clean Start or expiry). The resume reads every one before
  CONNACK: the row-5 subject census loaded every exchange, and the 7b inventory traversal
  adds one batched duty read per 1024 ordinals. This is unbounded storage growth and
  history-proportional reconnect latency, not eviction (`docs/CONFORMANCE.md`, the
  live-generation accumulation residual). **The deliverable** is live-generation duty
  resolution and exchange reclamation with a PROVEN contiguous floor: the traversal starts
  at the floor, and every ordinal below it is proven reclaimed. It is a new lifecycle
  class, so it needs both its creation and its exit
  (`feedback_lifecycle_class_needs_creation_and_exit`), and its exit condition is not a
  terminal protocol phase: an exchange may be reclaimed only once its identifier and
  Receive Maximum charge are released, no register, action, canonical PUBREL,
  abandoned-ACK binding or HandBack still references it, and its duty is terminal. An
  absence below the floor must remain distinguishable from a lost record — the inventory's
  current rule ("a missing entry is an inconsistency, never completed work") is what makes
  the traversal sound, and a floor must preserve it rather than trade it for an
  approximation. No approximate floor, retention limit or speculative compactor.

- [x] [F-095: a server-side default caps consumers per stream, and no stream or account
  setting this broker can read opts out of
  it](findings/nats-platform.md#f-095). **DECIDED: option (a), a deployment prerequisite
  (2026-10-12 convergence triage).** The decision closes the policy choice, not the
  limitation: the broker still cannot read the server default through its own API access,
  and the residual in `docs/CONFORMANCE.md` stays. The conformance environments set
  `default_max_consumers: -1` and `run.sh` checks the effective `/varz` value. The N+1
  behavioral experiment below was not performed by this decision. Filed 2026-09-09 by the
  nats-server v2.15.0-RC.1 dependency audit. From the v2.15.0 line the server carries
  `jetstream { limits { default_max_consumers } }`, whose own default is
  `JSDefaultMaxConsumersPerStream` = **1000**. Both the standalone
  (`addConsumerWithAssignment`) and the clustered (`jsClusteredConsumerRequest`) create
  paths apply it whenever the stream's `MaxConsumers` **and** the account tier's
  `MaxConsumers` are both `<= 0`, and upstream states in the code that `-1` counts as
  "not set" so the default cannot be opted out of per stream or per account. That is the
  exact shape four broker-owned stream validators REQUIRE, each with the reasoning that a
  cap eventually rejects a legitimate operation. The dossier holds the canonical
  stream-by-stream inventory; do not re-list it here, and note that `$MQTT_qos2assign` is
  **not** one of them — it has no consumers and waives `MaxConsumers`. The configuration
  the broker demands is the one that arms the cap.
  **What actually trips:** the cap is charged on **new consumer admission only**, since
  both create paths exempt a request naming an existing assignment — so steady state and a
  resume with intact durables are unaffected. What fails is a create of a consumer not yet
  present on a stream already at 1000: SUBSCRIBE on `$MQTT_msgs` (one durable per
  subscription, shared group and drain), and on `$MQTT_out` (per-session PUBREL and affine
  durables, scaling with concurrent sessions) a new QoS 2 SUBSCRIBE, a restoration that
  must create a missing durable for a saved filter, or an accepted-but-stalled recovery
  after CONNACK. Two independent axes; the dossier holds the full symptom list.
  **Deployment floor raised for this, 2026-09-09 (owner's decision):** the floor moved
  from 2.14.5 to **2.15.0** so the mitigation instruction is unconditional. Below 2.15.0
  the option does not exist — and v2.15.0-preview.1's config parser rejects the key
  outright — so a lower floor forces a version boundary onto the prerequisite, which two
  review rounds got wrong in opposite directions. Pre-alpha, with no released artifact and
  no deployed population, the raise cost nothing. **It does not close this finding:** the
  cap still cannot be opted out of per stream or per account, and still cannot be seen.
  **Why startup validation does not catch it:** the option is server-side and is reported
  by neither `STREAM.INFO` nor `$JS.API.INFO`. `JetStreamAccountLimits.MaxConsumers` is
  the *other* input to the `<= 0` test, not the server default, so reading the default
  needs `$SYS` VARZ, which the bridge's client connection has no claim to. The bump left
  this as a documented deployment prerequisite beside the version floor
  (`docs/ARCHITECTURE.md`) with the residual stated in `docs/CONFORMANCE.md`; that is a
  statement of the gap, not a fix.
  **What this needed was a decision, not an implementation (historical; taken as (a)
  above):** (a) keep it a deployment
  prerequisite only; (b) require an explicit positive account tier `max_consumers` at
  startup and validate it against a declared working set, converting an invisible cap into
  a visible one the broker refuses early; or (c) ask upstream to expose the effective
  per-stream consumer limit on a path a JetStream client can reach. Whichever is chosen,
  the acceptance test (for (b) or (c), if ever taken up; (a) changes no broker behavior)
  is behavioral rather than an options assertion: with the remedy in
  place N+1 durables on one broker-owned stream succeed, and with it reverted the N+1st
  fails with 10026.

- [x] **Move the external conformance environments onto the 2.15.0 floor.** Filed
  2026-09-09 by the v2.15.0-RC.1 bump's holistic review (round 10); **done 2026-09-20**,
  in the same change that advanced the pin from `v2.15.0-RC.1` to the **2.15.0 GA**. The
  trigger it named — the 2.15.0 image publishing — occurred, and `nats:2.15.0` resolves to
  `sha256:cd3fcd4ecdda44e3a66728a5334af0a959bc3979b32810e033d1c547241cd0f4`.
  All three launches now run that digest at the floor, with `default_max_consumers: -1`,
  in the fixed order the item required: image first, key second.
  **The three did not share one remedy.** The Paho job and
  `test/conformance/docker-compose.yml` mount `test/conformance/nats.conf`, whose PENDING
  block became a real `jetstream { enabled: true  limits { … } }` block. The Rust job ran
  `-js` with no configuration file, so it gained its own unauthenticated
  `test/conformance/nats-plain.conf` and a `-c` invocation; the Paho environment's ACLs
  would break that job's non-passthrough broker.
  **The assertion is on the effective limit, not the version**, which is what the item
  asked for. `test/conformance/run.sh`'s `check_nats_floor` reads `/varz` before any suite
  runs and fails unless the server is at or above the floor *and* reports
  `default_max_consumers: -1`; an unreachable or unresponsive monitoring endpoint fails
  too, within a bounded window, so there is no silent pass and no stall. It discriminates,
  measured on the 2.15.0 GA binary: `-js` alone reports `1000`, a config without a `limits`
  block reports `1000`, only the explicit `-1` reports `-1`.
  It **decodes** `/varz` with `python3` rather than pattern-matching it. Three review
  rounds each defeated a text-matching version — a prefix match reading `-1.5` as `-1`, a
  duplicate whose second value was malformed, and a duplicate key spelled `c` — so
  the mechanism was replaced rather than patched a fourth time. Decoding rejects a
  truncated document, collides an escaped duplicate with the name it duplicates, and reads
  both fields at their exact paths. `python3` is therefore a hard requirement of the check,
  and the Rust conformance job gained `setup-python` for it.
  **Verification.** Both config files were parse-checked and run against the 2.15.0 GA
  binary; both CI launch command lines and the pinned digest were exercised against the
  real container. The **Paho interoperability suite was re-run end to end** against it and
  showed no new failures — `test_keepalive` fails, but it fails identically on the parent
  commit at `nats:2.14.6`, so it is pre-existing and is recorded below rather than
  baselined. The **Rust `vibesrc/mqttconformance` suite was not re-run**: it needs a Cargo
  build of an external CLI that is not present locally. That is stated rather than implied.
  The `docs/CONFORMANCE.md` residual is replaced by the current statement.
  **The GA fixes none of F-108, F-109 or F-110.** Nine store-level functions those findings
  cite are byte-identical between RC.1 and GA; their bullets' RC.1 line numbers remain
  correct for the bodies they name. The bump neither closes nor narrows them.

- [x] **F-113: a clean-start CONNECT over a live QoS 2 session is refused.** A v5
  clean-start CONNECT over a live serving session that owns a PUBREL consumer but no
  affine QoS 2 consumer — any ordinary QoS 2 subscriber — is refused before CONNACK
  ("refusing to prepare an unsupported handoff ... an affine target carries a malformed
  affine generation", naming the empty one) and the client sees the connection close.
  Found during F-105 7c and verified at HEAD `0af6118` on an exported copy. Reproduction:
  v5, Session Expiry 60, SUBSCRIBE at QoS 2, one ordinary QoS 2 delivery, close, CONNECT
  with Clean Start. **The producer is wrong, not the validator:** the clean-start path
  (`internal/mqtt/mqtt.go`, the `mqttCleanStartRetireTargets` built in the
  discarding-handoff branch) names `PubRelDurable` from the session's PUBREL consumer with
  `AffineGen`/`AffineDurable` empty, and `mqttValidateAffineDescriptor`
  (`outbound_authority.go`) refuses a partial descriptor by design; `PubRelDurable` is
  read only when an affine target is enrolled (`mqttComposeClosingEnrolment`). The
  session-END path (the teardown receipt, same file) builds the same triple and is NOT
  refused — a QoS 2 session's expiry was measured to reach a Discard closure — so the
  defect is the clean-start producer alone. Keep the strict validator; name no affine
  target unless one exists (consult 103: filed here, fixed by its own change after 7c).
  **Reproduced across brokers with real clients on 2026-10-05** by F-105 WP-F W2's
  discarded-generation witness: a persistent 3.1.1 QoS 2 subscriber past PUBREC on
  broker A, then a clean-session CONNECT on broker B sharing A's JetStream, refused with
  the same message.
  **Fixed (2026-10-05, its own change, consult 129):** every producer of a retirement's
  affine descriptor — the clean start's discarding handoff request and both session-end
  teardown receipts — projects through `mqttAffineRetireDescriptor`, which names the affine
  target whole (affine durable, affine generation, PUBREL proof source) or not at all; the
  validator is unchanged and still refuses a PUBREL durable named alone. The clean start's
  teardown target keeps the PUBREL durable for the post-finalize classification. Tests
  (`outbound_cleanstart_qos2_test.go`): the bullet's v5 reproduction accepted with the
  predecessor discarded and both consumers retired; the cross-broker reproduction accepted
  with the discarding closure written by B's finalize; the projection and the validator's
  strictness, including the unprojected PUBREL-only shape through the intent gate; the
  retirement a clean start carries names no affine target. Mutations M1-M4 each killed.
- [x] **F-114: sixteen bounded re-attempt loops are unspaced against same-session write
  bursts.** **MOVED to §2 "Performance and resource audits" (2026-10-12 convergence
  triage)**: the SUBSCRIBE starvation that motivated it has its remedy. Since F-112 (holistic
  round H4, consult 2) a second loop HAS a demonstrated consequence: the delivery admission
  ladder (see the §2 item). Moving it implies no loop is harmless. F-105 7c measured
  the retained workers' authority bursts defeating the
  SUBSCRIBE name claim's three back-to-back attempts; the claim was given a spaced budget
  and the foreground-priority handshake (consults 104, 105). Sixteen further loops of the
  same shape re-attempt an authority or register write `mqttInflightStoreAttempts` (3)
  times with no spacing: `inflight_marker.go` (one), `inflight_register.go` (eight),
  `outbound_disposal.go` (one) and `pubrel_action.go` (six). Each needs classifying —
  foreground or background, authority transition or register CAS, what its exhaustion
  costs — before any is converted; they are not interchangeable retry policies (consult
  104).
- [x] **F-115: a QoS 2 PUBLISH is redelivered after the client's PUBREC.** Filed
  2026-10-02 by F-105 7c-lc, measured on `d5913f7` (the change's baseline, before any
  7c-lc edit): `TestMQTTQoS2InflightMsgsDeliveredAfterUnsubscribe` fails 6 of 8 standalone
  runs. With `AckWait` 250 ms, a client that answers PUBREC for an outstanding QoS 2
  delivery receives that delivery's PUBLISH again (DUP set) once the source's AckWait
  expires, before or instead of its PUBREL. Past PUBREC the sender must not send the
  PUBLISH again ([MQTT-4.3.3]); the redelivered source is evidently not suppressed by the
  exchange's PUBREC-observed state in that window. Not caused by 7c-lc (identical failure
  at the baseline); it passed the 7c baseline run by chance. Needs the window traced
  (which delivery path re-emits, and what it reads) before a fix is planned.
  **Done (2026-10-13); the window is traced.** The one symptom was two defects with one
  root, a source re-offer racing the client's acknowledgement of its exchange, plus a
  fixture timing race. *Shape A*: a re-offer mapped to the existing identifier opened a send
  section before the read loop handled the PUBREC, and the section's write ran after it
  (PUBREC, PUBLISH DUP, PUBREL). *Shape B*, the worse one: a re-offer arriving after the
  PUBREC handoff removed the source's `cpending` link was admitted under a FRESH identifier,
  delivering the payload a second time after its PUBREL. Fixed in two parts
  (`docs/ARCHITECTURE.md`, "An acknowledged PUBLISH is not sent again"): the
  acknowledgement (PUBREC, PUBACK, refused PUBREC) marks the open PUBLISH section's output
  `answered` in the critical section that consumes it, and the write boundary refuses it;
  and `trackPublish` declines a source represented by an in-memory PUBREL-phase entry, the
  live route's half of "one source delivery has at most one un-ended exchange". Scope by
  consult: the decline lasts as long as that entry, and a stale re-offer reaching admission
  after PUBCOMP's terminal reservation is released is a stated residual in
  `docs/CONFORMANCE.md`. The test's own race, an unacknowledged PI 3 retransmission
  overtaking PUBREL 2 at an AckWait of 250 ms, is a fixture budget, now 2 s with every
  assertion unchanged (20 of 20 standalone) and recorded in the
  [verification ledger](findings/release-investigations.md#verification-ledger). One
  fixture asked for what the invariant forbids and now seeds a distinct source per variant
  (`TestF105Row2EntryRefusesAHolderThatIsNoLongerLive`). Tests:
  `f115_answered_publish_e2e_test.go` (each window held by a seam, re-offers forced by NAK)
  and `f115_source_pubrel_test.go` (every entry shape, with an allocating control).
  Mutations M1, M2, M5, M6 and M7 each killed by their paired tests.
- [x] **F-116: a PUBREL attempt prevented from writing keeps its PUBCOMP debt, so its
  identifier stays reserved for ever.** Filed 2026-10-02 by F-105 7d (consult 114), measured
  on `a6ba9ee` (7d's baseline) and on the 7d tree alike. `beginPubRelSendLocked`
  (`inflight_register.go`) increments the send hold's `pubCompOwed` when a PUBREL
  transmission opens; the callback then re-checks the entry at write entry
  (`pubRelWriteKindLocked`) and returns WITHOUT writing when the client's PUBCOMP for an
  earlier transmission has already completed the exchange; `endSendWith` clears the token
  but not that unwritten attempt's debt, and action retirement preserves it, so
  `sendReservedLocked` stays true with no token, no pending entry and no record. A
  deterministic witness (the prevented write staged at `mqttTestAfterPubRelCommitV`)
  shows `pubCompOwed=1, reserved=true` on both trees; it is the mechanism behind
  `TestMQTTDrainBarrierFailsClosed`'s intermittent "packet identifier 1 is still reserved
  by a send hold" (interleaved: baseline 2 of 16, 7d 8 of 16 — 7d's higher rate is
  unattributed: neither its prompt drive nor its extra teardown enumeration explains it).
  The fix is per-attempt debt cancellation for a PROVABLY prevented write, keeping debt for
  any possibly emitted frame; moving the increment after the write would race an early
  PUBCOMP. The existing `TestPendingClosurePubRelWriteNeedsAnActingEntry` checks the
  action drain of a prevented write and not this debt.
  **A second trigger (widened 2026-10-07, F-105 integrated-gate fix 1, consult 134):** the
  client's Maximum Packet Size refusal inside `mqttWriteAuthorizedPubRel` (`mqtt.go`),
  which returns AFTER write entry and before any byte, from both current callers - the
  delivery callback `mqttDeliverPubRelCb` and the retained route's
  `emitRetainedPubRelOnce`. The refusal already leaked the debt at the gate base `9310151`
  (`mqttEnqueuePubResponse` -> `mqttEnqueueFittingProto`; `eea8531` moved it into the
  authorized writer); `emitRetainedPubRelOnce` is new in the F-105 span, so its route is
  not covered by any baseline exemption. Measured by the gate fix's long-lived test: each
  such refusal followed by one successful PUBREL and its single PUBCOMP leaves a send hold
  `{no token, no action, pubCompOwed 1}` (two holds per round, 20 after 10 rounds, 220
  after 110). Those schedules are therefore tested once each, and their repeated
  non-growth is recorded as masked by this finding, not covered. The fixture's limit below
  a PUBREL's four bytes induces the refusal; it is not evidence that negotiation admits
  such a limit. The fix is unchanged: exact, once-only cancellation of the provably
  prevented attempt, debt kept for any possibly emitted frame, the increment kept before
  the write.
  **Done (2026-10-13).** Closed by a property, not by the refusal sites
  (`docs/ARCHITECTURE.md`, "An attempt's PUBCOMP unit is kept only for a frame that may
  have left"). On both PUBREL routes a `possiblyEmitted` flag turns true on the line before
  a writer is called and false again only on the writer's own `mqttOutputPrevented`, which
  `mqttWriteAuthorizedPubRel` now also returns for the Maximum Packet Size refusal;
  `endPubRelSend` gives the attempt's unit back behind the exact-token match that ends the
  section, so every return before a writer cancels (including future ones), and a torn
  write, a completed one or a panic after the flip keeps the unit. The increment stays
  before the write. Plan review found the same leak one reader away: a lineage release
  converted an entry into a released delivery on the strength of an open attempt's unit,
  which no PUBCOMP would ever answer once that attempt was refused. The release now counts
  only PUBRELs possibly emitted (`pubCompEmittableOwnerLocked`, discounting an open
  attempt not yet entered, via the exact-token `relEntered` fact), and an entered attempt
  refused after a release retires the released delivery when its cancellation leaves
  nothing owed. That retirement can race a PUBCOMP mid-discharge for an earlier copy; the
  outcome (settlement skipped, an absent-register PUBREL record left until generation
  teardown) already exists today without F-116 and is stated as a pre-existing residual in
  `docs/CONFORMANCE.md` ("A stored PUBREL with no surviving authority"). Tests:
  `f116_pubrel_unemitted_debt_test.go` (T1 both forms, T4 torn write, T5b, T5c, T6 panic,
  T7 to T7d lineage release before and after entry, T8), the S6 and S8 single-shot tests
  now asserting the unit given back, and S6/S8 repeated on one connection in
  `TestF116ALongLivedConnectionEndsRefusedPubRelsWithOnePubComp` (a sibling of the F-105
  long-lived test on the same harness: all eight schedules on one connection ran 29 min
  under `-race`). Mutations M1 to M12b each killed by their paired test. Seven existing
  fixtures delivered PUBRELs over a client with no socket, so every such write was
  prevented, and then asserted the PUBCOMP debt of those "transmitted" copies or the
  released delivery it makes; they now transmit over a recording socket
  (`testWireInflightEnv`), with every assertion unchanged. Two structural guards and one
  inventory learned the new names (`endPubRelSend`, `endSendCore`,
  `retireReleasedDeliveryLocked`).
  **Not closed by this:** `TestMQTTDrainBarrierFailsClosed`'s "still reserved by a send
  hold". Interleaved at `85024d9`, 16 runs per arm under `-race`: baseline 11 of 16 red,
  F-116 tree 12 of 16, and on both trees the failing hold carries no PUBCOMP debt but an
  identifier exclusion (`slotOwed`) left by an action drain that did not complete the
  exchange. Moved to the
  [verification ledger](findings/release-investigations.md#verification-ledger).

- [x] **F-120: a connected client's packet length is not bounded before the packet is
  buffered.** **DONE (2026-10-14).** A claimed length is judged at the fixed header,
  before any body is buffered (`readPacketLen`): first against the connection's bound
  (`mqttInboundPacketBound`: max_payload before CONNECT, the advertised Maximum Packet
  Size for a connected v5 client, max_payload plus `mqttConnectedPacketOverhead` for a
  connected 3.1.1 client), then against the length its type fixes. The post-completion
  remaining-length check is deleted, not kept as a second gate. Departs from upstream's
  order on purpose: size first, so a v5 packet over the advertised size stays Packet Too
  Large (0x95) when its length is also wrong for its type. Partial buffering appends to
  an owned buffer (linear, not quadratic), keeping it only while it holds nothing but the
  partial. Tests: `TestMQTTIncompletePacketMaxPayloadViolationDisconnects`,
  `TestMQTTIncompleteFixedLengthPacketDisconnects`,
  `TestMQTTOversizedReservedTopicPublishDisconnects`,
  `TestMQTTMaxPayloadDoesNotCapPublishTopic`, `TestMQTTPartialPacketNotCopiedOnEveryRead`,
  `TestMQTTPartialPacketAfterCompletePacketIsCopied`,
  `TestMQTTReadPacketLenRefusesRemainingLengthBeforeBuffering` and
  `TestMQTTv5IncompletePacketRefusedBeforeBuffering`; mutations M1 to M7 each killed by
  their paired test. A v5 client with an encodable max_payload was already bounded, so
  the v5 oversized-PUBLISH case passes on the old parser too and guards "enforced ==
  advertised"; its PINGREQ case did not. Newly refused for 3.1.1, stated in
  `docs/CONFORMANCE.md` ("A connected packet's claimed length is bounded at its fixed
  header"): a SUBSCRIBE or UNSUBSCRIBE over max_payload + 65,544 bytes, and an oversized
  PUBLISH to the reserved `$MQTT/` namespace that used to be discarded with the
  connection kept. Mechanism in `docs/ARCHITECTURE.md`, "An inbound packet's length is
  bounded before it is buffered".
  Filed 2026-10-11 by the nats-server 2.15.1 bump's MQTT source-port triage,
  from upstream nats-io/nats-server#8692, which 2.15.1 ships and this repository's copied
  `mqtt.go` predates. Read from the parser's entry: in `mqttParse` (`mqtt.go`) `maxLen`
  is `max_payload` only before CONNECT; once connected it stays `jwt.NoLimit` unless
  `c.mqtt.maxInPacketSize > 0`, and that field is assigned only in the MQTT 5 branch of
  CONNECT handling. `mqttReader.readPacketLen` therefore accepts any claimed Remaining
  Length up to the variable-byte maximum (268,435,455) and keeps the partial packet in
  `pbuf`, `mqttReader.reset` re-copies the whole partial on every read, and both
  `mqttCheckRemainingLength` and the `max_payload` check in `mqttParsePub` run only on a
  complete packet. So an authenticated MQTT 3.1.1 client (and an MQTT 5 client when the
  server's maximum packet size is unlimited) that sends a fixed header claiming a large
  length and then trickles bytes makes the broker hold memory that grows with the bytes it
  sends, up to that maximum, with cumulative copying quadratic in the number of reads,
  before anything rejects the packet. Authentication is no reason to trust a packet
  length. Upstream's
  correction caps a connected client at `max_payload` plus a fixed overhead, keeps a
  genuinely unlimited `max_payload` unlimited, moves the per-type remaining-length
  validation ahead of buffering, and stops re-copying the partial packet. **Not fixed in
  the bump that found it**, which records findings and ports nothing. Acceptance: a
  connected client's claimed length is refused before its body is buffered when it
  exceeds the bound the server enforces for that protocol level, the fixed-length packet
  types are refused on their remaining length before buffering, partial-packet buffering
  is not quadratic, and a test drives a trickled oversized PUBLISH and PINGREQ on 3.1.1
  and on 5.0 and fails on the current parser. The triage's other 2.15.1 MQTT entries were
  not applicable here; the record is in `docs/findings/nats-platform.md`.
- [x] **F-121: a displaced connection records send intent after its certification, so a
  never-sent PUBLISH reads possibly attempted.** **ACCEPTED RESIDUAL (2026-10-12
  convergence triage).** No owed delivery is lost: the witnessed message never left, and
  withdrawing a never-started delivery after UNSUBSCRIBE is permitted (MQTT 5 §3.10.4).
  What remains is one identifier and one Receive Maximum charge stranded per affected
  exchange until generation discard, which at Receive Maximum 1 stops further QoS 1/2
  delivery to that session. The closure below remains the fix if this is ever taken up.
  Filed 2026-10-11 by the F-097/F-088
  close-out (consult C-W1). A takeover evicts broker A's resident session, and the eviction
  certifies A's connection: its gate seals, then the certificate's persist goroutine runs.
  A send section of A's still open across the certification (the witness parks it after
  it acquires its output token) then runs `recordSendIntent`
  (`inflight_register.go`), which deletes the unsent marker with no entitlement or gate
  check; the sealed gate prevents the write (`mayWrite()` false, the old reader reaches
  EOF). The durable state then says possibly attempted for a PUBLISH that never left. The
  finding's scope is three parts: A's confirmed intent delete after certification; the
  successor B's stale marker-backed classification and the entry removal it causes (where
  the certificate cleared A's token before B's read, the A-first ordering, B derived the
  entry proven unsent, and B's UNSUBSCRIBE withdraws it and lands the standalone occupant
  tombstone); and the markerless reservation that results: the exchange stays at
  `Publish`, B has no entry and no AckRoute, the custody ledger holds and charges the
  identifier, and the prompt re-drive is lost. The intent delete after certification was
  observed in every two-broker run (5 of 5 in witness run 1); the stranding was first one
  diagnostic run and is now asserted. The source's state is recorded as observed (its
  consumer at `NumAckPending` 2, `AckFloor.Stream` 0, `NumRedelivered` 0), never inferred
  from a missing `jsAckSubject`.
  **Evidence:** `TestF121IntentDeleteAfterCertificationStrandsAWithdrawnExchange`
  (`f088_cross_tenure_witness_e2e_test.go`) PINS the defect in the A-first ordering, with
  its premises checked (certificate persisted, token cleared, B's entry not `actionLive`;
  a B-first outcome there is a fixture failure), and checks A's certificate while A is
  still parked, before its intent. Its B-first subtest (certificate persist held, A's
  token live at B's recovery) keeps the post-certification intent assertions, and its
  consequence is now PRESERVED by F-088's fix: B classifies the delivery possibly
  attempted and enrols it, where before the fix that ordering stranded exactly as A-first
  does (witness runs 2 and 3).
  **Not claimed:** F-121 is the never-written case only. A transmission followed by the
  same removal with its completion lost is F-088's contract failure, not F-121's, and
  F-121 must not absorb one. **Not fixed here:** a gate check before the store operation
  is no concurrency proof, so the fix needs an ordering design. Disclosed in
  `docs/CONFORMANCE.md` ("A never-sent message can enter that possibly-attempted
  population"). **Closure:** an ordering design under which a certified connection's
  intent cannot strand its exchange, with the test's assertions inverted deliberately to
  assert the corrected behaviour (never loosened), and the disclosure retired.
- [x] **F-122: a displaced resident subscription keeps re-offering, and churns refused
  admissions, while its eviction is delayed.** **ACCEPTED RESIDUAL (2026-10-12 convergence
  triage).** Shown only under the fixture's artificial eviction hold, with no completion
  lost; stated in `docs/CONFORMANCE.md` under "A displaced subscription churns while its
  eviction is delayed". Filed 2026-10-11 by the F-097/F-088
  close-out (consults C-W3 and C-W4). Observed with broker A's eviction held by the
  fixture (`testEvictPauseBeforeLoad`) in
  `TestF088CrossTenureLegitimateTransmissionThenCancellation`: A's displaced session's
  still-live subscription keeps redelivering, and each offer's admission is refused
  ("unable to record the in-flight register for packet identifier N ... entitlement that
  has moved on"), about 1,750 identifiers per run in about 15 s. That rate was LOGGED,
  not asserted. **Starvation evidence (C-W4):** while the eviction is delayed, the
  displaced session's authority writes starve the successor's authority-CAS advances.
  With the PUBACK sent on the successor during the hold, that test failed 1 of 6 in a
  full parallel run, and `TestF088CrossTenureAckBeforeTheHoldersDrainCompletes` lost every
  advance for 15 s. The completion stays owed after a lost CAS (the terminal retry owner
  re-arms without a budget), so it is delayed for the hold's length rather than lost;
  both tests now release A's eviction before the PUBACK, so the starvation is the
  fixture's artificial hold, recorded here. **Not claimed:** identifier exhaustion, any
  bound on production eviction latency, or that a production eviction is ever delayed
  this long. Not a prerequisite for F-097/F-088. **Closure:** a deterministic witness of
  the churn and of the starvation, and a design that stops or bounds a displaced
  session's delivery processing while preserving the successor's service.
- [x] **F-124: a drain finalize retried with no connection leaves the retired drain's
  consumer to the next resume.** **ACCEPTED RESIDUAL (2026-10-12 convergence triage).**
  Reached only after a failed last-claim removal save; the exchange completes and releases
  its resources, and the retired consumer's retention lasts until resume settlement or
  generation disposal, with no time or storage bound while the session continues. The
  existing disclosure in `docs/CONFORMANCE.md` is the residual. Filed 2026-10-11 by the
  F-097/F-088 close-out (consult
  C-W5). The terminal-source continuation is armed with no connection
  (`holdTerminalSourceLocked` calls `armTerminalRetryLocked(nil, ...)`,
  `outbound_source_completion.go:151`). When it retries a drain's finalize
  (`finalizeDrainDelivery`, `unsub_drain.go:1692`), the removal entitlement it reads from
  that connection is zero (`unsub_drain.go:1695`); `markRemovalIfEligibleLocked`
  (`unsub_drain.go:3824`) accepts the zero, and disposal correctly refuses to record a
  decision no accepted entitlement made and defers to recovery
  (`outbound_source_retire.go:153-158`). That breaks the drain's invariant that the
  removal is the deciding connection's and is recorded under its entitlement or not at
  all (`removalEnt`, `unsub_drain.go:415-418`). **Consequence:** under `InterestPolicy`
  the retired drain consumer keeps retaining later publications that match its filter
  until resume settlement (F-105 7f) or generation disposal; no fixed time or storage
  bound is claimed. The observed exchange completes and releases resources in-session; it
  does not demonstrate false non-transmission, lost completion authority, or conflicting
  delivery under C1. The retained consumer is a supported-feature liveness/storage defect.
  **Evidence:** the injected arm of `TestF088RecoveredDrainClaimRetiresDespiteAFailedSave`
  (one failed claim-removal save) is its characterization: before any resume the failure
  is consumed, the claim retires, the exchange completes and the resources are released;
  the next resume's F-105 7f executor decides the consumer and a CLASSIFIED
  consumer-not-found follows. It gets no credit for in-session disposal after the
  failure, and is re-expressed when F-124 lands. The baseline arm deletes the consumer
  in-session. **Provenance:** the mechanism is in code the close-out leaves unchanged
  against `48a9003`; observed with the close-out applied, not separately reproduced on
  `48a9003`. **Not fixed here:** refusing a zero at marking needs a demonstrated owner
  that re-marks and disposes, and carrying the entitlement across retries and tenure
  changes needs an ownership design that keeps disposal authority distinct from MQTT ACK
  authority. Disclosed in `docs/CONFORMANCE.md` ("A drain whose completion is retried
  after a failed save"). Its resolution does not gate the F-097/F-088 ticks; its filing,
  this disclosure and the revised characterization do. **Closure:** an
  ownership design for the disposal decision on connectionless retries, plus a
  deterministic witness of in-session disposal after the failure.

### 0.4 Release-verification investigations

These entries preserve observations without pretending the implementation scope is
known. Capture complete logs, obtain a deterministic reproduction or identify the
failed test, and then close the investigation, accept it as a residual, or move it to the
[verification ledger](findings/release-investigations.md#verification-ledger). Only a
confirmed loss, corruption or security mechanism becomes a new §0.3 finding (the filing
rule at the top of §0.3). This section keeps only observations whose dangerous mechanism
is still unexcluded; pure test-infrastructure and unattributed flake observations live in
the ledger, whose entries satisfy the alpha gate only by an explicit accepted closure or a
confirmed finding's disposition, and never waive green CI.

- [x] [F-040: Reproduce reconnect versus DISCONNECT
expiry-zero](findings/session-and-will.md#f-040).
  **REPRODUCED and FIXED (2026-10-14).** Deterministic at `903bee1`, 3/3: hold the old
  connection's close at the teardown seam (after the DISCONNECT recorded its Session Expiry
  Interval 0 override, before the close contends for the gate), then CONNECT with Clean
  Start 0; the takeover resumed the session and reported Session Present 1, because the
  resume decision read only the session's confirmed terms and the close, finding the
  session rebound, did nothing. The takeover now persists the bound connection's ending
  override on the close's behalf (F-041's `applyRequestedLifetime` under
  `mqttLifetimeFinal`, fenced, per outcome) BEFORE its resume decision and before any
  detach, so `es.clean` drives the existing discard and every post-detach failure leaves a
  Clean record. Tests: `TestMQTTv5TakeoverHonorsBoundDisconnectExpiryZero` (with no-property
  and finite-override controls), `TestMQTTv5TakeoverDisconnectEndingSurvivesCancelledHandoff`,
  `TestMQTTv5TakeoverBoundDisconnectRefusesOnLateUncertainty`; six mutations each killed.
  **Residuals** in `docs/CONFORMANCE.md` ("A reconnect racing its own DISCONNECT"): an
  override recorded after the takeover's sample is a displaced connection's and does
  nothing; across brokers the record arbitrates; a definite failure of the ending write
  keeps F-041's residual. Its verification met four unrecorded pre-existing reds (equal at
  clean `903bee1`, interleaved); they are in the verification ledger.

- [x] [F-042: Identify the unexplained full-suite
failure](findings/release-investigations.md#f-042).
  **CLOSED as unidentifiable, not refuted (2026-10-12 convergence triage).** The failing
  test's identity was destroyed by a truncated log and 20 later full-suite runs did not
  reproduce it, so there is nothing actionable to keep open. Do not truncate suite output.

- [x] [F-044: Determine whether the phase-2 shutdown observation is
real](findings/release-investigations.md#f-044).
  **MOVED to the verification ledger (2026-10-12 convergence triage)** as an unresolved
  shutdown scan-count observation, accepted as nonblocking on present evidence; neither
  scheduling noise nor a broker defect is established.

- [x] **F-117: a re-SUBSCRIBE that changed No Local re-judged owned messages.**
  **REPRODUCED and FIXED (2026-10-14).**
  `TestMQTTv5NoLocal/QoS1_re-subscribe_toggles_No_Local` failed about 60% under `-race`
  because the delivery callbacks read the subscription's LIVE No Local option: m2,
  owned while the option was ON, reached its callback once (delivery count 1) just
  after the toggle-off refresh and was delivered. The same read, in the other
  direction, acknowledged away a message owned while the option was OFF once a
  re-SUBSCRIBE turned it ON, a loss ([MQTT-3.8.4-4]). [MQTT-4.5.0-1] fixes
  membership at ownership, so the decision now is a per-message stamp taken there
  (`Nmqtt-NoLocal`, one HMAC tag per publisher's own No Local filter that reaches
  the subject, `nolocal_stamp.go`), carried by the QoS 2 hold from PUBREC to
  PUBREL, and read by both delivery callbacks keyed by the subscription's filter
  key; retained replay keeps the current option. Tests in
  `f117_nolocal_admission_e2e_test.go`: both QoS 1 directions held at the
  delivery seam, both QoS 2 directions across PUBREC and PUBREL, overlapping
  filters, literal and wildcard level-up parents, a restored companion's backlog,
  and a DUP QoS 2 not refused for a stamp only a new hold would write.
  **Residuals** in `docs/CONFORMANCE.md` ("No Local is decided when the message is
  owned"): another server run does not verify the tags (fails open); the order is
  per connection; and QoS 2 fans out at PUBREL, so a subscription created between
  PUBREC and PUBREL receives the message, which now includes the publisher's own
  No Local filter created there (new behavior from this fix, characterized by
  `TestF117PubRelWindowSelfEchoResidual`; the pre-existing window is the defect). Its
  verification met three unrecorded pre-existing reds (equal at clean `28e5516`,
  interleaved); they are in the verification ledger.

- [x] **F-118: `TestMQTTRedeliveryAckWait` fails intermittently, pre-existing, mechanism
  untraced.** **MOVED to the verification ledger (2026-10-12 convergence triage)** as an
  unresolved drain/reservation liveness observation, not a proven fixture flake; its
  corrective work is owed before CI can be green. Allocated 2026-10-06, filed by F-105
  WP-F W3. The test (`mqtt_test.go`)
  fails with "Maps should be empty, got 1, 1", and its log carries "the durable PUBLISH
  action ... could not be drained; its packet identifier stays reserved"
  (`inflight_register.go` ~7431). It is red in the F-105 8a, 8b, 8c and WP-F W2 chains
  and absent from the W1 and F-113 chains. Measured interleaved A/B, prebuilt `-race`
  binaries run from one directory: base `c374099` failed 7 of 10, the W2 candidate 3 of
  10, so W2 did not cause it. The mechanism is not traced; a QoS 1 action drain that never
  lands and keeps the identifier (F-116's neighbourhood) is a candidate only, not a
  finding. Never loosen the assertion; treat it as pre-existing when comparing suites.

- [x] **F-123: `TestF097DispatcherIsFairUnderContinuousHints` fails intermittently,
  pre-existing, mechanism untraced.** **MOVED to the verification ledger (2026-10-12
  convergence triage)** as a suspected measurement or dispatcher failure; its closure
  below is the ledger entry's required work. Filed 2026-10-11 by the F-097/F-088 close-out
  (consult C-W4). The test (`outbound_custody_driver_test.go`) fails with "the item and
  the gate request took 4 iterations under continuous hints, want <= 3". Measured: 1 of 10
  at clean `48a9003` in a throwaway worktree (`-count=10`, tagged build), and 1 of 5 with
  the close-out's fix applied, which does not touch the dispatcher. That establishes the
  failure predates the fix; it does not establish whether the dispatcher or the
  measurement is wrong. The boundary to investigate first: the test samples `base` before
  registering both obligations, so iterations run between that sample and the second
  registration count against the bound. The close-out's regression run is not claimed
  fully green on its account. The test is untouched; never loosen the assertion, and
  treat it as pre-existing when comparing suites. **Closure:** a traced mechanism, then a
  dispatcher fix with this test as its witness, or a measurement correction justified by
  that trace.

- [x] End-to-end phase-aware lease ownership for the delayed-Will publish worker.
  **ACCEPTED RESIDUAL (2026-10-12 convergence triage).** Subscribers can receive a
  duplicate Will when a claimant stalls past its lease during subscription restoration.
  Two publications is the bound for one stalled claimant followed by a peer that
  completes; successive stalled claimants or a failed terminal reconciliation can produce
  more. The design notes below remain the fix if it is taken up, including the warning
  that bounding the barrier trades the duplicate for a loss.
  **Confirmed defect, bounded scope, NOT an investigation.** The worker does not renew,
  and the construction check bounds one JetStream REQUEST below the lease — not the
  worker's licensed lifetime. `mqttInitiateMsgDeliveryJSA` fans the Will out externally
  and then waits on `mqttSubsBarrier`, a wait bounded by SHUTDOWN. That barrier is held
  for a whole `processSubs`, which creates a consumer per filter, so a CONNECT session
  restore with a few dozen subscriptions on a cluster timing requests out holds it past
  30s; a peer reclaims and republishes with both brokers alive. That is TWO publications
  per stall, not unboundedly many — the barrier is per account manager, so the reclaiming
  peer is not stalled and commits `done`; a third needs that peer to outlive its own fresh
  lease too. Disclosed in `docs/CONFORMANCE.md` as the sanctioned duplicate's true
  boundary — a disclosure, not a bound.

  What a fix must do, and what it must not claim:
  - Renew the claim through dispatch, the barrier wait, publication and the terminal
    commit, with renewal and finalization SERIALIZED — a free-running renewal CAS
    would keep staling the worker's own `done` CAS.
  - Treat renewal failure by PHASE: before the irreversible fan-out an unconfirmed
    renewal stops the worker safely; after it, nothing can be stopped, so the worker
    must continue through barrier, store and terminal reconciliation as best it can.
    A design that pretends these are the same case is wrong.
  - Not claim exactly-once. A fencing token cannot make external subscriber delivery
    exactly-once unless the recipients participate in the fence; renewal narrows the
    ambiguity window and never closes it.
  - Not bound the barrier as the fix: the fan-out precedes it, so abandoning there
    trades a duplicate for a LOSS of the store that protects the racing subscriber.

  Verification: a two-broker regression holding one broker's barrier past a shortened
  lease (`mqttTestSubsBarrierHeldHookV` already provides the seam) and asserting the
  subscriber receives the Will exactly once.

- [x] Fix `TestMQTTOriginFirstEverCreateRaceReplicated`'s rejection-count assertion.
  **RETIRED by deletion, not fixed.** The write-lineage audit deleted the
  session-interval register and every test of it, this one included, so there is no
  assertion left to correct. The diagnosis below is kept because it is a worked example
  of a count assertion that is only correct for one concurrency, which is a shape that
  recurs.
  **Investigation, but the reproduction and cause are already in hand.** The test
  requires `gotEstablishes >= 2` and then asserts `gotRejections != 1`
  (`sess_origin_interval_cluster_test.go:207`). When a retry produces a THIRD establish
  attempt, two rejections out of three is the CORRECT outcome — one winner, two losers —
  and the assertion fails a passing broker. Measured in isolation with `-count=8` on
  2026-08-14: **2/8 at `81a8b9f`, 3/8 with the acceptance-fact change**, so it is
  pre-existing and the rates are within noise of each other; it is not the concurrent-suite
  starvation the other flakes are. Re-measured 2026-08-15 during the F-030 work at
  **2/12 on a clean checkout of `4db3764` and 2/12 on the working tree** — identical, and
  confirming both the diagnosis and that it is unrelated to whatever change is in flight.
  At roughly one failure in six on an ungated CI job, this is worth doing. The fix is to
  assert the INVARIANT — exactly one
  establish succeeds, every other attempt is rejected — rather than a fixed rejection
  count, which is only correct for exactly two attempts. Not folded into the
  acceptance-fact change: different mechanism (F-029b origin intervals), and that diff
  already spans 46 files.

- [x] Make the two `AbortsThePassOnATransientFactOutage` fixtures tolerate concurrent
  load — and settle whether the candidate-only half is really only a fixture.
  **MOVED to the verification ledger (2026-10-12 convergence triage)**, keeping the
  candidate-only caveat below.
  `TestWillClaimantAbortsThePassOnATransientFactOutage` and its sibling
  `TestWillPendingArmAbortsThePassOnATransientFactOutage`
  (`will_accept_fence_test.go:427` and `:476`) seed TWO expired records, inject a
  fact-read failure on the first, and assert the pass issued exactly ONE read — the
  point being that an outage costs one timeout per PASS, not one per record. Under
  load the assertion intermittently sees 2.
  **What was measured, at exact-test granularity.** A paired soak against `eadb22d`
  (the commit before the pre-CONNACK-signal work), selector
  `TestQ2Aff|TestMQTTQ2Aff|TestWill|TestMQTTWill`, 450 tests per run, **20 runs per
  arm POOLED FROM TWO INVOCATIONS of `make soak` (8 then 12)** with the same selector,
  refs and candidate tree — the script resets its worktree, logs and alternation
  between invocations, so the provenance matters for reproducing this. Pooled runs
  failing: baseline 9/20, candidate 7/20. Every failing test name is shared between
  the arms EXCEPT this pair, and the split is exactly one half each: **the Claimant
  test failed 2/20 in the candidate arm and 0/20 in the baseline; the Pending test
  failed 2/20 in the baseline and 0/20 in the candidate.**
  **COVERAGE CAVEAT, because it bounds the comparison.** 38 of the 40 runs executed
  the full 450 tests; **two did not** — one per arm, run 3 of the 12-run invocation —
  because `TestQ2AffOnBehalfRestorationIsNotChargedToTmaxack` PANICKED, truncating
  those runs at 286 (baseline) and 309 (candidate) tests. Both Will tests above did
  execute in all 20 runs of each arm, which is checked and is what makes their
  2/20 and 0/20 counts sound; but tests ordered after the panic had no chance to run
  in those two, so "no other name is arm-exclusive" is a claim about 38 full runs and
  2 partial ones rather than 40 full ones.
  **What that does and does not establish.** The two halves exercise separate switch
  arms, so reading them as one family is a POST-HOC aggregation, not the
  pre-registered rule — which said a candidate-only name is a regression signal
  whatever the totals. So this bullet does NOT claim "not a regression". What is
  established: the aggregate rate did not rise, no other name is arm-exclusive, and
  the scan and fixture code are untouched by that work. What is inferred but not
  settled: that concurrent passes inflate the count, which the observed value of
  exactly 2 supports. The isolation evidence covers **only the Claimant half** —
  0/30 on each arm run alone — and the Pending half was not run that way.
  **What would settle it:** run each half alone under injected concurrency, or make
  the assertion count reads for THIS pass rather than for the fixture's lifetime, so
  a neighbouring pass cannot contribute. Until then treat the Claimant half as
  suspect rather than dismissed.

- [x] Stop `testQ2AffAwaitMaterialized` dereferencing a reaped claim.
  **PARKED with the experimental shared-QoS-2 suite in chapter P, not fixed.** It is test
  infrastructure, and it CRASHES the experimental run rather than failing a test.
  `qos2_shared_affine_lifecycle_test.go:5499` reads `claim.Phase` on the result of
  `testQ2AffLoadClaim`, which is nil once the claim has been reaped — so the poll
  loop nil-derefs and the whole test binary takes a SIGSEGV, ending that run early
  and taking every later test with it. Reached through
  `TestQ2AffOnBehalfRestorationIsNotChargedToTmaxack`
  (`qos2_shared_affine_lifecycle_test.go:5521`).
  **MEASURED as pre-existing and symmetric**: it fired once in each arm of the
  paired soak above — the baseline arm at `eadb22d` and the candidate arm alike —
  so it is neither introduced nor fixed by the pre-CONNACK-signal work. It is
  recorded because a panic is worse than a flake: it truncates the run, which is
  what put the coverage caveat on that soak. A nil check that keeps polling until
  the deadline is the obvious fix; the awaited condition is already "the claim
  exists AND is materialized".
- [x] Make `TestQ2AffineResumeDoesNotRechargeAckBudget`'s reconnect loop tolerate a closed
  connection. **PARKED with the experimental shared-QoS-2 suite in chapter P, not fixed.**
  Test infrastructure, not a supported broker gap, and MEASURED as pre-existing.
  It closes and reopens the member three times in a loop
  (`qos2_shared_affine_lifecycle_test.go:791-800`) and intermittently fails at
  `:793` with `Error reading data: EOF` — the reconnect racing the previous
  connection's teardown, not the MaxAckPending accounting the test is about.
  Paired-soak rates, alternating arms under `-race`: **baseline 2/12 and candidate 0/12**
  with the F-063 selector, then **baseline 1/20 and candidate 1/20** with the drain tests
  added — combined **3/32 baseline against 1/32 candidate**, so it predates the tree and the
  soak's own verdict is "comparable rate, the failure predates this tree". Deliberately NOT
  folded into F-063: the two consults that adjudicated that change's scope set the bound
  that a failure appearing equally on both arms becomes its own item, and the fix is to the
  reconnect handshake rather than to anything F-063 touches. The three fixture defects that
  soak DID attribute to F-063's load were fixed there.

- [x] Cover retained convergence for a sourced `$MQTT_rmsgs` topology.
  **ACCEPTED as a coverage gap (2026-10-12 convergence triage)**: no convergence defect
  is reproduced, and `docs/CONFORMANCE.md` now states the sourced topology as untested,
  including TTL expiry and delete-marker propagation. This does not close F-059.
  **Investigation.** `docs/ARCHITECTURE.md` declares a cross-account sourced
  retained stream a supported topology, but the only test of it lives in the
  unbuilt `mqttport_clustertests` reference file, so nothing running exercises a
  record arriving through a source with a subject transform. Establish whether a
  native test can stand it up; if it can, the test is release verification. Include
  TTL expiry and any F-059 delete marker through the source: source/mirror marker and
  TTL behavior differs, so the unsourced test is not a substitute.

- [x] [F-070: Establish the affine contender fixture's rival-create premise rather than
  racing it](findings/release-investigations.md#f-070). **PARKED, NOT FIXED: experimental
  shared-QoS-2 test infrastructure in chapter P.** Filed with
  the mechanism PROVEN and the attribution SETTLED against a baseline on 2026-08-22, but never
  given an authoritative checkbox here, so it was invisible to the roadmap's own todo.
  `TestQ2AffRacingAbsenceConverge/the_relookup_finds_it_absent_again` installs a create hook
  that stages a RIVAL consumer under the same durable name with `MemoryStorage` inverted;
  JetStream treats `CONSUMER.CREATE` against an existing durable as an update and refuses
  a change to an immutable field, so the staging succeeds only while that durable does not
  yet exist. The fixture assumes it wins that race and does not establish it, and under
  load it loses. **The assertions the test makes are correct** — this is a fixture
  premise, not broker behaviour, which is why it is release verification rather than a
  blocker. F-084's soak re-measured it independently on 2026-08-27 at 4/10 failures on a
  clean `HEAD` worktree against 1/10 on its own tree, i.e. the flake is pre-existing and load-sensitive
  rather than attributable to any change; that measurement is further evidence for the same
  rival-create signature, not a second finding.

- [x] **Page the shared `$MQTT_out` consumer enumeration.** **MOVED to §2
  "Performance and resource audits" (2026-10-12 convergence triage)**; the existing
  disclosure that discovery scales with the whole consumer population stays. Filed
  2026-09-03 by F-036
  unit 2's escalation consult, which ACCEPTED that this is not that item's blocker but
  refused to let it be called closed. `consumerList` walks the entire `$MQTT_out`
  population on every orphan-sweep pass, and the candidate collection and the sort that
  follow grow with it. That was already true before unit 2 — the affine consumer leak
  course E accepted rides the same enumeration — so it is pre-existing rather than
  introduced. What changed is that it now MATTERS more: unit 2's retirement proof gate
  retains an unbounded population of PUBREL consumers by design, so the population this
  walk traverses grows without bound rather than converging. Unit 2 bounded the
  per-candidate REVALIDATION round trips (a per-pass budget with a durable-name cursor,
  so a persistent candidate is still eventually visited) and deliberately did NOT bound
  the discovery around them; `docs/ARCHITECTURE.md` and `docs/CONFORMANCE.md` both say so
  in those words rather than claiming bounded work.
  **What this item owes:** paged or bounded listing, bounded discovery memory, and a
  sort that does not require the whole population in hand — plus a decision on whether
  the affine and PUBREL classes share one paged walk or take one each. Not a blocker for
  F-036, which is closed.

- [x] **`TestSharedDenyDoesNotConsumeForAllowedMembers` fails intermittently, and it
  reproduces on clean HEAD.**
  **CLOSED as a fixture premise, measured (2026-10-14).** Nothing is lost. The ordinary
  shared deny hands each offer back on `mqttPacedRefusalNak`'s backoff, and nats-server
  routes each re-offer to a queue member picked from a random start index
  (`server/client.go:5638-5652`, v2.15.1), without consulting the MQTT ACL. alice joined
  at ~3s, when the backoff had reached seconds per offer, so the 15s window held about
  three offers, each going to the denied bob with probability one half: a miss about one
  run in eight. Measured at `9bf3a4d` with a diagnostic copy of the fixture, isolated and
  unloaded: 2 of 24 runs missed the window, and both were LATE, not lost (alice received
  at 25.5s on delivery 9 and at 81.1s on delivery 11). The shared consumer showed one
  ack-pending message with ack floor 0 throughout, and every arrival in all 24 runs fell
  on a scheduled paced offer. The historical "load-sensitive" reading below is
  **withdrawn as unsupported**: the rate needs no load. The historical rates are
  compatible with this mechanism, but rates alone cannot prove that every past failure
  had this cause.
  **The fixture now decides both halves deterministically.** (1) After bob's refusals,
  the group's two durables (`shared/#` maps to `shared` and `shared.>`) are read through
  the error-reporting consumer listing: the sum of ack-pending must be 1 and every ack
  floor must be 0. A terminal ACK clears both, and nothing else on this route does.
  (2) alice joins and tolerates a PUBLISH arriving before her SUBACK. Then bob DISCONNECTs,
  and his departure is made a server-side fact: nats-server's interest on both deliver
  subjects is 2 before he leaves and 1 after, because the broker forgets a client before
  it unsubscribes its queue members. Every later offer is then alice's. Her window covers
  one pending refusal delay (`mqttPacedRefusalMaxDelay` + 5s). Result: 48 of 48 pass,
  3.4-4.2s each. Mutations: a terminal ACK on the shared deny is killed by the accounting
  assertion (ack floor 1), and separately, with the accounting disabled, by alice's
  receipt. An immediate NAK is killed by the pacing bound (11486 re-offers in 3s).
  Skipping bob's departure but keeping the barrier fails at the barrier (2 members, want
  1). Skipping the departure and its barrier and restoring the 15s window reproduced the
  original miss (1 of 24, after the accounting had passed, so the message was provably
  still owed).
  **The false bound it rested on is corrected.** `docs/CONFORMANCE.md` residual 11,
  `docs/ARCHITECTURE.md` and the deny comment in `mqtt.go` claimed a late joiner is
  served on the next re-offer, within the 30s cap. In fact the offers until an allowed
  member is picked are geometric in number, with no finite deadline while a denied member
  stays in the group, and the cap bounds each gap, not the wait. Residual 11 now states
  that, with the 81.1s observation as an instance, not a bound.

  **The original record.** Observed 2026-09-06 while verifying the delivery-readiness
  change, under `-tags mqttport_fulltests`. **On the working tree:** pass, pass, fail.
  **On a clean `d75bdb1` worktree carrying none of that change:** pass, pass, pass,
  fail. Failing runs took ~18.2 s each.

  **What that does and does not establish.** It establishes that the failure occurs
  **without** the change, so the change is not required to produce it. It does **not**
  establish equal failure rates — one in three against one in four is far too little
  evidence for that — nor that the two are the same mechanism; matching durations do
  not show matching causes, and exacerbation is not excluded. Do not cite this bullet
  as a clean bill of health for a later change in this area; re-pair the baseline.

  The failure is an **assertion**, not a timed-out harness:
  `authz_affine_test.go:496`, "an allowed shared member joining after a DENIED member
  was offered the group message must still receive it: a terminal ACK on the deny
  destroyed it". The ~18 s is the wait that precedes it, so a lost delivery produces
  exactly that shape. Mechanism unattributed. Same class as the `TestMQTTInflight*`
  bullet below. **The baseline establishes reproduction without the change;
  attribution of the working-tree failure remains unresolved** — reproduction on a
  clean tree cannot tell an unrelated baseline failure apart from a change-induced one
  reaching the same assertion.

  **Two earlier measurements were consolidated in from a duplicate bullet on 2026-09-20.**
  That bullet was ticked and dispositioned "PARKED with the experimental shared-QoS-2 suite
  in chapter P", which was WRONG — this test is not affine — so it was removed and its
  evidence moved here rather than left to contradict this entry. Its findings are
  **attributed historical conclusions of the lanes that made them**, and they do not weaken
  the warnings above.

  - **F-089 lane, 2026-08-30.** Met once in a full-suite run, then measured rather than
    guessed. **Interleaved** repetition of the test alone gave **2/12 on the F-089 tree
    against 1/12 on a clean `HEAD` worktree**, so it fails without that change at a
    comparable rate. **Isolated single runs pass on both arms (3/3 each), so it is
    load-sensitive.** F-089 also **cannot reach it**: with no PUBREL debt anywhere in the
    flow, `sendReservedLocked` is semantically identical to the registry-presence test it
    replaced.
  - **F-066 enforcement lane, 2026-09-01**, recorded because it also shows how easily this
    one is misread. Repetition gave **3/48 on a clean `HEAD` worktree against 4/24 on the
    change tree**, but the `HEAD` figure arrived as 0/12, 0/12 and then 3/24 — so a
    two-block sample had shown 0 against 4 and looked like an attribution. That lane
    concluded it is not one (Fisher p ≈ 0.19, and the pooled rates matching the
    1/12-versus-2/12 above) — an interpretation of ITS data, not a general finding of equal
    mechanisms or of non-exacerbation, which the warnings above explicitly refuse. Disabling
    F-066's new ten-lookup startup preflight entirely still left **1/12**, which rules out
    added initialization latency as the driver.
  - **Take at least ~24 repetitions per arm before concluding anything here.**
  - The failing assertion is cited as `authz_affine_test.go:494` by the F-089/F-066 record
    and `:496` by the 2026-09-06 one; both quote the same text.
  - **Whether this is a fixture premise or a real broker race is exactly what is
    unestablished**, and the two have different classifications: a deny path that terminally
    acknowledges a group message another member is entitled to would be a data-loss defect
    and an alpha blocker, while a fixture that races the join against the deny is release
    verification. **Do not assume the F-070 shape** — that one was proven a fixture premise,
    and reasoning by analogy is what this bullet exists to prevent. Establish the mechanism
    against a baseline before classifying it.

- [x] **Attribute the `TestMQTTInflight*` family's intermittent failures.** Filed
  2026-09-01 by the F-066 enforcement lane, which met one in a full-suite run and then
  MEASURED it on both arms rather than guessing. **Pre-existing and NOT attributable to
  that change:** repeating the family under load gave **3/12 on a clean `HEAD` worktree
  against 2/12 on the change tree**, so it fails without the change at a comparable
  rate — and F-066's enforcement cannot reach it, since its only edits to
  `inflight_register.go` are a startup-time validator argument and comments, neither on
  the `endSend` path any of these assertions exercise.
  **A different member fails each run**, which is the part that matters for attribution:
  `HEAD` produced `TestMQTTInflightLandedButUnconfirmedAckLeavesNothing` twice and
  `TestMQTTInflightPubAckParksThroughTheAckPath` once; the change tree produced
  `TestMQTTInflightOrphanReconcile` and `TestMQTTInflightTerminalAckParksWhileASendIsInFlight`
  once each. So it is a family-level load sensitivity rather than one bad fixture, and a
  bug hunt that starts from whichever test failed today will chase the wrong thing.
  Isolated single runs pass (5/5 on the change tree for the first one observed), so it is
  load-sensitive in the same way as the entry below. The first question is whether the
  shared fixture these tests build serializes something the parallel suite does not, and
  the answer is worth having before alpha because the assertions are about deferred
  register deletes — the F-087/F-091 machinery.
  **A THIRD intermittent met in the same verification is a different phenomenon and is
  recorded here so it is not merged into this one:** `TestMQTTMaxAckPending` failed once
  under full-suite load with `TempDir RemoveAll cleanup: ... /streams/$MQTT_msgs/obs/...:
  directory not empty` — Go's own temp-directory cleanup racing JetStream's file removal,
  not a broker assertion at all. It reproduced 0/12 on both arms in isolation. Whoever
  takes this bullet should not spend time on it as if it were the same defect. It is
  carried in the verification ledger since 2026-10-12, not by this bullet.
  **ATTRIBUTED (2026-10-15); one broker mechanism FIXED, one residual ACCEPTED.** Closure
  here means attribution and disposition per member, not a green family run. Measured at
  `b334415` on a prebuilt tagged `-race` binary: the four named members failed 0 of 19
  family runs (11 at `-parallel 8`, 8 CPU-starved at `GOMAXPROCS=2`). There is no shared
  server fixture, and no process-global interference was identified (every family test
  installing a `setMQTTTest*` hook is serial, under
  `TestParallelTestsTouchNoProcessGlobalState`). The "family-level" reading does not hold;
  per member: `TerminalAckParksWhileASendIsInFlight` — an immediate read of an asynchronous
  delete, already fixed by 9a16434 four days after the filing;
  `PubAckParksThroughTheAckPath` — the same premise, unconverted (a first terminal attempt
  that cannot confirm hands off to the retry by design), now observed at the designed
  completion; `OrphanReconcile` — F-069, now CLOSED: the test owns its sightings (its own
  quarantine and grace, `reconcileOrphanInflight` taking both as parameters) and observes
  the delete request rather than reading back a fire-and-forget delete;
  `LandedButUnconfirmedAckLeavesNothing` — **unattributed**, carried in the verification
  ledger: its fixture is made faithful (the ack really lands before its confirmation is
  reported lost) and its retirement assertion moved before the PUBCOMP, which made its
  documented mutation real.
  **A fifth member the bullet never named carried two mechanisms.**
  `TransmittedRedeliversWithDup` (T4) failed under CPU starvation with an acknowledged
  QoS 1 message delivered again as a new one (fresh identifier, DUP=0).
  (1) **Fixed: the parked release blocked the delivery goroutine.** A PUBACK landing on a
  still-draining send section parks, and the section's end ran the acknowledgement's whole
  terminal release inline on the subscription's delivery goroutine, so the source's
  re-offers queued behind it and, once the release had lifted the suppression, were
  admitted as new deliveries. Witnessed deterministically (4 of 4 with the parked PUBACK,
  0 of 4 with the PUBACK sent after the section closed). Every send-section completion site
  now hands the release to the terminal-retry dispatcher with a prompt first attempt
  (`handOffAcknowledgedPublish`); only the read loop's handler completes inline, bound by
  `TestMQTTAcknowledgedReleaseRunsInlineOnlyOnTheReadLoop`. Proof:
  `TestMQTTInflightParkedAckReleaseLeavesReOffersSuppressed` (500 ms AckWait, controlled
  overlap; 10 of 10, and the duplicate reappears in it when the release runs inline);
  `TestMQTTInflightQoS2SlowSourceAckLeavesNoDuplicate` (QoS 2 control);
  `TestMQTTTerminalRetryPromptFirstAttempt`.
  (2) **Accepted as a residual: a re-offer that came due before the acknowledgement was
  processed.** The fix did NOT move T4's starved rate at its original 500 ms AckWait
  (interleaved, 8 runs per arm: 8 of 8 at the baseline, 7 of 8 on the change): there the
  release took 590-680 ms, so JetStream's re-offer came due before the confirmation and was
  admitted 165-222 ms after it. That window is the residual in `docs/CONFORMANCE.md` ("a
  QoS 1 re-offer dispatched before its acknowledgement landed"); at the 30 s default it
  needs a release slower than 30 s. T4's AckWait is therefore a 2 s fixture BUDGET (as
  F-115 set for its fixture), every assertion unchanged: 0 of 8 starved runs.
  Tests whose immediate observation of the parked continuation moved to its designed
  completion (claims unchanged): `TerminalAckParksWhileASendIsInFlight`,
  `PubAckParksThroughTheAckPath`, `TestMQTTAcknowledgementTerminalCleanupSurvivesFailures`
  (its deferred cases now await the injected fault, which a failed-delete witness proves).
  **Remaining reds, carried in the verification ledger, not here.** Pre-existing:
  `StrandedReservationOverADeletedConsumerIsReleased` (11 of 11) and the starved-only
  `RetiredSourceSurvivesRestart` (interleaved CPU-starved family runs, 4 of 18 on each
  arm). UNMATCHED — seen once each on the change only, never reproduced on either arm, so
  neither pre-existence nor causation is established:
  `RecoveryPubRelKeepsOwnerWhenSourceComplete` (1 of 28 starved family runs on the change,
  0 of 18 at the baseline; 0 of 200 per arm in isolation) and
  `QoS2IdentifierSurvivesRestartBeforePubRec` (1 of 10 against 0 of 10, a PUBREL
  retransmission where its flush expected PINGRESP).

- [x] **The Paho suite fails against this broker beyond its baseline.** Filed 2026-09-20
  by the v2.15.0 GA bump's conformance verification as a `test_keepalive` failure.
  Re-titled 2026-10-15, when that failure was root-caused and fixed and a second one,
  `test_subscribe_options`, was found. Neither failure is in
  `test/conformance/baseline/paho.txt`: a regression is not a documented divergence.
  **CLOSED 2026-10-15: both FIXED and verified on the local rig; the CI-runner confirmation
  is still outstanding.** Closing does not mean the Paho CI job is green, and it does not
  guarantee `test_keepalive`'s 0.3 s CONNACK on a slower machine (its margin is below). A
  runner that fails either test is a new filing that links this history. Decided by
  consult (Codex and the advisor agreeing): the causal fixes and the comparative
  measurements close the box, and an environment confirmation no lane can perform is not
  an open implementation defect.
  **`test_keepalive`: FIXED (2026-10-15).** It was never a keepalive defect. It runs right
  after `test_flow_control2`, which floods `myclientid` (Session Expiry 0) with 1025 QoS 2
  PUBLISHes. The broker PUBRECs 1024 (the default Receive Maximum), answers DISCONNECT
  0x93 on the 1025th, and the session's discard then arbitrates the 1024 inbound holds.
  Serially that was two JS round trips per hold, about a second, and it ran under the
  teardown that the next same-ID CONNECT waits for. `test_keepalive`'s `setUpClass`
  `cleanup()` CONNECTs `myclientid` with Clean Start the moment the previous process
  exits, and its Paho client allows 0.3 s for the CONNACK (`getPacket` sets the socket
  timeout). The report was `Ran 0 tests`. The walk (`arbitrateQoS2ReleaseClaims`) now runs
  the per-hold arbitrations through `mqttWalkBounded`, at most `mqttQoS2DiscardWalkWidth`
  (32) at once, with every per-hold decision unchanged. A failure stops admission, and the
  error returned names the lowest-sorted failing hold. See ARCHITECTURE "The release
  claim" and CONFORMANCE "Reconnect after a discard of many held QoS 2 exchanges".
  **Measured on a local rig, not a CI runner**, because this sandbox cannot reach a Docker
  daemon. The rig is nats-server v2.15.1 built from the pinned module, with ci.yml's paho
  job configuration (`test/conformance/nats.conf`, auth passthrough, `-keepalive-max 60`).
  - The two-test sequence fails deterministically at `76d8c28` and passed 5 of 5 on the fix.
  - Same-ID CONNACK, CONNECT sent the instant the DISCONNECT is read: 0.94 s at HEAD, 0.25
    to 0.28 s on the fix. When it is sent 0.25 s later: 0.76 s at HEAD, about 0.05 s on
    the fix.
  - Width was chosen from a plateau: 0.91 / 0.35 / 0.25 / 0.25 / 0.28 / 0.26 s at widths
    1 / 8 / 16 / 32 / 64 / 128.

  The margin under Paho's 0.3 s is thin when the CONNECT follows the DISCONNECT immediately.
  The suite's process start adds incidental slack, but a slower runner may still fail, so
  the first CI run on a runner is the confirmation still owed.
  Tests: `TestMQTTWalkBounded*`, `TestQoS2DiscardArbitrationUsesTheBoundedWalk` and
  `TestQoS2DiscardWalkUnderReleasingClaims` (`internal/mqtt/qos2_discard_walk_test.go`).
  Eight mutations (serial, unbounded, no admission stop, error dropped, join dropped,
  first-published error, slot freed before publication, serial wiring), each killed.
  **`test_subscribe_options`: FIXED (2026-10-15).** A stall, not a loss. Every PUBACK of a
  retained QoS 1 delivery installed an ordinary terminal-source owner whose source
  coordinates were EMPTY (the retained arm of the register has none). The terminal attempt
  ended the exchange correctly, but `finishTerminalSource` then classified the owner's
  source through a record SYNTHESIZED from `(dur, seq)`, which carries no payload or digest,
  so the shape-only retained guard let it through to `CONSUMER.INFO` on `$MQTT_msgs.` with
  an empty name. That request never got a reply and waited the full 5 s JetStream API
  timeout (measured), the owner never settled, its retry re-armed for the session's life,
  and its identifier stayed held. Each attempt held foreground authority, which retained
  replay, a background writer, waits out. So a later SUBSCRIBE or re-SUBSCRIBE replayed
  some of the retained messages or none until the attempts paused. The copies stayed in the
  store: one probe run received its backlog late. The late acknowledgement of a retained
  ABANDONMENT reached the same owner by a second site.
  - Bisect, with both endpoints verified on the rig: `3bb3b45` passes 2 of 2, and the first
    bad commit is `d5913f7` ("F-105 7c: retained QoS 1/2 through the manifest").
  - Two earlier leads were wrong. Auth passthrough is NOT the distinguishing configuration:
    a probe reproduces it with `-auth-passthrough=false`. The logged `Permissions Violation
    for Publish to "$JS.API.CONSUMER.INFO.$MQTT_msgs."` is the server refusing a publish to
    an invalid subject (empty last token), not an ACL decision, and its cost is the
    unanswered request. The Go suites missed it because their test server, which has no
    permissions, answers that lookup in under a millisecond (an error, so Unknown, measured).
    The stray owner leaked there too, but cost no foreground time, no test asserted the
    terminal-source state after a retained acknowledgement, and the zero-count sink
    assertions never saw a synthesized record.
  - The fix. The ACK handler installs a source owner only for a slot that names a source
    (`mqttInflightSlotRef.namesSource`), at both sites. Every ordinary-source sink refuses,
    and counts, anything that names no source: a retained-shape record, or empty
    coordinates in any shape (`mqttRefuseSourcelessRecordAtOrdinarySink`, and the
    coordinate form at `holdTerminalSourceLocked`). See ARCHITECTURE "Retained delivery
    through the manifest (F-105 7c)" and CONFORMANCE "Retained replay after acknowledged
    retained QoS 1". No residual.
  - Measured on the same rig. A probe of repeated `+/+` SUBSCRIBEs on one session delivered
    3, 3, 3, 1, 0, ... retained frames at `210c33e`, and 3 of 3 in 8 of 8 rounds on the fix.
    The test alone passed 5 of 5 on the fix (0 of 3 at HEAD).
  - The cascade was this defect too. The next tests' setup CONNECT (`test_unsubscribe`,
    `test_user_properties`) timed out at 0.3 s behind the stuck attempts. The earlier
    "pre-existing, HEAD 3 of 6, the fix 2 of 6" measurement used a teardown that waited only
    1 s for the broker, and a HEAD broker still draining its attempts outlived it. With a
    teardown that waits for both processes to exit, six interleaved rounds of the three-test
    sequence: HEAD failed all three tests in every round, and the fix passed all three in
    every round. Under the old teardown, one fix round's first CONNECT was refused because
    its outbound authority read as corrupt on a fresh store. It was not reproduced in 50
    first-CONNECT trials on fresh rigs or in the 12 later rounds, and it is ATTRIBUTED (on
    timestamps alone, not proven) to the previous HEAD broker still writing as the next
    rig's NATS server started.
  - Tests: `TestF105RetainedPubAckInstallsNoSourceOwner`, an extended
    `TestF105RetainedReplayTooLargeIsAbandonedQoS1`, and
    `TestF105SourcelessCoordinatesAreRefusedAtEveryOrdinarySink`. The two behavioural tests
    answer every source classification Unknown, so a quick server answer cannot mask a
    stray owner. Mutations, each killed: the original code (T1 and T2 fail on the leaked
    owner, retry and identifier hold, T3 at every record sink); each of the two routing
    sites removed (the counter); the hold's entry guard removed (T3); the predicate reverted
    to shape-only (T3 at five record sinks); and each routing site with the guard removed
    (the leaked owner). T1's first form passed on the original code, because it looked
    before the PUBACK had been processed. It now waits for the exchange's ending first.

  Full `SUITES=paho` on the fix: 27 tests; the only failure was the baselined
  `test_subscribe_identifiers` (`run.sh`: no regressions). At `76d8c28`: `test_keepalive`,
  `test_subscribe_identifiers` and `test_subscribe_options`.

- [x] **MOVED to the verification ledger (2026-10-12 convergence triage)** as a
  deterministic fixture-reach failure.
  **A displaced-writer wire test fails deterministically at `c1b37b0`, pre-existing:**
  `TestMQTTExclusionReclaimWireSafetyUnderADisplacedWriterCreate`, its
  `a_displaced_fresh_allocation_create` arm. Filed 2026-10-12 by F-119's verification, not
  investigated there (3 of 3 runs on a clean HEAD worktree fail identically): no allocation
  is observed against the contested subject within 5 s, at the test's own fixture-reach
  check. Its observation seam (`testInflightStore`) watches the standalone register CAS,
  which ordinary admission no longer takes since F-105 converted it to the authority batch,
  so the first question is whether the fixture still reaches its route.

- [x] **Three more deterministic pre-existing failures at `c1b37b0`**, filed by F-119's
  verification. **Split 2026-10-12 (convergence triage):** the nil-map teardown panic, the
  `mqttRetainedCacheTTL` harness data race with its leaked goroutine, and the isolated
  `TestMQTTShutdownV5SendsDisconnect` failure moved to the verification ledger.
  **CLASSIFIED 2026-10-16, all six remaining assertion failures**, each measured at `b5ec59b`
  (tagged, isolated) and traced to a mechanism:
  - `TestMQTTRestoreAmbiguousSettlesOnTheResidentObject` (3 of 3): a STALE FIXTURE, bisected
    to `cd731a1` (F-105 cutover). The accepting CONNECT's save no longer reaches the
    store-result seam, so the boundary save the fixture loses is write 1, not 2; its sibling
    T8 had been retargeted at the cutover and T8b was missed. Retargeted, and it now proves
    the lost write is the boundary's (the restore's drain is staged when it is lost).
  - `TestMQTTReconcileForeignWriteDuringPhase` (3 of 3): a STALE ROUTE, bisected to
    `00b35a4` (F-105's ownership-gated sweep; listed then as owing an "assembled-tree
    reconcile" that never happened). A 10071-disposed repair phase leaves its replacement
    LIVE and undecided, so the orphan sweep correctly withholds it; the next ACCEPTED
    tenure's settlement decides it `tenure-superseded` and the owner retires and deletes
    it. The test now asserts exactly that.
  - `TestMQTTQoS0DowngradeRollbackReleasesAPacketCreatedDurable` (5 of 15): an UNPINNED
    FIXTURE with a pre-F-105 oracle ("deleted within 8 s"). It passed only when the
    rollback caught the delivery before admission. When an exchange holds the rolled-back
    consumer, the release decides it `rolled-back` and the owner withholds the delete while
    the hold stands (F-105 7c-lc). Split in two, each pinned: the test now holds the send
    section open through the conversion it exists for and stops at "decided and held"; the
    new `TestMQTTQoS0DowngradeRollbackWithdrawsAnAdmittedUnsentDelivery` pins an admitted
    exchange whose section never opened (a nil-in-production seam,
    `mqttTestDeliveryAdmittedPauseV`) and proves nothing reaches the client and the next
    resume withdraws it (`op-withdraw-unsent`), after which the consumer is retired and
    deleted. The section-open arm, once its intent lands, is possibly attempted and is NOT
    retransmitted on resume: stated in `docs/CONFORMANCE.md` under F-098's "A
    possibly-transmitted PUBLISH without a deliverable source holds its identifier".
  - `TestMQTTReconcileChargeAccounting` (`a_failed_repair...`, 6 of 30) and
    `TestMQTTv5RetainHandling` (`RH=1...`, 1 of 15; 4-9 of 40 looped): a BROKER defect, now
    FIXED. A live QoS 1 delivery's send-section acquisition (`acquirePublishAction`) lost
    all three of its authority CAS attempts to this broker's own background writers (the
    retained reclaim chain after a retained replay; the source retirement owner and the
    tenure settlement after a resume) and waited out AckWait — latency, not loss. It now
    announces itself as a foreground authority operation (the retained driver's own
    acquisition excepted) and its budget is one more than the five background writers
    (`mqttForegroundAcquireAttempts`), spaced by the shared contention wait. A source census
    pins the writer count and each writer's single-flight launch guard. Both tests 30 of 30;
    the looped reproduction 0 of 40. A loop of F-114's unspaced bounded re-attempt family,
    classified and converted (the §2 item).
  - `TestMQTTRestoreQoS0OverLiveConsumerCompletesQoS1Exchange`: NOT REPRODUCED at `b5ec59b`
    (0 of 18), which is not "resolved"; moved to the verification ledger with its original
    observation.

- [x] **Three broker defects the ledger reds traced** (filed 2026-10-16 by the
  verification ledger's red-test pass; FIXED 2026-10-16). None lost or duplicated an
  application message. Each mechanism was traced with timestamps under `-race` before
  anything was planned, and the original witnesses were measured interleaved against
  `c247593`, ten runs per arm under `-race` (HEAD -> fix): `TestMQTTQoS2RetriesPublish`
  9/10 -> 0/10, `TestMQTTDrainBarrierFailsClosed` 5/10 -> 0/10,
  `TestMQTTRestoreQoS0OverLiveConsumerCompletesQoS1Exchange` 9/10 -> 0/10, and the
  re-measured `TestMQTTDrainClusterDefiniteIntentFailureResumesElsewhere` 2/10 -> 0/10 (it
  reproduces in isolation too, not only under suite load). No test budget was widened. Met
  by this item's verification and filed in the verification ledger, not here: a pre-existing,
  deterministic hang of `TestF112AHealthySessionAdvancesItsFloorWithoutAReconnect`, a
  fixture race in `TestF105RetainedAbandonedArrowRefusedAfterDiscard` and two unmatched
  singles.
  - **PUBREL after PUBCOMP.** The client's PUBCOMP converts the entry into its terminal
    reservation and then spends round trips (~185 ms under `-race`) before the slot leaves
    `pubrel_ready`; an AckWait redelivery of the canonical PUBREL in that window was
    admitted on the durable slot, acquired a fresh action, bound over the reservation
    (same nonce) and was written, and its acquisition contended with the terminalization.
    Now a reservation is marked by its one constructor (`pubCompAnswered`), and a stored
    PUBREL of an exchange this broker has decided complete is WITHHELD before admission
    (`mqttDeliverPubRelCb`) and refused at the bind (`bindActionToPubRelLocked`), never
    acknowledged there; and when the PUBCOMP hands the cleanup to this process's own live
    action (a redelivery that acquired before the conversion), it marks that action
    answered before releasing the reservation, so its bind and write entry refuse it
    (holistic review round 1). This reverses F-105's "AckWait can re-admit the PUBREL
    after an unrecorded PUBCOMP" clause (`30eb5bc`); the three completion-worker tests
    that pinned it were rewritten to the new contract by consult (`docs/ARCHITECTURE.md`,
    "A PUBREL exchange decided complete is not admitted again"). A PUBREL whose write had
    already entered still reaches the wire (`docs/CONFORMANCE.md`).
  - **A send hold outlives its completed exchange.** Same trigger: the post-PUBCOMP
    redelivery acquired, the client answered it with a second PUBCOMP, and the retained
    terminal cleanup owner (enrolled once, from the first PUBCOMP's reference) carried the
    pre-acquisition transmission counter, so every attempt was refused by the gated reap's
    C-I counter check and the slot and its identifier exclusions stood forever. Terminal
    cleanup now acts on the observation it reads, counter included (`adoptObservation`),
    at the terminalization's loop head and exhausted re-read, the retained owner, the
    drain's token-gone branch and recovery's lost-response readback; C-I still holds
    (`docs/ARCHITECTURE.md`, "A terminal cleanup acts on the observation it reads").
  - **PUBACK did not release a restored drain's claim promptly.** The PUBACK's prompt
    terminal release made ONE `op-ack-advance`, lost the authority CAS to the reclaimer
    stepping the resume's source-retirement owner (`op-dispose-tenure-superseded`), and
    waited out the 2 s continuation. `attemptTerminalRegisterRelease` now re-runs a
    contended attempt whole (fresh read, every premise, a landed batch adopted, never
    advanced twice) up to `mqttForegroundAcquireAttempts`, spaced by the contention wait;
    exhaustion by writers outside the handshake still falls back to the continuation.

## P. Shared-subscription QoS 2 (post-alpha / maybe someday)

This chapter is the **only active roadmap home** for session-affine shared-subscription
QoS 2. It is intentionally outside the dependency-ordered alpha path. Nothing here is
selected by “next roadmap item” automation, and no item here blocks an alpha release.

### Supported boundary and experimental gate

The supported alpha feature is shared subscriptions with a maximum granted QoS of 1.
Requesting QoS 2 on a shared filter succeeds at granted QoS 1; a QoS 2 publication may
therefore enter the broker at QoS 2 and be delivered to that shared subscription at QoS 1.
Non-shared QoS 2 is unaffected. MQTT 5 continues to advertise shared subscriptions as
available.

The existing affine implementation remains in the repository as experimental work. Its
current gate is a programmatic test option which the production command does not expose.
The production default therefore does not create new affine shared-QoS-2 state. The
supported and experimental contracts are deliberately asymmetric:

- entering experimental mode opts the entire MQTT persistence bundle out of compatible
  upgrade, rollback, backup/restore and downgrade guarantees;
- turning the gate off with experimental state present is **unsupported**; operators must
  perform the documented all-brokers-stopped reset of all ten `$MQTT_*` streams first;
- detection, migration, reinterpretation and cleanup of incompatible experimental state
are
  not supported or required;
- there is no automatic conversion from a persisted shared QoS 2 membership to QoS 1, and
  no migration or legacy reader is owed for experimental state.

This cost is acceptable only for an explicit experiment: the ten streams are one recovery
unit, so resetting experimental state also resets ordinary sessions, accepted messages,
retained messages and pending Wills in that account.

### Preservation without an alpha maintenance tax

Do not delete the affine implementation, its dossiers, or its tests merely because it is
parked. Preserve them as the starting point for a future investment decision. Preservation
does **not** make them a compatibility or regression gate:

- alpha work may change general session, QoS 2, PUBREL, in-flight, teardown or stream code
  without preserving the experimental affine behavior;
- a sound supported-path fix must not be rejected or enlarged because an affine test
fails;
- the default build and supported test suites must remain sound, but the experimental
suite
  is quarantined behind `mqttport_experimental_shared_qos2` and its CI job is non-gating;
- do not repair an affine-only regression opportunistically. Append it to the single
  completion item below, with the responsible general change and failing test recorded.

Historical closed findings remain closed as evidence about the implementation that exists;
they do not impose a promise that later alpha work preserve that implementation unchanged.

### Fallout from F-092's admission half, filed here by the supported-scope rule

F-092's sixth attempt (closed 2026-09-04) left **two experimental affine failures**, measured
against HEAD `77c2168` in a separate worktree rather than remembered: HEAD's baseline is four
failures in `make experimental-shared-qos2-tests` and the tree's is six. The two new
ones are affine-only, so by the 2026-09-02 supported-scope rule they are appended
here rather than widening that item.

- **`TestQ2AffForgedRegisterCannotCreditAnotherClaim`.** The affine disposer cannot
  delete a slot in `pubrel_ready`, because the phase gate refuses
  `mqttSlotDeleteComplete` from every past-PUBREC state — deliberately, since a ready slot
    is the identifier's live durable
  claimant. The disposer needs the same **advance-then-delete** treatment recovery got:
  a proof that the exchange is finished licenses an advance to `pubrel_terminal`, and
  the advance licenses the delete. Neither step is sound alone.
- **`TestQ2AffDuplicatePubRelCallbackDischargesBeforeAcking`.** An affine duplicate
  whose slot is absent is no longer acknowledged, so it redelivers forever. The
  supported-path stance is now FAIL-CLOSED — §8.1's absent-slot disposal was refuted, so
  a record carrying a valid SOURCE CORRELATION with a definitely-absent slot is REFUSED,
  not acknowledged (an absent slot cannot prove the exchange is over; a register can be
  released by the drain path while the exchange is still in `publish`). It is genuine
  residue that may occupy `$MQTT_out` until a complete terminal scan or operator removal
  (R8). An affine duplicate carries an affine reference and no source correlation, so it
  is likewise refused; a retained record is slot-less by construction and must stay
  refused and unacknowledged (R6).

Both are consequences of the phase gate and the admission scope working as specified on the
supported paths, not of an affine defect newly introduced. Neither is an alpha blocker.

### Fallout from F-098's resident-reaper authority enforcement, filed here by the same rule

F-098's attempt 3 (2026-09-13) leaves **twelve new experimental affine failures**, measured
against a clean HEAD `00f2ab8` built in a separate `git worktree` rather than remembered.
Both arms ran the same prebuilt test binary, the same `TestOrphanSweep` filter and the same
`-test.parallel 2`; the tree's twelve reproduce identically at `-test.parallel 1`, so they
are not a parallelism artefact.

**The cause, and it is the general change working as specified.** The affine disposer's
RESIDENT arm still asks for `mqttSlotDeleteComplete` through the consolidated gated door.
C-IV requires proven-unsent authority for that disposition, and arm 4 excludes a `Retired`
record unconditionally — retirement is backed by the client's own PUBREC, so
non-transmission is false by construction. Every register this disposer handles is retired
by the sweep's own predicate. Precisely: the reaper still reports success for an absent,
replaced or moved observation; what it can no longer do is **delete the unchanged retired
occupant** these fixtures ask it to delete. The NON-RESIDENT arm is unaffected — its guards
are the same three HEAD applied — so only the resident half regressed.

**What that costs, disclosed rather than minimised**: F-064's disposal of a settled affine
triple no longer happens for a RESIDENT session, so the durable register and the in-memory
resident retirement both accumulate, one pair per exchange, exactly the growth F-064 closed.

**Affine-only by REACHABILITY, not by test tag** (consult D-9). The sweep's predicate
requires retirement and both affine references; supported subscription admission lowers a
shared QoS 2 request to QoS 1 before provisioning, and `cmd/mqtt-nats-broker/serve.go:44`
fixes the experimental gate off. A defect exposed by experimental tests would still block
if it touched supported states; this one does not. F-098 therefore stays **closed over the
supported boundary**, with that qualification load-bearing.

Per this section's own rule the failures are recorded, not repaired opportunistically:

    TestOrphanSweepCheckedReapTreatsAnAbsentClaimAsReaped
    TestOrphanSweepCheckedReapTreatsAnAlreadyGoneRecordAsReaped
    TestOrphanSweepCollectsARegisterWhoseClaimIsAlreadyReaped
    TestOrphanSweepCollectsARegisterWhoseClaimIsLegitimatelyRetained
    TestOrphanSweepCollectsARetiredAffineRegisterWhoseCopyIsGone
    TestOrphanSweepCollectsAnOldRegisterWhoseClaimWasSucceeded
    TestOrphanSweepRegisterArmChargesTheBudget
    TestOrphanSweepRegisterArmChargesTheBudgetForANoOpWrite
    TestOrphanSweepRegisterArmDeletesTheRegisterAfterASuccessfulReap
    TestOrphanSweepRegisterArmsNeedNoQuarantineObservation
    TestOrphanSweepSettlementForgetsTheResidentRetirement
    TestOrphanSweepSettlesARetiredAffineRegisterAgainstALiveCopy

They are kept AS THEY ARE, deliberately: each one is now evidence of the lost F-064 disposal
behaviour, and rewriting them to the new outcome would erase the only record of what was
given up. Note also that HEAD's own baseline failure in this filter,
`TestOrphanSweepPubRelSurvivesAnUnreadableAffineInventory`, is **absent** from the tree's
run — unexplained, recorded as such, and NOT claimed as a repair.

- [ ] **F-090: the orphan sweep strands a void verdict held only by a non-resident
  session's record.** Filed 2026-08-30 by F-087's holistic review (round 5) into §0.3, and
  moved here 2026-09-06 unimplemented: the stranding branch is
  `disposeSettledAffineRegister`'s non-resident arm, at its
  `deleteInflightRegister` call (`internal/mqtt/orphan_qos2_sweep.go`), whose only
  production route requires `mqttQ2AffSettleableRegister` and therefore a register carrying
  both `Q2AffClaim` and `Q2AffCopy`. The supported default never creates that pair
  (`ExperimentalSharedQoS2` false at `cmd/mqtt-nats-broker/serve.go:44`; the
  `f.shared && f.qos == 2 && !c.mqtt.experimentalSharedQoS2` arm in `mqtt.go`
  lowers a shared QoS 2 request to QoS 1 before provisioning), so the population is
  experimental-only. The sweep's other register delete (`:1848`) cannot strand a verdict:
  it requires a proven generation retirement, and by then the record carrying
  `VoidedRegs` is already gone. F-087 gave the affine sweep a retention rule keyed on
  what the REGISTER carries; a verdict living only in a non-resident session's persisted
  `VoidedRegs` is invisible to a walk that reads registers rather than records, so such
  a register is still deleted and its verdict is retired only at generation retirement —
  one nonce per occurrence, **cumulative over a generation**, which is NOT a claim that the
  aggregate is bounded. A corrected design (a verdict set carried on the sweep's existing
  account-wide record walk, plus a budgeted act-time reread that fails closed to retain) is
  drafted at `~/.claude/lanes/f-090/plan.md` revision 2, reviewed once; it is a
  **mitigation, not a closure**, and its residual is stated there.

- [ ] **F-096: the affine owner's normal create can inherit a stream-imposed inactivity
  timer without noticing.** Filed 2026-09-09 by the v2.15.0-RC.1 dependency audit's
  holistic review (round 5). **Pre-existing — it is equally true at v2.15.0-preview.1 —
  and affine-only, which is why it is filed here rather than in §0.3:** both call sites of
  `ensureQ2AffineConsumerSubscription` are affine-gated (`if f.shared` at SUBSCRIBE,
  `if q2AffGen != _EMPTY_` on restore, `internal/mqtt/mqtt.go`).
  `validateOutStream` refuses a nonzero `$MQTT_out` `ConsumerLimits.InactiveThreshold` at
  startup, so the exposure is a stream mutated **after** a manager initialized — the case
  `docs/CONFORMANCE.md`'s "Startup validation is a snapshot" already owns. On that path
  `ensureQ2AffineConsumerSubscription` creates with `buildQ2AffineConsumerConfig`, whose
  `InactiveThreshold` is deliberately zero, and then validates only the returned
  **identity names** through `mqttValidateQ2AffineNames`
  (`internal/mqtt/qos2_shared_affine.go`). JetStream substitutes the stream's limit into a
  consumer that requests zero, so the durable is created reapable and the owner never
  reads the threshold back — arming deletion of the affine durable together with its
  interest-retained backlog.
  **The guard that exists is in the wrong place, which is the whole finding.** The
  identical hazard is already reasoned about, and refused, inside
  `recoverQ2AffineStorageConflict` — but that function is entered *only after a create has
  already failed*, and its own comment names `validateOutStream`'s startup refusal as the
  reason the live case "can only be a stream mutated afterwards". The normal create path
  reaches the same server behavior with no such check.
  **Do not fix it by copying the recovery's read into the create path without deciding
  what a positive answer does.** The recovery path may decline because it is allowed to
  fail inertly and preserve its caller's original error; the owner's create has no such
  licence, and refusing there turns a stream mutation into a refused resume. Whether the
  right answer is a refusal, a threshold read-back that fails closed, or a `$MQTT_out`
  runtime guard mirroring the shared-subscription one on `$MQTT_msgs`, is the decision
  this item owes. `docs/CONFORMANCE.md`'s runtime-guard list is corrected meanwhile to
  stop implying this path is covered.

### One gathered completion item

- [ ] **Promote shared-subscription QoS 2 only after an explicit product investment
  decision.** **POST-ALPHA / MAYBE SOMEDAY; not eligible for automatic selection.** This
  one item owns all changes needed to make the experimental path supportable again. New
  affine-only regressions discovered by unrelated roadmap work are appended here rather
  than filed as alpha blockers or folded into that work.

  The starting inventory is:

  - [F-047](findings/shared-qos2.md#f-047) and
    [F-048](findings/shared-qos2.md#f-048), the transactional SUBSCRIBE-cleanup cluster;
  - [F-049](findings/shared-qos2.md#f-049), unreadable-claim diagnostics and recovery;
  - [F-064](findings/ha-arbitration.md#f-064), including residual 8 and W-c;
  - [F-070](findings/release-investigations.md#f-070) and the two parked affine fixture
    failures in §0.4;
  - [F-071](findings/release-investigations.md#f-071), affine same-topic ordering;
  - the parked F-033 transition/reap census. **`TestSharedDenyDoesNotConsumeForAllowedMembers`
    was REMOVED from this inventory on 2026-09-20: it is not affine.**
    `internal/mqtt/authz_affine_test.go` carries `//go:build mqttport_fulltests` alone, its
    members subscribe at QoS 1 (`:438`) and its publisher publishes at QoS 1 (`:456`) — the
    FILENAME was the only affine thing about it. Its investigation was CLOSED in §0.4 on
    2026-10-14 as a fixture premise (late delivery behind random queue selection, nothing
    lost);
  - **F-105's affine completion work**, relocated here on 2026-09-20 and described under its
    own heading below;
  - every affine-only branch or regression accumulated from F-036, F-072, F-092/F-093, or
    later supported-path work. Their non-affine acceptance remains with their original
    finding; only the experimental delta belongs here;
  - **F-105 cutover 7b, reduced slot-only test coverage.** Removing the shared-contest and
    ordinary custody-transfer tests removed incidental killers for the experimental affine
    contest and transfer arms that remain: displacement and unfenced-resume refusal,
    older-claim takeover, consumed-close handling, claim ordering, once-per-delivery
    transfer and post-transfer acquisition. Censused edges keep verified killers, and the
    existing affine tests are unchanged and green. When this item disposes of the class,
    restore direct slot-only coverage for the surviving arms or delete those arms with the
    mechanism. This is an evidence gap, not a demonstrated behaviour regression.

  Before promotion, re-audit the current code rather than assuming this inventory stayed
  complete. Settle the affine persisted-data model and stream topology first, then declare
  the compatibility boundary that begins with the first release which actually supports
  shared QoS 2. Experimental records from earlier builds remain outside that boundary and
  may require a reset; promotion does not retroactively create a migration promise.

### F-105's affine completion work

**Relocated here from §0 on 2026-09-20**, under the ruling that session-affine
shared-subscription QoS 2 is not an alpha release feature. F-105 itself stays a §0 alpha
blocker: its authority, recovery directory, inventory, resource authority and retirement
machinery are alpha infrastructure, and so is its supported conversion set. What moves is
the affine half alone — the cleanup family the recovery pass executes end to end. Recovery
transfers cleanup for `mqttResourceAffineConsumer` and withholds every other resource kind
(`internal/mqtt/outbound_duty_exec.go:125`), and a closure enrols that resource only
when an affine target exists (`internal/mqtt/outbound_close_enrol.go:108`).

- [ ] **Complete the affine cleanup transfer, under BOTH missing premises.** Neither premise
  holds today and neither is an alpha prerequisite.

  1. **An admission closure that accounts for every previously admitted producer** — F-107,
     relocated to this chapter with it. `beginQ2AffOp` admits against LOCAL session
     state and the claim CAS never contends with the generation's durable closure, so a
     mover paused before that CAS can install a fresh undiscarded claim after the pass's
     final verification read.
  2. **Authoritative enumeration completeness** — **F-108**, an upstream data-layer defect
     tracked in `docs/known-upstream-issues.md` (nats-server#8632). Only the affine
     application of it is this item's; the general freshness, completeness, incarnation and
     boundedness requirements
     are the data layer's to satisfy and await the upstream correction, not broker-side
     work.

  **What is demonstrated today** is execution of the executor over the fixture populations
  the existing restart tests exercise, conditional on both premises. It may not be presented
  as an independently safe production cleanup transfer. The unconditional claim that affine
  recovery is demonstrated was WITHDRAWN and is not reinstated by this relocation.

  **Acceptance this still owes**, preserved so a future attempt does not re-derive it: commit
  closure through the intended path, authorize and perform metadata release while cleanup
  remains outstanding, lose all transient state, then let a restarted broker discover and
  complete the cleanup from persisted evidence alone — with the oracle being the terminal
  duty and the completion certificate, never "the consumer is gone", which a synchronous delete
  that ran anyway satisfies equally. Removing the evidence or the recovery reachability must
  make that witness FAIL.

  **Relocation is not completion.** The unresolved populations stated in
  `docs/CONFORMANCE.md` — a delayed affine create landing after certification, and a
  creation over a CLOSED predecessor — are unchanged by it. (A third, roots that never
  retired while the shared duty was untransferred, is CLOSED by F-105's shared-group
  lifecycle transfer: the duty no longer exists.)

- [ ] **F-068's affine remainder: the raw affine enqueue is not revalidated at the
  write.** Moved here on 2026-10-06 by F-105 WP-F W3 (consult 133 Q2).
  [F-068](findings/runtime-correctness.md#f-068) has no supported-path form. An ordinary,
  shared QoS 1 or retained QoS 1/2 frame re-enters `beginSendLocked` and is written only
  through `writeAuthorizedFrame`, which consumes its acquisition for the live holder on
  the writing connection's own binding; `mqttEnqueuePreparedPublishMsgTo` refuses such a
  frame without its acquisition. An acquired action executing on its original, still-open
  connection after the session retires is permitted by F-105's contract, not a defect.
  What remains is the experimental affine route (`qos2_shared_affine.go`):
  `storeInflightRegister` confirms the register, `beginSendLocked` opens the send section
  from the pending entry, and the frame is then queued RAW (a slot-only frame requires no
  action), so nothing at the write consumes an acquisition or revalidates the generation,
  the binding or the subscription's closure. It closes with the affine route's conversion
  onto the authority. F-068's supported closure is conditional on F-105's integrated gate.

### F-119's affine remainder, filed here by the supported-scope rule

- [ ] **A cancelled experimental affine delivery can still land after its pending fence is
  disposed of.** Filed 2026-10-12 by F-119's closure (lane `~/.claude/lanes/f-119/`,
  `DECISIONS.md` D1), which closed the supported classes with an entitlement-fenced
  authority advance before fence disposal. The slot-only class has no authority to advance:
  its fence is written by `storePendingFenceCAS` and its admission by
  `storeInflightRegister`'s plain CAS, and it opens no custody admission, so neither the
  advance nor the ladder-liveness check applies. **Not claimed fixed, and carried here
  unchanged:** consult C5's two counterexamples to keying disposal on ledger settlement (an
  affine admission paused after its `stillOurs` check while the cancellation fences, so a
  disposer sees an empty ledger and the create-only pair lands after the fence; and an
  ambiguous affine pair that commits after its placeholder was abandoned), P-15 finding 2
  (an ambiguous affine attempt followed by a successful retry), and T36 rows 5 and 6's
  observation — in their construction the late pair was cleaned up whole on its standing
  marker and the retry arm was unreachable, one construction's observation and not a
  guarantee. Stated in `docs/CONFORMANCE.md` ("A cancelled experimental affine delivery can
  still be served after its pending fence is disposed of"). It closes with the affine route's
  conversion onto the authority, like F-068's remainder above.

### F-107: affine claim admission is fenced only in memory

**Relocated here, UNRESOLVED, from §0 on 2026-09-20**, under the user's ruling that
session-affine shared-subscription QoS 2 is not an alpha release feature, and Codex consult
`task-mu9kpvx7-knm9dr`. **Relocation is not completion.** The defect below stands, both
orderings of admission against closure are unresolved, and the bullet is reproduced verbatim
except for the single sentence that used to call it a prerequisite of F-105's cutover
step 2.

Why it is not an alpha prerequisite: the premise F-107 supplies is consumed only by the
affine half of F-105's recovery pass. Recovery transfers cleanup for
`mqttResourceAffineConsumer` alone (`internal/mqtt/outbound_duty_exec.go:125`), and a
closure enrols that resource only when an affine target exists
(`internal/mqtt/outbound_close_enrol.go:108`). Production never reaches the affine mover,
because admission downgrades a shared QoS 2 filter to QoS 1 first (`internal/mqtt/mqtt.go:9508`).

- [ ] **F-107: affine claim admission is fenced only in memory, so a paused mover can
  install an undiscarded claim after its generation is closed.** Filed from F-105's holistic
  round 5 and ruled a separate item by its consult 7. `beginQ2AffOp`
  (`qos2_shared_affine.go`) admits a shared QoS 2 move against LOCAL session state alone —
  the session mutex and its in-memory `q2AffClosing`, `q2AffReady`, `q2AffGen` and
  `q2AffConsumer` — and `moveStartedSharedQoS2` then creates the assignment claim with a
  subject-only create-only CAS that never contends with the generation's durable closure.
  `closeQ2AffAndDrain` supplies quiescence only for a teardown that actually runs on the
  owner. The reachable schedule is cross-broker: the old owner is alive with a mover already
  admitted and paused, a successor drives the durable handoff that closes the generation
  remotely, so the old owner's local closing flag is never set, and the mover resumes and
  writes a fresh UNDISCARDED claim.

  **What this invalidates.** F-105's recovery pass asserts readiness on a completed claim
  scan, and that assertion rests on an admission premise that does not hold — a claim can
  arrive after the pass's final verification read. A read of the closure before the CAS
  does
  NOT fix it: `read open → pause → closure and final scan → resume → CAS` defeats an
  authoritative read as surely as a stale one. And the two records live on DIFFERENT
  streams,
  while an atomic JetStream batch is per stream, so this is not a one-read patch.

  **What a fix must supply** is durable arbitration with three outcomes: admission first,
  and recovery has durable, discoverable responsibility for that admitted work including a
  producer paused before claim creation; closure first, and the producer cannot install an
  effective undiscarded assignment afterwards; ambiguity, and it is resolvable without the
  old owner returning. An atomic authority-and-claim transition could supply it; so could
  a
  durable reservation, but only if recovery can resolve the reservation and stop a delayed
  producer acting after that resolution. A local counter or a post-create compensating
  check
  cannot. Every assignment-creation route must be covered, delayed attempts after claim
  reaping included.

  **Not a residual.** A late undiscarded claim disproves readiness. It has NOT been shown
  to
  produce a redistribution witness — contenders ordinarily honour a claim's winner, and
  some
  paths discard and acknowledge the source instead — but neither has it been shown to
  cause
  only harmless accumulation, so it is not admissible in `docs/CONFORMANCE.md`.

  **This is a prerequisite of EXPERIMENTAL AFFINE RECOVERY**, and it ceased to be an
  alpha prerequisite on 2026-09-20: the premise it supplies is consumed only by the
  affine half of F-105's recovery
  pass, and the integrated change needs its own holistic review with a
  deterministic cross-broker paused-mover test covering BOTH orderings of admission against
  closure.

## U. Upstream data-layer defects (tracked, not ours)

These were F-108, F-109 and F-110 - defects in the JetStream data layer, not in this
broker, moved out of section 0 by the boundary ruling of 2026-09-21. They are no longer
carried as roadmap items: the standing record, with the real-server witnesses, the
upstream references and the unblock conditions, is `docs/known-upstream-issues.md`. The
boundary ruling itself is restated there. Each closes when upstream ships a correction and
the pin moves onto it under the ordinary reviewed-bump procedure; none is an alpha blocker.
At the v2.15.1 pin (2026-10-11) the store functions all four cite are byte-identical to
v2.15.0 and every witness still reproduces, so none closed with that bump.

- [x] **Retire the F-109 point-read absence census (repository housekeeping).** The census
  (`internal/mqtt/point_read_absence_*_test.go`, its manifests, and the dossier
  `docs/findings/point-read-absence-census.md`) mapped broker decisions onto an upstream
  answer this broker no longer compensates for, and it is no longer maintained: red at
  `34c50ef` and not updated for F-105 7b's reads (the record and counts are in
  `docs/known-upstream-issues.md`, F-109). Scope: review each check for one that states an
  independently applicable broker contract and keep that one on its own, then delete the
  census tests, manifests and dossier and reconcile every reference to them. This is not a
  broker-repair item and does not reinstate F-109.
  **DONE 2026-10-15.** Of the census's 52 tests, two stated a broker contract rather than
  census bookkeeping: every point read enters through the five `mqttJSA` message-GET methods
  and one decode arm, no second decoder of their replies exists, and no direct-get route
  exists. They survive, without census data, as
  `internal/mqtt/jsa_msgget_transport_test.go`, and they are what keeps the persisted-identity
  census's name-based read enumeration complete. The rest of the census, its manifests,
  evidence and engine, and the dossier are deleted. `census_scan_test.go` keeps only the walk
  those checks and the persisted-identity census use. References in `docs/ARCHITECTURE.md`,
  `docs/CONFORMANCE.md`, `docs/findings/README.md`, `docs/known-upstream-issues.md` and the
  persisted-identity census's comments are reconciled. With its minutes of closure computation
  gone, the untagged `-race` package ran in 529 s on 2026-10-15, inside the 10-minute default
  test timeout it had been exceeding, though with little headroom.

## 1. Settle release decisions

These decisions affect binary behavior, package names, images, charts,
documentation, and release artifacts. Make them before creating those surfaces.

### Product name

- [x] Decide the product name before renaming packages, images, charts,
  workflows, or release artifacts. **Decided 2026-10-17: `mqtt-nats-broker`.**
  The process terminates MQTT and owns broker semantics over JetStream, while
  the old name `mqtt-nats-bridge` suggested broker-to-broker forwarding, which is
  what "bridge" means in MQTT vocabulary. `nats-mqtt-broker` may imply an
  official NATS project and has an existing exact-name GitHub repository. On
  2026-10-17 no exact-name `mqtt-nats-broker` was found on GitHub, Docker Hub or
  pkg.go.dev, and `proxy.golang.org` had nothing for
  `github.com/nberlee/mqtt-nats-broker`; that is dated discoverability evidence,
  not a name reservation, and trademarks and domains were not checked.

- [x] Rename the product to `mqtt-nats-broker` (2026-10-17), ahead of the
  surfaces sections 2, 4 and 5 create, so they are born under the final name:
  - The Go module path `github.com/nberlee/mqtt-nats-broker` and every import,
    `cmd/mqtt-nats-broker`, the binary, the `Makefile` image
    `ghcr.io/nberlee/mqtt-nats-broker`, the `Dockerfile`, CI and release
    workflows, and `.gitignore`.
  - Persisted consumer metadata in `internal/mqtt/ephemeral_identity.go`: the
    keys are `mqtt_nats_broker_owner`, `_purpose`, `_run` and `_schema`, and the
    owner value is `mqtt-nats-broker`. They are renamed, not migrated: there is
    no dual-key acceptance and no `_schema` bump, because the keys themselves
    changed and these per-run ephemeral consumers expire under
    `InactiveThreshold`. Tests now pin the exact literals.
  - Operator configuration: the `-nats-name` default and the flag-set name are
    `mqtt-nats-broker`. **The `MQTTNB_` environment prefix is kept**, read as
    "MQTT NATS Broker"; there is one prefix and no aliases.
  - NOTICE and the copyright headers ("The mqtt-nats-broker Authors"), upstream
    attribution unchanged; README, CONTRIBUTING, `docs/deploy/kubernetes.yaml`
    (names, labels, image, and the container formerly named `bridge`), and
    "the bridge" as the product's name in README, CONTRIBUTING,
    `docs/ARCHITECTURE.md` and `docs/CONFORMANCE.md`.
  - Not renamed: "bridge" as identity language in Go comments, operator-visible
    diagnostics and `test/depproof`, which section 7 tracks, and finished roadmap
    narrative, which records what was true then.

  The repository rename on GitHub (`nberlee/mqtt-nats-bridge` to
  `nberlee/mqtt-nats-broker`) is an outward step for the owner, before the module
  path is pushed.

### Durable enforcement of the session/marker coupling

- [x] Decide whether the `$MQTT_sess` + marker "one logical unit" constraint should be
  durably enforced at startup rather than documented. **Answered by deletion.** The
  stream this item was about, `$MQTT_willcancel`, no longer exists: the permanent
  CONNECT acceptance facts that replaced its watermark live on `$MQTT_sess` ITSELF, so
  there is no second stream to echo an epoch into and no way to recreate the evidence
  without recreating the records. The remaining coupled stream, `$MQTT_teardown`,
  already carries the adopted epoch in STREAM metadata and its validator already refuses
  a session-only recreation at startup — which is exactly the enforcement this item
  proposed, for the one stream where it is still meaningful. (`$MQTT_origin` was the
  second such stream until the write-lineage audit deleted the register; startup now
  refuses an account that still holds it.)
  What is left is not enforceable and is documented instead: a bundle reset discards
  every client's acceptance history along with the records — and, because the reset takes
  `$MQTT_willq` as well, the delayed Wills those facts would have suppressed are
  discarded with it rather than firing (`docs/CONFORMANCE.md`). The FIRE outcome belongs
  to a partial operation that keeps `$MQTT_willq` while losing `$MQTT_sess`, which is
  exactly what the enforcement now refuses. **F-066 has since generalised the
  enforcement this item proposed** from that one pair to all ten broker-owned streams,
  landed 2026-09-01: every stream carries the epoch, and a proper subset is refused by
  an account-wide preflight before anything is provisioned.

### In-flight verdict schema names

- [x] Decide the names of the in-flight verdict surfaces before alpha (split from
  [F-046](findings/inflight-followups.md#f-046)). The persisted field `CancelledRegs` and
  the session set `cancelledRegNonces` held void-register verdicts for TRANSMITTED
  deliveries too, not only cancelled allocations. Renaming them is free only before a
  release: after the alpha a persisted rename needs migration, which the compatibility
  boundary rules out. **Decided and done 2026-10-17: renamed.** The persisted field is
  `mqttPersistedSession.VoidedRegs` with JSON key `voided_regs`, the session set is
  `voidedRegNonces`, and with them `forgetVoidedReg`, `stageVoidVerdictLocked` and
  recovery's `voidedNonces` snapshot, matching the void vocabulary every sibling already
  used. Comments, test messages and the living docs say "void verdict" where they named
  the set or a member of it. "Cancelled" stays where it is the protocol fact: the
  register's cancellation tombstone, `mqttRegCancellation`, `regCancelled` and the
  cancelled-allocation helpers and tests. No migration, no dual-key reader, no schema
  bump: a development record carrying `cancelled_regs` decodes with no verdicts, and
  development streams may need recreating. Finished roadmap narrative and closed finding
  records keep the old names as history.

### Memory-limit strategy

- [x] Decide whether to use `github.com/KimMachineGun/automemlimit`. **Decided and done
  2026-10-17: adopted, pinned at v1.0.0.** Go 1.26 does not derive `GOMEMLIMIT` from
  cgroups (golang/go#75164 is still an open proposal), Kubernetes `resourceFieldRef`
  cannot multiply the memory limit by 0.9, Helm quantity arithmetic would put the policy
  outside the binary, and a hand-rolled reader would carry the mount and hierarchy
  parsing the library already maintains. The binary calls `memlimit.Set` once at startup,
  before TLS or NATS (`cmd/mqtt-nats-broker/memlimit.go`), with an explicit 0.9 ratio and
  the broker's logger, not the blank import (which logs through `slog.Default()` before
  the broker's logger exists). `GOMEMLIMIT` overrides it entirely; `AUTOMEMLIMIT` sets the
  ratio or `off`. Neither gets an `MQTTNB_` form: they are the runtime's and the library's
  variables, not broker aliases. **No container limit** (every level of the hierarchy
  `max` or missing) sets the Go limit explicitly unlimited; **a discovery or read failure**
  is logged and keeps the prior limit, and startup continues. No host-memory fallback, no
  floor, no refresh: an in-place resize applies at the next start. Acceptance as
  decided: cgroup v1/v2 parsing is covered by the library's pinned fixture suite
  (`memlimit/cgroups_test.go` at v1.0.0), not re-tested here; the broker's tests cover its
  own wiring (ratio and truncation, both environment variables, unlimited, unreadable,
  and placement in `run()`), each paired with a mutation that fails it. Cost: a direct
  dependency plus the transitive `github.com/pbnjay/memory`, reachable only through the
  unused `FromSystem`. Mechanism: `docs/ARCHITECTURE.md` "The Go memory limit follows
  the container limit"; limits: `docs/CONFORMANCE.md` "The Go memory limit is a GC
  target, not an OOM guarantee".

## 2. Make the MQTT core maintainable

- [x] Split `internal/mqtt/mqtt.go` along stable responsibilities: protocol
  parsing, connection lifecycle, sessions, subscriptions, retained messages,
  QoS flows, MQTT 5 properties, JetStream persistence, and transports.
  **Done 2026-10-17, move-only.** The 27,856-line file became `mqtt.go` (the
  package vocabulary: wire and stream constants, defaults, errors, `srvMQTT`, the
  per-connection `mqtt`) plus 27 files named by responsibility; the map is
  `docs/ARCHITECTURE.md` "Source layout of the protocol core". Whole declarations
  moved verbatim; const and var groups moved intact; the thirteen upstream
  section banners were replaced by per-file responsibility comments. No function
  was decomposed, renamed or tidied: `mqttProcessConnect` and the other large
  functions are unchanged, and the dead-code and lint findings stay with
  F-052/F-054/F-055.

- [x] Keep behavior unchanged during the split and run the upstream and native
  suites after each extraction. **Done, with a stated exception to the cadence.**
  Behavior preservation is proven mechanically per extraction batch: every one of
  the 557 HEAD declarations, with its carried comment text, appears byte-identical
  in exactly its mapped file; every comment is accounted for; every file's imports
  are a subset of HEAD's with the same bindings; every other production file is
  byte-identical; and no moved initializer has side effects or cross-file order
  dependence (there is no production `init()`). The later comment-only pointer
  edits were proven to leave every production token stream, build constraint
  and compiler directive unchanged. Four batches each ran build and vet under
  both tag sets, the source-guard tests and `make test`. The tagged suite and the
  cluster tests ran once, on the final tree, not after every extraction: a
  verbatim move cannot change what they measure, and the literal cadence would
  have cost days. The source guards that read `mqtt.go` by name now scan every
  production file (`censusProductionFiles`, `censusFuncSource`), and the
  retained-reoffer guard gained a floor, so a future move cannot narrow them.

- [x] Preserve provenance for code derived from NATS Server through NOTICE and
  concise file or package documentation. **Done.** Every split file keeps the NATS
  Authors' Apache-2.0 header verbatim. NOTICE, `internal/mqtt/doc.go`,
  `CONTRIBUTING.md` and `docs/PORTING.md` name the file family.

- [x] Do not preserve file layout merely to keep a line-by-line upstream diff;
  maintainability of this broker is the controlling requirement. **Done.**
  `CONTRIBUTING.md` no longer asks contributors to leave the port untouched for
  diff-ability; upstream fixes are forward-ported by symbol. The lint exclusion
  carried over to the split files names them exactly, and its stated reason is now
  F-055, not diff-ability.

- [x] Introduce a narrow JetStream transport boundary and migrate ordinary
  stream/consumer CRUD, info, publish-ack and double-ack operations incrementally to
  `nats.go/jetstream`; spike ordered consumers for the two observation consumers.
  **Decided 2026-10-17: premise superseded, not adopted.** Reviewed against `nats.go`
  v1.54.0: `mqttJSA` stays the MQTT operation layer, with neither a second transport nor
  an interface whose only implementation is `mqttJSA`. Stream/consumer CRUD and
  information calls, publish acknowledgements and confirmed acknowledgements stay on the
  existing path, because its submission ordering (one FIFO `sendq` shared with no-wait
  mutations), request admission, deadline and shutdown semantics, server-type and error
  contracts, and fault seams are integral to their callers. Message gets and subject
  walks are excluded outright (library Direct Get on `AllowDirect`; unvalidated
  subject paging); atomic batches and the split confirmed acknowledgement keep their
  existing implementations. The ordered-consumer spike is retracted: the library's reset
  lifecycle contradicts the committed per-run ephemeral-consumer mechanism, and the
  session-lineage deletion audit it waited on has already run. Request-count and
  performance questions belong to "Audit every JetStream query" and "Benchmark" below.
  Any future library request on the service connection must reply under a reserved
  prefix and keep per-client authorization; the one that exists, the startup
  `AccountInfo` probe, replies under `$MQTT.inbox.` since 2026-10-20, which closed
  `docs/CONFORMANCE.md` Residual 13 (with the JetStream query audit below). See the
  [platform audit](findings/nats-platform.md#staged-use-of-the-high-level-jetstream-client).

### Dead-code and reachability cleanup

Each checkbox below is intentionally scoped as one reviewable commit. Complete
them in order where a later lint change depends on an earlier cleanup.

- [x] [F-052: Connect MQTT option validation to production
startup](findings/resolved.md#f-052).
  Adapt or retire the upstream validator, and make its tests exercise the real
  standalone startup boundary. **Done 2026-10-17.** One implementation,
  `ValidateOptions`, run first by `Start` and by the command before NATS or the
  health port; the embedded-server checks are deleted. The startup check found
  `-max-conns` silently ignored (written to an unread `MQTTOpts.MaxConn`): it is now
  wired to the accept path's `Options.MaxConn`, the stray field is deleted, and a
  negative value is refused. Also refused now: a negative stream replica count, a
  non-finite or unrepresentable timeout, a TLS pin set (pinning is not
  implemented), and a Response Information value no CONNACK can encode.
  `KeepAliveMaximum` is `int`, so a flag value cannot wrap into range. Tests drive
  `Start` and `run`; the unread option fields are listed in F-054.

- [x] [F-053: Remove independent dead declarations](findings/resolved.md#f-053).
  Delete the unreferenced constants, errors, leaf wrappers, singular shared-holder
  helper, superseded no-sequence message lookup, and the two orphaned JetStream-name
  compatibility helpers found by the final reference scan. No live call site changed.

- [x] [F-053: Remove obsolete upstream integration clusters](findings/resolved.md#f-053).
  Delete the unused configuration-reload, embedded-routing, and retained-permission
  rescan paths with the imports and shim state they strand. The standalone broker
  now carries no dormant embedded-server account/reload mechanism.

- [x] [F-054: Move test-only helper APIs out of production
files](findings/resolved.md#f-054).
  Keep live hook points, but place tagged-test setters and convenience methods in
  the test build where possible. **Done 2026-10-18.** 115 test-only declarations (75
  hook setters, test vocabulary, test-only members of live types) moved verbatim into
  test files placed by the build that uses them; 15 dead declarations deleted; the
  obsolete acknowledgement-binding model, its custody predicates, two `PubAck`
  accessors and the unreachable `recordTx` non-owner token deleted with the tests
  that only exercised them. Ten superseded mechanisms that tests still use as
  fixtures are F-055's, named in the dossier.

- [x] [F-054: Reconcile the constant-state v5 capability](findings/resolved.md#f-054).
  Derive it from supported configuration or collapse its impossible false branch.
  **Done 2026-10-18, collapsed.** `srvMQTT.v5Enabled` was `true` under every
  configuration; the field, its assignment and the false arms in `mqttNoLocalStamp`,
  `mqttStampedOrigin` and `mqttOriginRetained` are deleted, so production behavior is
  unchanged. Comments and `docs/PORTING.md` no longer describe a 3.1.1-only or
  opt-in-v5 server, and the CONNECT refusal no longer names a nonexistent
  configuration key. The `MaxProtocolVersion=4` cap turned out to be selected only by
  tests; its disposition moved to the last F-054 box below.

- [x] [F-054: Isolate the parked WebSocket path](findings/resolved.md#f-054).
  Keep the future transport milestone explicit without carrying an apparently
  selectable non-nil WebSocket path in current production flow.
  **Done 2026-10-18, deleted.** The placeholder `websocket` type, the `client.ws`
  field, `createMQTTClient`'s nil-only `ws` parameter, the constant-false
  `isWebsocket()` and the `ws == nil` exemption that would have skipped the MQTT
  listener's TLS handshake are gone, so production behavior is unchanged. The
  pre-CONNECT HTTP `GET` diagnostic stays, because it names an HTTP client on the MQTT
  listener, but no longer sends the operator to a WebSocket port this broker does
  not have (`errMQTTHTTPRequestOnMQTTListener`), and a default-build test now
  asserts it. The milestone stays explicit in §3 "WebSocket"; the parked reference
  tests and the test-only `mqttConnInfo.ws` refusal are that section's "Replace or
  retire" box.

- [x] [F-054: Isolate upstream-only QoS and protocol-version compatibility
switches](findings/resolved.md#f-054).
  Keep reference-test behavior without presenting the switches as reachable
  production configuration. Also decide the disposition of `MaxProtocolVersion=4`,
  which no flag or environment variable sets: retain it explicitly as test
  compatibility, expose a supported production mode, or remove it together with its
  TTL waiver and the tests and documentation that describe a
  `max_protocol_version: 4` broker.
  **Done 2026-10-18, removed, both families.** "Keep reference-test behavior" was
  superseded by decision: a reference test does not justify carrying a mode no
  deployment can select. `rejectQoS2Pub` and `downgradeQoS2Sub` are deleted with their
  four production arms (QoS 2 PUBLISH refusal, QoS 2 Will refusal and its CONNACK code
  0x10, QoS 2 retained skip, QoS 2 SUBSCRIBE downgrade) and the four upstream tests whose
  only subject they were; the shared-subscription QoS 1 cap is a different policy and
  stays. `MaxProtocolVersion` is deleted with the CONNECT cap, its option check and the
  `AllowMsgTTL` waiver, so `$MQTT_msgs` and `$MQTT_rmsgs` require `AllowMsgTTL` on every
  broker, as every production broker already did. The protocol-level switch (3.1.1 and
  5.0 accepted, anything else refused) is now the one definition of the accepted
  versions. Production behavior is unchanged.

- [x] [F-054: Delete the option fields production never
reads](findings/resolved.md#f-054).
  The inventory in the dossier, which F-052 left to F-054 and none of the four boxes
  above covered: unread `MQTTOpts` and `Options` fields, and `MaxSubs`, copied into a
  `client.msubs` nothing reads. A production-only `unused` pass does not report a
  write-only field, so F-055 does not catch these.
  **Done 2026-10-19; F-054 resolved.** Sixteen fields nothing read or wrote are deleted
  with the types and the uncalled `Options.Clone` they stranded; `Options.Host` and
  `MaxSubs` (with `client.msubs`) were written by production and read by nothing, and
  are deleted too. Test assignments that configured nothing are deleted, including two
  `o.Users` permission blocks no broker ever applied. `StoreDir` and `ServerName`, read
  only by the fulltests harness, moved into a test-side table that
  `TestFulltestsHarnessStoreInputs` pins. `internal/config` wrote none of them and is
  unchanged. `Options` and `MQTTOpts` now hold only what the broker reads. The dossier
  moved to `findings/resolved.md`.

- [x] [F-055: Re-enable reachability lint for `mqtt.go` and the files split from
  it](findings/resolved.md#f-055).
  Replace the file-wide exclusions with default- and full-suite analysis plus
  narrowly documented exceptions. Not done until the ten superseded mechanisms F-054
  left in production (dossier) are deleted and a production-only `unused` audit is
  clean apart from stated exceptions: the test-build lint cannot see them.
  **Done 2026-10-19; F-055 resolved.** No file is excluded from any analyzer: the core,
  F-105-substrate and shim rules are deleted. `make lint-go` (and CI, which only installs
  golangci-lint 2.14.0 and runs it) makes four passes — production only, default tests,
  `mqttport_fulltests`, and the experimental tag — each clean. The exceptions are four
  `//nolint:unused` named zero values, each stating its invariant. The ten mechanisms are
  deleted with their fixtures on the live routes; the two test-only arms are deleted, the
  `persistRecord` one with the two-member acceptance machinery and the record resolver's
  paired fact readback that only it fed. Three batch shapes only those composers wrote
  are now refused at the write boundary: an exchange admitted with no register, an
  exchange advanced with no slot member, and a cleanup completed without its duty. The
  findings the passes reported, including the never-linted tagged test corpus, are fixed in
  place.

- [x] Remove the JetStream request leftovers the query audit found (filed 2026-10-20; see
  its [Follow-ups](findings/nats-platform.md#follow-ups-filed)): the `mqttJSAStreamUpdate`
  decode arm and `JSApiStreamUpdateT` serve only the tagged `updateStream` test helper,
  which must move to a raw request first; `JSApiStreamDeleteResponse` has no use beyond
  its alias; the client-ID hash in `storeSessionMsg`'s and the teardown receipt's reply
  subjects is read by nothing, and `mqttJSAClientIDPos`/`mqttJSAIdTokenPos` have no users.
  **Done 2026-10-20.** The `cidHash` parameter is deleted from the whole request family
  (`newRequestEx` and `newRequestExMultiSubjectsCID` fold into `newRequest` and
  `newRequestExMultiSubjects`; `newAtomicBatchRequest` takes no hash), which also removes
  it from a third caller the audit had not named, the accepted-CONNECT batch. Every JS API
  reply is `$MQTT.JSA.<node id>.<kind>.<nuid>`. The `SU` kind and its decode arm are gone;
  `updateStream` is a raw request on the service connection that keeps the `-js-domain`
  prefix, with a tagged witness on the exact domain-qualified subject. "Remove
  `JSApiStreamUpdateT`" meant from production: a raw request still formats that subject, so
  the helpers name `server.*` directly and the six JetStream API aliases with no production
  user leave `shim_natsalias.go`. The `m-accept-split-write` depproof mutation is re-anchored
  to the new call. No production behavior changes.

### Performance and resource audits

- [x] Classify F-114's sixteen unspaced bounded re-attempt loops (moved from §0.3 on
  2026-10-12): `inflight_marker.go` (one), `inflight_register.go` (eight),
  `outbound_disposal.go` (one) and `pubrel_action.go` (six) each re-attempt an authority
  or register write `mqttInflightStoreAttempts` times with no spacing. Classify each as
  foreground or background, authority transition or register CAS, and what its exhaustion
  costs, before converting any; they are not interchangeable retry policies (consult 104).
  No loop is claimed harmless by the move. **Demonstrated consequence (F-112, holistic
  round H4, consult 2):** the ordinary and shared delivery admission ladder
  (`admitThroughAuthority`, three attempts) is foreground since F-112, but four background
  writers (retained driver, continuation, reclaimer, F-112 floor unit) may each hold one
  attempt admitted before its announcement, and three interfering commits exhaust it: the
  delivery is re-offered at AckWait (latency, not loss; stated in `docs/CONFORMANCE.md`).
  This item owns the remedy — a budget above the writer count, spacing, or stronger
  arbitration such as one outstanding background attempt per session (a draft of the
  latter threads a release through about twelve admission-hook sites). **One loop of the
  family classified and converted (2026-10-16, the `c1b37b0` classification item):**
  `acquirePublishAction`'s ordinary-slot acquisition (`inflight_register.go`) is a
  FOREGROUND authority transition whose exhaustion stalled a live delivery until AckWait
  (`TestMQTTv5RetainHandling`, `TestMQTTReconcileChargeAccounting`). It now announces
  itself, its budget is `mqttForegroundAcquireAttempts`
  (`mqttBackgroundAuthorityWriters` + 1, the writer count pinned by
  `foreground_priority_census_test.go`), and its re-attempts take the shared contention
  wait; the retained driver's own acquisition stays background on the old budget. **A
  second classified and converted (2026-10-16, the §0.4 ledger-red defects):** the
  acknowledgement's terminal release (`attemptTerminalRegisterRelease`) made a SINGLE
  `op-ack-advance` attempt, a FOREGROUND authority transition whose loss cost the
  continuation's 2 s delay; it now re-runs its whole attempt up to
  `mqttForegroundAcquireAttempts`, spaced by the contention wait. The other loops stay
  unclassified (`inflight_register.go` alone now has ten re-attempt sites on
  `mqttInflightStoreAttempts`, against the eight counted when this item was filed), and
  the admission ladder's exposure above is unchanged.
  **Done 2026-10-19: classification complete and the admission ladder remedied** (not "the
  unspaced foreground loops remedied"; the remaining conversions are the next box). The
  count was stale: HEAD has 19 loop sites on `mqttInflightStoreAttempts`, not sixteen
  (`clearCertifiedRegister` came later, and `acquirePublishAction`'s retained-driver arm
  kept the old budget), and five background writers, not four. Every loop on either budget
  is classified in one table, `bounded_reattempt_census_test.go` (what its attempt writes,
  whether this session's background writers can exhaust it, what exhaustion costs, and its
  disposition), which pins each listed function's budget references, `for` and `range`
  headers and budget bindings and each ladder entrance's budget argument, so an
  unclassified loop fails. Five are owed below; the rest are left with a stated reason
  (register deletes and CASes, a watermark read, a walk restart, a fence read, the
  retained driver's background acquisition, the cleanup-tier tombstone, the disposal step,
  the drain completion, the chapter-P entrance). The admission ladder's entrance now
  passes `mqttForegroundAcquireAttempts`, and an attempt that reached `commitOp` waits
  before the reread, so the admitted writers alone cannot exhaust an admission to an empty
  identifier; the attempts it spends classifying or cleaning up an occupied one are not
  covered (`docs/CONFORMANCE.md`). A failed replacement no longer carries the occupant's
  QoS into the fresh create that follows it, which reached the commit at QoS 2 against QoS
  1 dependencies before. Measured on the way: an exhausted admission is re-offered at
  once, by the NAK at its custody reservation's release, not at AckWait as the residual
  had said. Tests: `admission_ladder_budget_test.go`, `bounded_reattempt_census_test.go`.
  Mutations killed: the ladder budget back to three, the wait removed, the wait on every
  failure, the QoS reset removed, a second loop in a listed function (on a constant, on a
  local budget, and a range loop, which an observer self-test also pins), the chapter-P
  entrance given the foreground budget. Not killed, argued from the code: the wait moved
  between the reread and the next write.

- [x] Convert or refute the foreground authority loops F-114's classification left
  unspaced (filed 2026-10-19 by consult 1 of the F-114 lane; the rows marked OWED in
  `bounded_reattempt_census_test.go`). Each re-attempts three times without spacing and,
  but for the last, without announcing itself, so this session's own background writers
  can exhaust it without even the pre-admitted bound (`docs/CONFORMANCE.md`, "Foreground
  authority loops outside the handshake's bound"). Source reading only so far: a witness
  must precede accepting a fix, and its absence does not make the path harmless. In order:
  `markInflightRegisterCancelled` and `recordRecoveredPubRec` first (entrances from
  CONNECT, SUBSCRIBE, UNSUBSCRIBE, close and the PUBREL callback; exhaustion excludes or
  quarantines the identifier until a later resume, which under Receive Maximum 1 stalls
  QoS 1/2: establish a reachable contention witness, including that case); then
  `acquirePubRelThroughAuthority`'s foreground arms together with an announcement across
  `admitStoredPubRel`'s redispatch, keeping the retained continuation's background
  admission and the outer redispatch budget unless evidence requires otherwise; then
  `mqttTerminalizeOnPubComp` (announced, three attempts; exhaustion holds the Receive
  Maximum unit, the terminal reservation and the canonical PUBREL acknowledgement for the
  completion worker's 2 s). For each: the announcement's lifetime and every path from it
  to a background admission or awaited background work (an announcement reached from a
  goroutine that then waits in `admitBackgroundAuthority` deadlocks), the retry and
  settlement semantics, the attempt and persistent-abort costs, and paired tests with
  named mutations. Stopping rule: convert the reachable contention cases with
  discriminating evidence, or refute their reachability or consequence; "not reproduced"
  is not "resolved".
  **Done 2026-10-20: all five converted, each with a real-store contention witness that
  fails at the prior code.** The cancellation (`markInflightRegisterCancelled`), the
  recovered PUBREC (`recordRecoveredPubRec`), the PUBREL acquisition's foreground arms
  (`acquirePubRelThroughAuthority`) and the PUBCOMP terminalization announce themselves
  for their whole span, take `mqttForegroundAcquireAttempts`, and wait out the contention
  after an attempt whose batch carried a handle (reached `commitOp`; a refusal before it
  re-attempts unspaced); the withdrawal reports the handle through `withdrawUnsentHandled`
  and `storeCancellationTombstone` returns it beside its error. The cancellation and the
  PUBREL acquisition announce only when their caller passes no admission (an admitting
  caller keeps three attempts, unannounced, or it would wait on itself), and the PUBREL
  callback's redispatch (`admitStoredPubRel`) is announced across its whole loop, its own
  budget kept at three. Announcement lifetimes are function scope except the redispatch;
  no path from any span reaches an admission (audited, and pinned by the admitting-caller
  tests). Witnesses (`foreground_authority_loops_test.go`, a writer that takes the real
  admission and commits a real authority write in the target's presend window,
  `authority_contender_test.go`), each run against the prior code first: the PUBCOMP went
  to the completion worker after three attempts lost to five pre-admitted writers; route
  B's PUBREC lost every attempt and the identifier space went incomplete (`bumpPI`
  allocated nothing); the PUBREL callback emitted nothing within 5 s (six acquisitions
  lost); the PUBREC handler's retained PUBREL was handed to the continuation; and under
  Receive Maximum 1 the teardown ladder's withdrawal (door two attempts, cancellation
  three) left the exchange at Publish with capacity zero. Corrected on the way: route B's
  exhaustion stops allocation session-wide (not one identifier), and the cancellation's
  entrances include the deferred-disconnect discharge, uncertain-write continuations, the
  resident sweep and the ladder's cleanup. Residuals in `docs/CONFORMANCE.md`: the bound
  covers pre-admitted background attempts only (not other brokers, recovery, unadmitted
  writers, foreground siblings, store failures, or attempts spent on a loop's own
  classification and cleanup), a never-landed cancellation exhaustion records no owed
  withdrawal, and the cost is up to six attempts and five waits per call (eighteen and
  fifteen on the PUBREL callback's acquisition path) with background held, and up to six
  ABORTED outcomes per cancellation. Tests also: the budget (B and B+1 commit-stage
  losses), the spacing discriminator, the bound with real pre-admitted commits and a held
  sixth writer, per loop; the redispatch probe between acquisitions; three existing tests
  re-expressed on the new budget so they keep their kills. Mutations killed (35 runs, all
  compiling): per loop the budget back to three, the wait removed, the wait on every
  failure, the announcement removed (and each witness with it), the handled flag forced
  false and true (withdrawal, tombstone), announcing unconditionally (cancellation, PUBREL
  acquisition), the redispatch announcement removed, the PUBCOMP exhaustion re-read
  deleted and a single attempt, route B's exhaustion re-read deleted, and the retained
  driver's retry disabled. Met on the way and carried by the verification ledger, not
  here: `TestMQTTInflightStrandedReservationOverADeletedConsumerIsReleased` (pre-existing,
  5 of 5 at `7ed23d8`, re-measured in its existing entry).

- [x] Page the shared `$MQTT_out` consumer enumeration (moved from §0.4 on 2026-10-12).
  `consumerList` walks the whole `$MQTT_out` population on every orphan-sweep pass, and
  the candidate collection and sort grow with it; per-candidate revalidation is already
  budgeted. Owed: paged or bounded listing, bounded discovery memory, a sort that does not
  need the whole population in hand, and a decision on whether the affine and PUBREL
  classes share one walk. The full history is in the original §0.4 entry.
  **Done 2026-10-20: the `$MQTT_out` enumeration is ELIMINATED, not paged, by discovering
  through bounded ownership-record reads.** Paging was planned first and refused in plan
  review: `CONSUMER.LIST`/`NAMES` offsets are positions in a name-sorted set other brokers
  change between requests, so no cursor over them survives deletions below it, and the
  pinned handlers' covered span is not uniform. The one-walk decision fell out of the code:
  only the PUBREL class consumed the listing (course E removed the affine arm), and its
  licence is the name's RETIRED `own.out` ownership record, so the in-flight walk, which
  already read and skipped every F-105 record, now hands those on (`collectInflightCandidates`,
  `pubRelCandidateFromOwnership`): decoded against the manager's epoch, filed under exactly
  its own name's subject, naming a PUBREL durable (`mqttParsePubRelDurable` now requires the
  prefix); anything else is refused with a warning. The walk stops at
  `mqttPubRelSweepAllowance` (128) names, so a pass costs at most 128 session reads and
  consumer lookups, holds at most 128 candidates and sorts nothing, and the sequence cursor
  never passes a name it did not hand on. With RETIRED as both licence and discovery, the
  arm's quarantine, reference gate (`mqttSessRefs.pubRelDurables`, deleted), `Created`
  fingerprint, ownership re-read and name cursor are deleted (consult 1 of this lane): a
  spent name has no construction window, so a resurrected consumer is deleted on the first
  pass that reaches it. The session-record veto and the config validation stay. Residuals
  in `docs/CONFORMANCE.md`: a rotation is sized by the whole `$MQTT_inflight` population and
  every RETIRED out-name costs a lookup per rotation, and eventual visitation holds only for
  an append rate the pass can serve against both the record budget and the allowance.
  Tests: `orphan_pubrel_ownership_discovery_test.go` (no `$MQTT_out` LIST/NAMES on the
  production pass, six refused ownership shapes asserted at admission, the allowance stop
  and cursor, a retirement behind the cursor) and the PUBREL subtests in
  `orphan_qos2_sweep_test.go`; the four listing-rotation tests are deleted with the
  mechanism. Mutations killed (11, all compiling): the `$MQTT_out` listing re-added, the
  two-sighting quarantine re-added, LIVE names admitted, the body's epoch trusted, the
  subject equality dropped, the parse error ignored, the parser's prefix requirement
  dropped, the allowance stop removed, the record veto removed, the config validation
  removed (two shapes). **The separate change consult 1 owed landed 2026-10-20:**
  `consumerList`, still used by the `$MQTT_msgs` sweep and tests, advanced its offset by
  `len(all)` and stopped on a page with no infos, so a clustered page of timed-out
  consumers ended the walk and an offline one re-read entries; and it returned the
  clustered synthetic offline entry, which has no name. It now advances by the distinct
  names a page accounts for across its infos and `Missing`, continues past an
  all-`Missing` page, drops repeats and does not return a `Missing`-named entry
  (`mqttWalkConsumerList`; `consumer_list_paging_test.go`, six mutations killed). The
  pinned handlers' paging is a new row in `docs/ARCHITECTURE.md`'s JetStream behaviors.

- [x] Audit every JetStream query and request pattern inherited from the early
  NATS Server MQTT implementation against current JetStream APIs and behavior.
  Look for avoidable request/reply round trips, repeated stream or consumer
  lookups, serial operations that can be batched or run concurrently, full
  scans where direct lookups are available, and polling that can be replaced by
  push or event-driven behavior. Keep the leader-routed management GET for
  correctness-sensitive loads: Direct multi/batch Get lacks read-after-write
  coherence. The [platform audit](findings/nats-platform.md#deliberate-non-adoptions)
  records this and the pinned-consumer, KV/counter, pause/reset, and fast-batch
  non-adoptions so the audit starts from decisions rather than rediscovering them.
  **Done 2026-10-20 as a record, not a rewrite** (consult 1 of the lane, option 1): the
  inventory and one disposition per pattern are the
  [audit section](findings/nats-platform.md#inherited-jetstream-request-patterns-audit-2026-10-20)
  — kept with the quoted reason, candidate for the Benchmark item with its path and a
  STATIC request estimate, or follow-up. No query path changed: the Benchmark item gates
  that on measurement, and several repeated reads are distinct validation moments. Landed
  with it: **`docs/CONFORMANCE.md` Residual 13 closed** (user scope addition) — the
  service connection's library replies use `CustomInboxPrefix("$MQTT.inbox")`, so the
  startup `AccountInfo` probe no longer answers on `_INBOX` where an MQTT `#` subscriber
  could read it; the probe stays, as the only JetStream check before the listener opens
  (`internal/backend`, two tests, both mutations killed); and the stream count corrected
  to ten in operator errors (now naming no number), comments, tests and docs.

- [x] Settle the JetStream API error contract on no responders (filed 2026-10-20 by the
  audit above). `processSub` frames a status message's headers and `processJSAPIReplies`
  JSON-decodes the framed message, so a header-only 503 would surface as an invalid-JSON
  error rather than "JetStream not available". Established by source; the trigger (a
  JetStream-disabled account behind a startup probe that passed) and its production
  reachability are unverified: reproduce first, then fix or refute. **Reproduced and fixed
  2026-10-20.** On a real server every JSON kind surfaced a 503 as invalid JSON (10025), and
  the reproduction found a sharper defect the filing had not named: the double-ack arm read
  ANY reply as a confirmation, so a confirmed acknowledgement sent where no consumer leader
  holds the ack subscription (deleted consumer witnessed; a clustered leader change can do
  the same) returned success, and callers such as `settleRetiredSource` would drop a retired
  QoS 2 source's suppression on that false proof (by source reading; the witness is at the
  JSA). `processJSAPIReplies` now classifies a
  `Status` header in the reply's own header block once, before any per-kind decode, as an
  `ApiError` with the status as `Code` and no JetStream error code; a double ack fails on
  it. Every broker classifier matches by error code, so a status stays ambiguous, as a
  timeout is; nothing is reclassified as definite. Mechanism in `docs/ARCHITECTURE.md`
  "JetStream behaviors this design depends on"; witnesses in
  `internal/mqtt/jsa_no_responders_test.go`, eleven mutations killed. No residual.

- [x] Evaluate pipelining the inbound QoS 1 (and QoS 2) ack paths, using upstream
  nats-server PRs 8415 and 8416 as prior art. Our inbound QoS 1 path is the
  pre-8415 synchronous shape: `mqttProcessPub` case 1 stores through
  `mqttInitiateMsgDeliveryJSA` and only then sends PUBACK, so throughput is capped
  at one store round trip per connection. Upstream reports 2.7K → 68K msg/s (QoS 1)
  and 690/s → 18.1K/s (QoS 2) single-connection on R3. The aggregate many-connection
  gains differ by PR and must not be conflated: **1.1–1.2x for QoS 1 (8415)** but
  **1.4–1.5x for QoS 2 (8416)**. Note that 8416's figures measure its **whole**
  implementation, including the load-skip and round-trip removal this project
  rejects — **no published number isolates a pipeline-only QoS 2 design**, so
  producing one is part of the evaluation. All of it is **comparison data, not
  this broker's cap or target**, since this port uses real `nats.go` connections
  plus a standalone JSA dispatcher and a retained PUBLISH incurs an extra store.
  Benchmark before changing
  the path, per the bullet below. 8416's dedup/discard protocol is separately
  **rejected**; only its pipelining half is in scope. See the
  [platform audit](findings/nats-platform.md#deferred-prior-art-not-rejected).

  The invariants such a pipeline must preserve, enumerated up front so they are not
  rediscovered one review round at a time:

  - **`jsa.subsBarrier.wait` still precedes every `$MQTT_msgs` store.**
    Interest-based retention means a store racing a SUBSCRIBE is discarded outright.
    Note the scope: the *retained* store runs in `mqttHandlePubRetain`, before the
    barrier, so "every store" would be false. (Corrected 2026-10-20: it said
    `processInboundClientMsg`, which is only the live fan-out.)
  - **Store-submission order, not only PUBACK order.** Same-connection `$MQTT_msgs`
    writes must be *issued* in PUBLISH order so JetStream consumers keep an ordered
    carrier; FIFO acks over out-of-order stores is not sufficient.
  - **One ack rule, and it must say when a position is reserved.** Every
    response-producing inbound PUBLISH or PUBREL **reserves a position in the
    per-connection ack FIFO in readLoop receipt order, before any asynchronous work
    begins**; store completion — or a no-store result — only marks that reserved
    position ready; emission drains ready positions from the head. Without the
    reservation clause, an implementation that enqueues on completion satisfies the
    words and still emits in completion order. Scope: **one FIFO per connection**
    across PUBACK, PUBREC and PUBCOMP; a per-type split needs its own argument about
    client flow control, not silent adoption. Examples that make the rule concrete,
    not its definition: the pre-store rejections written directly today (reserved
    topic 0x87, quota 0x97, payload-format 0x99); permission-denied once F-079 is
    fixed; and QoS 2's own no-store answers — a duplicate held PUBLISH answers PUBREC
    without storing, and a missing record or terminal tombstone answers PUBCOMP
    without an onward store.
  - **Owned queued data — a complete snapshot, not a field list.** `c.mqtt.pp`,
    `c.pa` and their slices are readLoop scratch: the live fan-out
    (`processInboundClientMsg`) reads `c.pa`. `mqttHandlePubRetain` takes its
    publication as an argument (F-060; corrected 2026-10-20, this bullet said it read
    `c.mqtt.pp`) and reads its topic, `c.mqtt.pubOrigin` and the client username.
    Either an immutable publish/retained snapshot, or a fully pre-encoded retained
    operation.
  - **No Local origin and the retained flag captured before handoff.**
    `c.mqtt.pubOrigin` is valid only for the synchronous delivery call.
  - **Message Expiry keeps ageing while queued** — a delayed async store must not
    restart the original PUBLISH's interval.
  - **Wills stay synchronous.** No client PI, no PUBACK obligation, and delayed-Will
    terminalization depends on `deliverWillRecord` returning only after its store
    completes. This is the port-specific reason to carry upstream's
    `hasClientPI` / `owesPubAck` distinction.
  - **Receive Maximum accounting keyed on packet identifiers.** Today's check counts
    only `qos2InFlight` *because* QoS 1 is synchronous. Queued QoS 1 PUBLISHes become
    observable unfinished work, but a count of queued messages is the wrong unit: it
    must track unique outstanding PIs, must not double-count a DUP retransmission
    (mirroring the existing QoS 2 exception), and must define what a *non-DUP* reuse
    of a still-queued PI means. Completion happens on another goroutine, so "the
    check stays on the readLoop" is not by itself a concurrency design.
  - **Teardown ordering.** A worker failure must close the socket so the readLoop
    runs `mqttHandleClosedClient` before `closeConnection`, preserving session detach
    and Will handling; calling `closeConnection` from the ack goroutine bypasses it.
  - **No post-DISCONNECT or post-takeover ack.** Pending work must be cancelled or
    drained when a connection is displaced, or when shutdown consumes its one
    DISCONNECT claim.
  - **Bounded window, and every JSA reply registration released** on timeout,
    pipeline rejection, connection close and shutdown — registrations here are
    account-scoped, not connection-scoped.
  - **The ack worker needs an owner in the shutdown graph.** `clientWG` covers
    readLoops only, and shutdown waits for clients *before* stopping the account/JSA
    infrastructure the worker would still be calling into. Name the wait group, the
    admission gate's closing point, and what proves the worker cannot outlive its JSA.
  - **Store-failure-closes-connection scoped to the `$MQTT_msgs` store**, or
    deliberately coordinated with F-060: a retained store failure is returned joined
    with the `$MQTT_msgs` store's result and closes the connection with a v5 DISCONNECT
    reason, and the onward store is attempted regardless (corrected 2026-10-20: this
    bullet said a retained failure was only logged).
  - **Trace at emission time**, where the ack is written, not where the store is
    submitted.
  - **Concurrent releases of one session** (found by the evaluation). Overlapping
    PUBRELs of one connection must not refuse each other the session record
    transaction: `classifyQoS2ReleaseAgainstRetirement` takes it with
    `tryBeginRecordTx` and answers Unresolved when it is held, which withholds the
    PUBCOMP and closes the connection. The release claim, the hold proof and
    `beginQoS2ReleaseOp` were written for one release at a time per connection and need
    the same audit. Queued work also outlives the packet's incarnation lease, so each
    job needs its own.

  **Decided 2026-10-21 by the user: GO, adaptive per connection** (the implementation item
  follows). The measurement itself stayed unresolved at the one-exchange bound; the user
  accepted the depth gain on the condition that a client with nothing outstanding keeps
  today's direct path, so the cost lands only where the gain does, and declined a
  per-installation switch (one setting would be wrong for a fleet mixing one-at-a-time
  devices and streaming gateways; a switch returns as an explicit decision only if the
  adaptive design cannot meet the bound). The evaluation as measured:

  **Evaluated 2026-10-20: UNRESOLVED.** Record, numbers and method:
  [platform audit](findings/nats-platform.md#inbound-ack-pipelining-evaluation-2026-10-20).
  The pipeline-only QoS 2 number this bullet asked for was produced: an experimental
  pipeline for both QoS levels that keeps every request of the serial path (the request
  census matched per exchange, class by class, in every case), saved as
  `docs/findings/inbound-ack-pipeline-prototype.patch` with its kill tests and mutation
  script. Depth reproduced in two full paired runs: on one connection at a window of 256,
  QoS 1 9-23x and QoS 2 6.5-8.5x, p99 down accordingly; across eight connections 1.6-3.8x.
  One exchange at a time did not: under criteria fixed before measuring (W=1 neither
  throughput nor p99 worse by more than 10%), run 1 was a valid no-go for both QoS levels
  and run 2 a valid go for both, and a pre-registered pinned extension (ten pairs, A76 cores)
  left both unresolved, every point estimate inside the bound but the paired ratios
  straddling it. The W=1 cost lies between an 11% gain and a 13% loss (30% in one run).
  What resolves it is a decision that a cost of that size is acceptable for the depth gain
  (a product choice), a design clearly inside the bound (one that answers on the readLoop
  while nothing is queued is an untested hypothesis), or a measurement with more power on
  quieter hardware; a further extension is not automatic. Committed: the benchmark
  `TestMQTTInboundAckThroughput` (`inbound_ack_throughput_test.go`, `make bench-inbound`,
  opt-in) with a ledger of owed responses closed by a PINGREQ fence, an eventual read-back
  through an interest consumer, and a request census at the broker's own boundary; its
  self-tests `TestMQTTInboundAckBenchOracleDiscriminates` and
  `TestMQTTInboundAckBenchLedgerRejectsExtraResponses`; and two probes in `make full-suite`,
  `TestMQTTInboundAckFollowsDurability` (no PUBACK, PUBREC or successful PUBCOMP enqueued
  before its store, hold CAS or terminal CAS returned) and
  `TestMQTTInboundAckOrderUnderPressure` (issuance and response order). Mutations killed:
  the four oracle predicates and the ledger's order, fence and quiet checks (committed
  self-tests); P6, a dropped store with its PUBACK sent, by the read-back; and, in the
  prototype tree only, P1-P5 and P7 (issuer and emitter taking a non-head job, each response
  marked ready before its durable effect, classification unserialized), each on its named
  assertion. The prototype's exclusions (post-DISCONNECT and takeover suppression,
  registration release on close, shutdown ownership, QoS 1 Receive Maximum) remain required
  work for any implementation.

- [x] Implement the inbound acknowledgement pipeline, adaptive per connection (decided
  2026-10-21 by the evaluation bullet above). Scope and obligations, all of them, so none
  is rediscovered in review:

  - **Adaptive, no switch.** A connection with nothing outstanding handles a PUBLISH or
    PUBREL on today's direct path; it enters the pipeline only when a packet arrives while
    earlier work is still unanswered, and leaves it when the pipeline drains. The switch
    between the two keeps every invariant at the boundary (a direct-path packet never
    overtakes queued work, in issuance or in responses).
  - **Acceptance, measured with `make bench-inbound`:** at one exchange outstanding
    (window 1), neither throughput nor p99 worse than the direct path by more than 10%, on
    both rigs, both QoS levels, under a decision rule fixed before measuring that says
    what two valid but contradictory runs mean and sizes repetitions to the bound (the
    evaluation's first two runs disagreed at this bound on this hardware). Add a
    many-connections case at window 1, the typical device fleet, which the evaluation did
    not measure. At window 256 the evaluation's gain is the reference (one connection
    6.5-23x; eight 1.6-3.8x).
  - **Every invariant in the evaluation bullet above**, all 15, including the three
    premises it corrected and the concurrent-release hazard it found (overlapping PUBRELs
    of one connection must not refuse each other the session record transaction; each
    queued job holds its own incarnation lease).
  - **The prototype's exclusions, as required work:** no response after a DISCONNECT or a
    takeover; every reply registration released on close and shutdown, not only by
    timeout; the pipeline's workers owned by the shutdown sequence, with a proof they
    cannot outlive the JSA; QoS 1 Receive Maximum accounting keyed on packet identifiers
    (DUP and non-DUP reuse defined); an audit of the release claim, the hold proof and
    `beginQoS2ReleaseOp` under overlapping releases.
  - **Tests:** the committed probes `TestMQTTInboundAckFollowsDurability` and
    `TestMQTTInboundAckOrderUnderPressure` keep passing (necessary, not sufficient: they
    observe, they do not force a wrong path); kill tests equivalent to the prototype's
    P1-P5 and P7 written against this implementation's own decision boundaries (the
    prototype's evidence does not transfer; see
    `docs/findings/inbound-ack-pipeline-prototype.patch`), plus kills for the
    direct/pipeline switch and each exclusion above; the bench's read-back and census on
    every measured run; `make cluster-failover-tests`.
  - **Docs:** a `docs/ARCHITECTURE.md` section for the mechanism and `docs/CONFORMANCE.md`
    for any residual.

  **DONE 2026-10-21.** `internal/mqtt/inbound_pipeline.go`; mechanism in
  `docs/ARCHITECTURE.md`, "The inbound acknowledgement pipeline"; residuals in
  `docs/CONFORMANCE.md`, "The inbound acknowledgement pipeline: what a close drops, and
  what is newly enforced". One implementation of each exchange's steps (the hold step, a
  release's fences, the ordered issuance, the completion), run inline by the readLoop when
  the pipe is empty and nothing more is buffered, otherwise queued: one FIFO per
  connection with positions reserved at receipt, one issuer at its cursor (sendq's single
  drainer keeps submissions in order), single-drainer emission from the head, a
  same-identifier chain for QoS 2, incarnation leases taken in FIFO order, a Receive
  Maximum ledger of unanswered and held identifiers (QoS 1 now counted), Message Expiry
  aged through the queue (a new QoS 2 hold included), a connection-lifetime abort latch
  fired by the one transport-close helper (`closeTransport`), a prefix-preserving failure
  policy, drains before every non-publication packet and before an armed rejection gate,
  the readLoop's own error exit (an oversized packet's included) completing the packets
  before it, and a release claimed by the issuer completing through an abort. Wills stay
  synchronous. Process memory only; no persisted shape changes.

  Acceptance (`make bench-inbound`, the frozen W=1 rule: ten balanced pairs per case,
  pinned, ratio of medians, throughput >= 0.90 and p99 <= 1.10): **candidate 1 FAILED**
  one of eight cases (r3, QoS 1, 64 connections: pooled twenty pairs 0.894; an A/A control
  of that case read 0.998, no arm bias); the direct path's per-exchange allocations were
  removed and **candidate 2 PASSED all eight in the first block** (throughput 0.980-1.003,
  p99 0.984-1.022; the failing case 1.003 / 0.999). Depth, descriptive, one pair each at
  W=256: QoS 1 one connection 8.6x (standalone) and 15.5x (r3), eight connections 2.6x and
  3.4x; QoS 2 one connection 4.4x and 4.9x, eight connections 1.4x and 2.1x (pinned, so
  against a faster serial baseline than the evaluation's unpinned depth runs; in absolute
  throughput at or above the prototype, see the record). Record: [platform
  audit](findings/nats-platform.md#inbound-ack-pipelining-evaluation-2026-10-20).

  Tests: the committed probes; `inbound_pipeline_test.go` (T1-T18) and
  `inbound_transport_close_test.go`; `TestMQTTv5ServerReceiveMaximumQoS1NotPoliced`
  replaced by `TestMQTTv5ServerReceiveMaximumQoS1Counted` (QoS 1 overrun is now observable
  and refused, 0x93). Mutations, each killed on a named assertion: issuer cursor, emission
  head, the three early responses, classification serialization, the switch both ways, the
  PINGREQ and acknowledgement drains, emission after a server DISCONNECT, the teardown
  join, both registration releases, the four Receive Maximum ledger faults, expiry from
  issuance (QoS 0/1 and the new hold), the same-identifier chain, both lease placements,
  the failure close (a declared liveness kill), the DISCONNECT drain failure, the abort
  latch, the gate's drain-first, the error-exit drain, both oversized-packet drains, a
  release claimed by the issuer dropped at abort, and a queued job's ownership. Verified
  with `make test`, the tagged suite under `-race` and `make cluster-failover-tests`; reds
  met: `TestMQTTInflightStrandedReservationOverADeletedConsumerIsReleased` and
  `TestF105RecoveryKickIsBoundedAndFair` (recorded, both reproduced at clean `4839059`:
  3/3 and 3/20); `TestRetainedResetCreateResponseLostReconciles` (recorded intermittent;
  0/20 at clean `4839059` this time); and
  `TestF105RetainedCloseLocatesTheRegisterByItsAdmission`, once, in the final chain (not
  reproduced in isolation on either tree, 0/10 each; its rig's only inbound packet is one
  acknowledged publish on the direct path), carried as unattributed in the verification
  ledger (`docs/findings/release-investigations.md`).

- [x] Benchmark the current and proposed JetStream paths under connect,
  subscribe, publish, reconnect, retained-message, and session-recovery loads.
  Record request counts, transferred bytes, latency, and NATS/JetStream load;
  change a query path only when measurements show an improvement without
  weakening MQTT semantics, authorization, consistency, or HA behavior.
  **Done 2026-10-22.** Record:
  [JetStream path benchmark](findings/nats-platform.md#jetstream-path-benchmark-2026-10-22).
  Scope (lane consult 1): measure every load, change each audit candidate that improves and
  has a consistency argument, one disposition per candidate, no new bullets. Gate for overlap
  changes (consult 2, superseding consult 1's): admitting paths unchanged in requests; a
  refusal, error or restart path may issue only read-only requests its admitting round would,
  cost stated per class, and its recurrence on AckWait re-offer accepted.

  Measured (`make bench-jspaths`, standalone and R3, pinned, arms interleaved): a QoS 1
  publish costs about 105 JS API requests end to end, about 98 of them message GETs on
  `$MQTT_inflight` (the audit estimated 8-10 for admission); reconnect 48, connect 20,
  retained replay 192, a session recovery of 12 messages about 1 400-1 600 warm or cold
  (descriptive), a broker start 45, a sweep tick 14 per offline session. The server's
  API counter does not count successful message GETs (nats-server answers them outside
  the API layer), so it is reported raw and is not the request count.

  Changed, each in docs/ARCHITECTURE.md: admission readiness reads the name ownership
  once, with the resource-name, stream and client equalities now explicit refusals
  (`readyForUse`; one fewer read per dependency per attempt, no refusal reading more,
  pinned by `TestReadinessRefusalCosts`); a consumer lookup answered with neither
  information nor an error is indeterminate in `recoverDrains`, not "gone"; the no-wait
  consumer delete takes the `-js-domain` route (reachability in a current-format
  production path not established, so no leak is claimed). Measured and not adopted
  (lane consults 4 to 6, the record has each one's benefit and cost): overlapping the
  readiness, traversal and composition reads (Q2 to Q4: latency only, against refusal
  costs and hot-path complexity), and the resume-reconcile and startup-preflight rounds
  (Q5, Q7: on risk/benefit while F-126 was open). Kept, with the reason in the record:
  the per-delivery STREAM.INFO (the only fresh evidence of a stream inactivity limit a
  later consumer re-add substitutes, verified in v2.15.1), every write-boundary re-read,
  the resume PUBREL walks, the sweep walks, the provisioning loop. Found and not changed:
  almost every QoS 1 delivery's first admission attempt loses its CAS on the source
  consumer's resource record to the previous delivery's hold release and is retried, a
  dependency-recording question outside a query-path item.

  Filed: **F-126**, a recovering session's own admissions and acquisitions can exhaust its
  PUBREC advance and close the connection; reproduced on the baseline (4 of 30 screen
  trials), accepted as a nonblocking residual (`docs/CONFORMANCE.md`, "F-126"; dossier in
  `docs/findings/runtime-correctness.md`); no new bullet.

  Tests: `jsa_request_observer_test.go` (controls), `outbound_admit_readiness_test.go`,
  `outbound_inventory_uncertainty_test.go`, the tagged
  `outbound_inventory_floor_cross_fulltests_test.go` and
  `outbound_op_refusals_fulltests_test.go`, `resume_reconcile_test.go`,
  `TestNoWaitConsumerDeleteTakesDomainRoute`. Mutations on the final change: 31 killed
  (23 on the readiness, traversal and operation refusals, 8 on the resume reconcile,
  domain, preflight and observer), the removed candidates' recorded as not applicable.
  Census, observer, reply hold and the bench: docs/ARCHITECTURE.md, "Measuring the request
  paths". Not resolved: the R3 publish-load time ratios (0.78-1.07 across measurements:
  this hardware's run-to-run spread); session recovery's figures are descriptive. Also
  open in the ledger, and reproduced on the baseline arm: the
  `TestWillClaimantAbortsThePassOnATransientFactOutage` intermittent (4 of 30 each) and
  `TestF105RefusedConnectIsCertifiedAndStaysSilent` (1 of 15 baseline, 0 of 15 change);
  `TestMQTTInflightStrandedReservationOverADeletedConsumerIsReleased` stays red as recorded.

- [x] Design reclamation of the per-CONNECT permanent records (moved from §0.3 on
  2026-10-12, where it was decided as an accepted residual): acceptance facts and handoff
  receipts on `$MQTT_sess`, F-105 transition outcomes on `$MQTT_inflight`, across all
  classes at once, since reclaiming one leaves the growth in place. A sound design must first
  make every reader of these records and every delayed Will not yet in `$MQTT_willq` (the
  retention queue's in-memory entries carry an original origin) durably visible, with
  creation and exit rules; the refuted fact rule in §0.3 shows what fails without that. Not
  an alpha precondition; the exposure is stated in `docs/CONFORMANCE.md`.
  **Reclamation design audit completed (2026-10-23): all three classes are retained under
  the current protocol, and the residual remains.** No reclamation mechanism was designed,
  because the visibility it needs does not exist today. Facts: nothing durable names a
  delayed Will held in the retention queue (the enqueue refuses before any record exists,
  and the persisted session Will is vestigial), and the fact scans read a deleted fact as
  absent. Receipts: a live accept drive can reintroduce its intent ID after the authority
  dropped it, and every receipt writer is create-only, so a late writer re-creates a deleted
  slot, in one direction closing a serving generation. Outcomes: every reader holds an
  in-memory handle whose exit nothing records, and a holder's own abort turns a deleted
  committed outcome into "never landed". Reopening needs a protocol that makes every
  delayed Will durably discoverable, fences every live writer and reader of all three
  classes including already-issued requests, and covers the classes together. No
  implementation item follows. Codex consult decided; the per-class inventory,
  counterexamples and reopening conditions are in
  [the reclamation audit](findings/per-connect-record-reclamation.md).

- [x] Perform a full memory and resource-lifetime scan of the broker. Cover clients,
  sessions, subscriptions, retained-message caches, QoS state, JetStream request state,
  timers, goroutines, channels, buffers, NATS connections/subscriptions, TLS reload state,
  and shutdown paths. **Done 2026-10-23, with the churn/soak bullet below as one item
  (Codex consult).** The census walked every goroutine spawn (52), timer (38), channel
  (37), NATS subscription and connection, server- and account-lifetime container (35) and
  session or connection field (60), plus the process boundary; [the resource-lifetime
  dossier](findings/resource-lifetime.md) keeps it as an evidence index, each row marked
  verified or carried. It found eight defects. Seven are fixed: a stale drain retry left
  its shutdown-registry entry (D2); the retained background loops outlived a stopped
  server (D3); a subscription installed after its connection closed was never removed
  (D4); emptied pending-binding buckets accumulated on persistent sessions, now owned by
  `mqttConsumerPending`, whose census exposure added rows C37d and C37e (D5); the accept
  loop died on a non-timeout error (D6); shutdown and a failed `Start` left subscriptions
  and handlers on the caller's service connection (D7, shutdown Phase 5); the backend's
  close did not wait for its drain (D8). The eighth is a stated residual by Codex consult:
  a session object removed without its reset keeps working while its work is unresolved,
  with a conditional rather than a bounded end (D1, `docs/CONFORMANCE.md`); the one case
  the soak found exceeding it, a displaced disposal waiting for a pair its successor's
  abandonment made impossible, is fixed. The nats.go per-subscription pending buffer is
  stated as well. TLS certificate RELOAD state is not covered because reload does not
  exist yet; it is an obligation of §3's reload bullets, which have since landed with their
  soak (`TestResourceTLSReloadChurn`).

- [x] Add churn and soak tests that repeatedly connect, subscribe, publish,
  disconnect, expire sessions, fail over, and reload TLS. Use heap and goroutine
  profiles plus leak detection to prove memory and resource counts return to a
  stable baseline after work drains; fix every confirmed leak before alpha.
  **Done 2026-10-23; TLS reload churn joined with §3's reload (2026-10-23).**
  `make resource-soak` runs alone:
  `TestResourceChurnSingleBroker` (blocks of 100 churn cycles, including failed and stalled
  TLS handshakes; every container at the value completed work leaves, broker goroutines equal
  the idle set by root, every removed session object collected, broker-allocated heap and
  descriptors without sustained growth), `TestResourceChurnFailover` (takeovers between two
  brokers on a three-node cluster with a stream-leader restart and the floor driven past its
  slack; every displaced object collected once its work completed),
  `TestResourceDisplacedDisposalCollected`, and `TestResourceShutdownComplete`. The
  baseline is the EXPECTED value per resource, not a warm-up count, which would accept a fixed
  leak. Process heap is logged, not asserted: it grows with the embedded nats-server's
  file-store caches and subject index as records accumulate, which the heap profiles place
  entirely outside broker code. Each fix has a focused test paired with a mutation that was
  run and fails it.

### NATS platform follow-ups

- [x] Enable `js_ack_fc_v2` in clustered CI and prove the broker's dual-layout ACK
  parser and ACLs accept the domain/account-aware subjects. This is a compatibility test,
  not a production feature toggle. The flag is off at the v2.15.1 pin; upstream's `main`
  states it becomes the default in **2.16.0** (the "v2.15 default" this bullet first
  carried was stale). Done: every node of the replicated test harness runs it, so the
  clustered tests CI runs (the Will-outbox replicated tests, the R3 smoke tests in the
  fulltests job, and the new witness) read v2 while single-node tests read the pin's v1.
  The witness, `TestMQTTAckReplyLayoutV2Replicated`, runs a named account, a configured
  domain and the recommended client ACL; it feeds every acknowledgement it observes on the
  wire to both parsers (`ackReplyInfo`, `mqttParseSourceAckReply`) and checks, from the consumers'
  own accounting, a QoS 1 acknowledgement, a takeover broker acknowledging on the v2
  reply its predecessor recorded across a consumer-leader change with no redelivery, a
  redelivered PUBREL acknowledged at PUBCOMP, and a shared source held by a HandBack on
  its v2 reply (paced NAK, the positive-ACK gate refusing) until another member takes
  it. No broker change was needed. Seven mutations were run and each fails the witness:
  the flag removed, each parser forced to v1 offsets, the shared-gate scope excluding
  v2, recovery refusing a recorded v2 reply, the service ACL denying the v2 prefix, and
  the gate reporting a hand-back it never sent.
  The ACL half is a documented constraint, not code: acknowledgements are published on
  the service connection, so a service grant positional on the v1 layout silently
  denies every v2 acknowledgement (`docs/ARCHITECTURE.md` "Recommended NATS
  permissions").

## 3. Add MQTT transports

### TLS and certificate rollover

- [x] Support MQTT TLS and mutual TLS as tested production listener modes.

- [x] Expose TLS on a second listener, or document why a single configurable
  listener (`-port`, plaintext or TLS) is sufficient. Decided and documented in
  the README: one listener, `-port` selects it in either mode, no implicit port
  change when TLS flags are set, and TLS deployments use `-port 8883`. Serving
  1883 and 8883 at once would need multiple listeners and is not supported.

- [x] Reload certificate, private-key, and client-CA changes from projected
  Kubernetes Secrets without restarting the process or pod.

- [x] Do not require Stakater Reloader or another rollout controller.

- [x] Replace the active TLS configuration atomically only after the complete
  replacement configuration validates.

- [x] Keep the last valid configuration active when a projected update is
  incomplete, malformed, or contains a mismatched key.

- [x] Prove that new handshakes use the replacement certificate while existing
  MQTT connections remain connected.

- [x] Cover expiry, malformed files, mismatched keys, CA rollover, concurrent
  handshakes, and failed reload recovery in native tests.

- [x] Prove the reload's resource lifetime, transferred from §2's resource-lifetime scan
  (2026-10-23), which could not cover a mechanism that did not exist: the replaced
  configurations, certificate pools and any watcher are released, repeated reloads under
  connection churn leave memory, goroutines and descriptors without sustained growth, and
  the reload churn joins `make resource-soak`.

  **Done 2026-10-23, the seven bullets as one item.** A new package, `internal/tlsreload`,
  owns the listener's material from startup on, and `config.Config.TLSConfig` is deleted,
  so one validation decides startup and every reload. The core's `TLSConfig` is a fixed
  configuration whose `GetConfigForClient` returns the active per-handshake configuration
  from an atomic pointer. Each handshake gets one consistent certificate, key and CA, and
  an established connection keeps what it was admitted under. A watcher reads the three
  files every `-tls-reload-interval` (default 10 s), twice per check so that a kubelet
  `..data` swap in progress is skipped. It publishes only a replacement that validates as
  a unit:
  - strict PEM framing that accounts for every byte (a truncated block can no longer be
    skipped silently);
  - key match and a full chain parse;
  - the leaf inside its validity window;
  - a CA bundle in which every block parses.

  A refusal keeps the last valid configuration. It is logged once per run of identical
  causes and re-validated every check, so a certificate refused before its `NotBefore`
  applies when that time comes. An expired certificate is now refused at startup too.
  `docs/ARCHITECTURE.md` "TLS material reload" has the mechanism. `docs/CONFORMANCE.md`
  "A TLS reload reaches new handshakes only" states the residuals: clients admitted under
  a removed CA stay connected, resumed sessions see no replacement certificate, detection
  is bounded by the poll, and a restart on invalid material fails. Tests:
  - `internal/tlsreload`: every invalid shape refused at load and at reload, legitimate
    framings accepted, barrier tests for publication order and for a joined `Stop`,
    resumption across reload for TLS 1.2 and 1.3, CA rollover, and concurrent handshakes
    proving single-snapshot material;
  - two broker integration tests, an existing connection surviving a server-certificate
    renewal and a CA rollover;
  - `TestResourceTLSReloadChurn` in `make resource-soak`, which checks superseded
    configurations and client-CA pools collected by weak pointer, exactly one watcher
    goroutine, and no growth.

  Twenty mutations were run, and each fails its paired test:
  - the callback serving the startup material;
  - publishing a candidate before validation;
  - skipping the key match, the validity window, the CA DER parse (`AppendCertsFromPEM`),
    the chain parse or the single-BEGIN framing check;
  - a refusal clearing the active material;
  - the double read removed;
  - `Stop` not signalling, or not waiting;
  - superseded configurations or CA pools retained;
  - a refused-content cache;
  - log dedup removed, or not cleared on `in-flux`;
  - the CA swap ignored;
  - the expiry warning skipped on refused and in-flux checks, or flagged globally;
  - the soak's classifier blind to `internal/tlsreload`.

- [ ] **Serve plaintext and TLS MQTT at once, for example 1883 and 8883.** Filed
  2026-10-25 by the user's decision, which reverses the earlier one above ("one
  listener, `-port` selects it in either mode ... serving 1883 and 8883 at once is not
  supported"). Today the TCP listener is plaintext or TLS, never both, so a deployment
  moving clients to TLS needs two Deployments or a cutover. The second WebSocket listener
  (`-ws-port`, with `-ws-no-tls`) is the precedent for a second listener in the core.
  What the item has to settle:
  - the flags: a separate TLS port beside `-port`, how each is enabled, the defaults, and
    whether the README's "no implicit port change" rule still holds;
  - what is shared and what is per listener: the `-max-conns` bound, the CONNECT
    deadline, readiness (`/readyz` only once both are up), and shutdown closing both
    (`BeginShutdown` closes the TCP and WebSocket listeners today);
  - mTLS (`-tls-ca`) applies to the TLS listener only. Auth passthrough credentials cross
    the plaintext one in clear, which `docs/CONFORMANCE.md` or the README must state;
  - the example deployment: `docs/deploy/kubernetes` (section 5) checks exactly two
    container ports and one Service port, so a dual-listener option there needs its own
    port and Service entries and the test's closed sets widened to match.

### WebSocket

- [x] Implement MQTT over WebSocket and secure WebSocket.

- [x] Define listener, path, origin, proxy-header, timeout, and TLS settings.

- [x] Add native integration tests for MQTT 3.1.1 and 5.0 over WebSocket.

- [x] Replace or retire the parked upstream WebSocket reference tests once
  equivalent behavior is covered.

  **Done 2026-10-24, the four bullets as one item.** A second listener (`-ws-port`, off by
  default) carries MQTT over WebSocket. It feeds the unchanged protocol core through a
  `net.Conn` adapter, `wsConn` (`internal/mqtt/transport_websocket.go`), adapted from
  nats-server v2.15.1's frame codec. `Read` is one stream of binary-frame payload, so packets
  split across or packed into frames parse as on TCP [MQTT-6.0.0-2], and `Write` sends each
  call as one binary frame. The opening handshake (`transport_websocket_upgrade.go`) is read
  off the accepted connection inside connection setup, after registration and TLS and before
  the CONNECT deadline, so shutdown releases a stalled upgrade the way it releases a stalled
  TLS handshake. It is not `net/http`. `docs/ARCHITECTURE.md` "MQTT over WebSocket" has the
  mechanism:
  - the handshake parsed with `net/textproto` under an 8 KiB socket budget, every field name
    a token, origin-form targets only, one Host, no body, Version 13, the configured path,
    and a valid subprotocol list that must offer `mqtt` [MQTT-6.0.0-3], answered with exactly
    `mqtt` [MQTT-6.0.0-4], no extension negotiated;
  - Origin same-origin by default against the listener's own scheme, `-ws-allowed-origins`
    replacing it;
  - text frames refused with Close 1003 [MQTT-6.0.0-1], with the rest of RFC 6455 §5's
    refusals;
  - Pings answered and peer Closes echoed through a frame slot that keeps frames atomic,
    with terminal states after a torn frame or a sent Close;
  - one function installing the socket's write deadline, a control write bounded by the read
    deadline as well so a pinging, non-reading peer cannot hold the reader past keep-alive.

  Settings: `-ws-port`, `-ws-host`, `-ws-path` (`/mqtt`), `-ws-handshake-timeout` (5 s),
  `-ws-no-tls`, `-ws-allowed-origins`. With TLS material configured the listener serves WSS
  from the same reloading `tlsreload` source; `-ws-no-tls` is for an ingress that
  terminates TLS. Proxy headers are not consumed, since the broker records no client address.
  Startup binds both listeners or neither.

  One finding on the way: the `mqttReader` was bound to `c.nc` at registration, before TLS
  and the upgrade replace it, so the keep-alive deadline reached the socket under the
  adapter. It is now bound in `readLoop`, to the transport setup ended with.
  `docs/CONFORMANCE.md` "MQTT over WebSocket" states the one transport residual: a
  broker-initiated close sends no Close frame, so browsers see 1006. It keeps that separate
  from the scope statements (no compression, `mqttv3.1` alone refused, no proxy headers, NATS
  `connection_types` blind to WebSocket).

  Tests:
  - frame codec, handshake and Origin matrices (`transport_websocket_test.go`,
    `transport_websocket_upgrade_test.go`);
  - paho 3.1.1 and paho.golang 5.0 over `ws://`, each with QoS 1 and 2, retained delivery
    and a resumed session; cross-transport delivery and takeover; a WebSocket Close firing
    the Will; the CONNECT and keep-alive bounds under Ping and unsolicited-Pong traffic;
    shutdown at every setup stage and during a stalled upgrade; injected upgrade failures
    arriving through the accept loop; MaxConn across both listeners; and atomic startup
    (`transport_websocket_server_test.go`);
  - `wss://` with real clients, a certificate renewal, a client-CA rollover, a refused
    reload, and `-ws-no-tls` beside a TLS MQTT listener (`transport_websocket_tls_test.go`);
  - `TestResourceWebSocketChurn` in `make resource-soak`.

  The v5 session test found a pre-existing gap on every transport, recorded rather than
  fixed here: a QoS 1 message published while its subscriber's DISCONNECT is being processed
  is resent only at AckWait after the resume (`docs/CONFORMANCE.md`, "A QoS 1 publish racing
  its subscriber's DISCONNECT waits for AckWait").

  The parked `mqtt_ws_ref_test.go` and the `mqttport_wstests` tag are deleted. Its pub/sub,
  TLS and wrong-port cases are covered natively, and its `connection_types` case cannot apply
  under auth passthrough.

  Thirty-one mutations were run, and each fails its paired test:
  - accepting unmasked frames, text frames, or a field name that is not a token;
  - the mask offset reset per read, the declared length allocated, an empty frame
    returning `(0, nil)`;
  - control writes outside the frame slot, data after a Close, a peer Close not echoed or
    read as a DISCONNECT;
  - the core's write deadline not delegated, not restored after a Pong, deferred while one
    is in flight, or installed directly over one;
  - a Pong bounded without the read deadline, its slot wait unbounded or judged on a
    deadline snapshot, control frames re-arming the read deadline;
  - the upgrade before registration, or without its deadline, or a failed upgrade never
    leaving setup;
  - the Origin check skipped, an allowlist not replacing the default, or the same-origin
    scheme taken from the request;
  - the subprotocol matched case-insensitively, by membership only, or answered with
    another token;
  - an absolute-form target accepted;
  - WSS served from frozen material;
  - a WebSocket bind failure dropped;
  - the `mqttReader` bound at registration again.

## 4. Build the runtime image

- [x] Build the runtime image `FROM scratch`. **Done 2026-10-24**, as the
  **external-NATS** image (the broker connects to a NATS deployment it does not run). The
  runtime stage replaced `gcr.io/distroless/static-debian13:nonroot` and holds the static
  binary and the CA bundle only: no shell, no passwd database, no zoneinfo (log
  timestamps are UTC). Mechanism and limits: `docs/ARCHITECTURE.md` "The runtime image".

- [x] Copy the system CA bundle into the runtime image. **Done 2026-10-24**:
  `/etc/ssl/certs/ca-certificates.crt`, root-owned 0644, from the build stage's Debian
  `ca-certificates`. It is load-bearing, since a `tls://` NATS URL is verified against
  Go's system pool, which reads that file first. A private NATS CA is a mounted file
  named by `SSL_CERT_FILE`.

- [x] Run as a numeric non-root user with a read-only root filesystem. **Done
  2026-10-24**: `USER 65532:65532`, so `runAsNonRoot` can verify it. The distroless
  `nonroot` name could not be verified without a pod `runAsUser`. Nothing on the
  exercised paths writes to disk, so the image supports `readOnlyRootFilesystem` with no
  `emptyDir`; enforcing it is the pod's. `-nats-creds` takes an absolute path, since
  `HOME` is unset.

- [x] Verify outbound TLS connections to NATS, mounted MQTT TLS Secrets,
  health probes, graceful shutdown, and signal handling. **Done 2026-10-24** by
  `test/imagesmoke` (`make image-smoke-external`). It runs the image itself with
  `--read-only --cap-drop ALL --security-opt no-new-privileges` and its own user,
  against the pinned NATS image, with every file delivered through volumes. Eight
  tests:
  - the image's files, owners and modes, and the numeric user, both declared and as
    the kernel reports PID 1;
  - the bundle's file path is what the image's Go runtime reads (directory scan
    disabled);
  - an untrusted NATS certificate is refused as unknown authority;
  - `SSL_CERT_FILE` trusts a private CA, with QoS 1 round trips through JetStream over
    TLS;
  - MQTT TLS served from a read-only, kubelet-shaped Secret mount;
  - SIGTERM turns `/readyz` to 503 `draining` while `/healthz` stays 200 and connected
    clients still round-trip, and a second signal ends the grace with DISCONNECT 0x8B
    and exit 0;
  - the drain starts no earlier than the grace;
  - SIGINT reaches PID 1.

  Not verified: the bundle's public roots against a publicly trusted server, the later
  shutdown phases (in-process tests), unexercised write paths, and the
  pre-registration signal window (source reading). Fifteen mutations were run, and
  each fails its paired test: the bundle dropped, moved to `/etc/ssl/cert.pem`, or
  made 0600; the user named (distroless `nonroot`), group-less, root, or unset; a
  fatal startup write; SIGINT unregistered; readiness not withdrawn; the
  second-signal registration removed or its channel ignored; `InsecureSkipVerify` on
  the service connection; the drain skipped; the grace skipped.

- [x] Build and smoke-test linux/amd64 and linux/arm64 images in CI. **Done
  2026-10-24**: the `image-external` job builds and loads each platform on a native
  runner (`ubuntu-latest`, `ubuntu-24.04-arm`) and runs `test/imagesmoke` against it.
  The multi-platform no-push `docker` job is kept, because it alone exercises the amd64
  builder cross-compiling arm64, as `release.yml` does. Locally only the arm64 leg was
  run (this host is arm64 with no amd64 emulation).

- [x] **Embedded NATS: an in-process JetStream mode and a second production image.**
  Filed 2026-10-24 by the user's decision: at least two production images, one with
  NATS embedded and one without. The deployment topologies are:
  1. the broker with NATS embedded (this bullet);
  2. the broker as its own Deployment, NATS elsewhere (the external-NATS image above);
  3. the broker as an extra container in the official NATS chart's pods (section 5,
     same external-NATS image).

  Each comes single-server and clustered. **Done 2026-10-24.** `-nats-embedded-config`
  runs a nats-server in the broker process from a native configuration file, reached
  in-process, never by URL. On the parsed options the broker:
  - always sets `NoSigs`, so the broker owns SIGTERM and the drain;
  - sets an unset `default_max_consumers` to -1 (F-095, now enforced in this mode);
  - refuses: JetStream disabled, no `store_dir`, another consumer default, a
    `-js-domain` the server does not serve, nats-server's own MQTT/WebSocket listeners,
    leaf nodes, gateways, process log/pid files, and empty passwords.

  Startup waits for JetStream through transient conditions only (a clustered cold
  start is part of its own quorum). Shutdown is the MQTT drain, then the backend
  close, then the server. The second image is the `embedded` Dockerfile target, tags
  suffixed `-embedded` in the same repository. It carries a baked single-node
  configuration whose two users take the broker's own credential variables (an
  anonymous MQTT default with `$MQTT.>` denied), and `/data` owned 65532 as a volume.
  Clustered mode is configuration only (`embedded/nats-cluster.conf`).

  Three things surfaced during implementation and were settled by consult:
  - **An upstream defect**: a refused in-process connect waits out nats-server's 10 s
    write deadline and reports a closed pipe. It is witnessed against the pin and
    recorded as INPROCESS-REFUSAL in `docs/known-upstream-issues.md`. Reporting it to
    nats.go and nats-server is a section 8 item after the alpha. It is a stated residual (a
    wrong credential is refused about 10 s late), not worked around.
  - **The bundle preflight rescans a partial vector, read-only, for up to 30 s before
    refusing**, so brokers that leave the JetStream wait together no longer refuse a
    peer's in-flight provisioning. Its refusal no longer prescribes a destructive
    reset from one observation. This is an availability-contract change carried here:
    the overlapping-scan test now asserts a rescan admission on a fresh complete scan.
  - **A pre-existing residual**: a 5 s `$MQTT_sess` lookup timeout under a concurrent
    first start against an R3 cluster, observed once in four runs. Unresolved.
  - **A security correction found by the holistic review**: the default MQTT identity
    could reach the JetStream API, since an MQTT topic maps onto a NATS subject. An
    anonymous publish to `$JS/API/STREAM/DELETE/$MQTT_sess` deleted the session stream,
    in embedded mode and through the repository's previously recommended external ACL.
    Every shipped ACL (`embedded/nats.conf`, `embedded/nats-cluster.conf`,
    `test/conformance/nats.conf`) now denies `$MQTT.>`, `$JS.>`, `$KV.>` and `$O.>` in
    both directions. The documented deployment prerequisite says the same, and
    `cmd/mqtt-nats-broker/deployment_acl_test.go` checks the ACLs and the attack.
    The second holistic round found the clustered example's route port
    unauthenticated: a peer reaching 6222 joined the cluster past every client ACL.
    The example now authenticates routes from environment credentials, and the broker
    refuses any route listener without route authentication.

  Verified in-process (enforcement table, in-process transport, passthrough and the
  baked ACL on NATS itself, the JetStream wait's transient/permanent split, real-signal
  shutdown order in a subprocess, a three-node cluster formed after a failed first
  probe). The embedded image itself is verified by `make image-smoke-embedded`:
  - its contents;
  - the default invocation;
  - every missing or empty credential;
  - a no-users configuration keeping passthrough's error;
  - persistence across a restart;
  - a three-container cluster surviving SIGKILL of its stream leader.

## 5. Package for Kubernetes

### Kubernetes manifest

- [x] Replace JSON-like flow mappings and sequences in the example manifest
  with readable block-style YAML. **Done 2026-10-24.** `docs/deploy/kubernetes.yaml` is
  now the kustomize tree `docs/deploy/kubernetes` (`base`, `components/tls`,
  `overlays/tls`), all block style, and a test refuses any flow-style node.

- [x] Set the effective Go memory limit to 90 percent of the container memory
  limit. **Done 2026-10-17 by the binary, not the manifest** (§1 "Memory-limit
  strategy"): the manifest sets `resources.limits.memory` and the broker derives the Go
  limit from it at startup, so no `GOMEMLIMIT` value or downward-API arithmetic is
  carried in the manifest or a chart.

- [x] Include secure defaults, probes, resource requests and limits, graceful
  termination, and optional TLS Secret mounts. **Done 2026-10-24**
  (`docs/ARCHITECTURE.md` "The example Kubernetes deployment"):
  - Restricted Pod Security: uid and gid 65532, RuntimeDefault seccomp, all capabilities
    dropped, a read-only root, and no service-account token.
  - Credentials from the Secret `mqtt-nats-broker-nats`, and a labelled placeholder image
    tag instead of `latest`, which a prerelease never publishes.
  - Readiness withdrawn after one failed probe. The grace was raised from 30 s to 60 s:
    the drain delay plus the client drain, health server and NATS close budgets (45 s)
    plus headroom. The old value could cut the backend close short.
  - TLS as an opt-in overlay: port 8883 from a required `kubernetes.io/tls` Secret,
    mounted as a read-only directory so the reload sees renewals.

  The settings are judged against the binary by
  `cmd/mqtt-nats-broker/kubernetes_manifest_test.go` in `make test`:
  - `config.Load` evaluates the environment, and every name is checked against
    `internal/config`;
  - the probe paths are served by the extracted `healthMux`;
  - the grace is summed from the exported budgets (`mqtt.ShutdownDrainTimeout`,
    `backend.ServiceCloseBudget`, `httpShutdownTimeout`);
  - `resolveTLS` loads a certificate laid out under the Secret's keys.

  `make kubernetes-render` (CI `kubernetes-manifest`) renders the base and the TLS
  overlay with kustomize v5.8.1 (`go install`, checksum-authenticated) and applies the
  same checks. The test refuses any field outside the set the example uses, so an
  override the checks cannot judge (container `args`, a container `securityContext`, a
  probe scheme, a Secret mode) fails, and the same for environment variables. Forty-six
  mutations were run; each fails its paired check. The test's contract is agreement with
  the binary and the stated defaults: resource values are not judged.

  Not verified: real API-server and Pod Security admission (a request above its limit
  passes the test), Secret projection in a cluster, and endpoint propagation timing (the
  Helm CI bullet's cluster install).

### Helm chart

- [ ] Add a production Helm chart for the broker.

- [ ] Include Deployment, Service, TLS mounts, probes, resources, security
  context, autoscaling options, PodDisruptionBudget, and graceful termination.

- [ ] Support an external NATS/JetStream cluster.

- [ ] Add the official NATS chart as an optional dependency.

- [ ] Expose the dependency configuration under `nats:` so users can place
  normal NATS chart values there without translating every field.

- [ ] Offer the broker as an extra container in the official NATS chart's pods.
  Filed 2026-10-24 by the user's decision on deployment topologies (section 4, the
  embedded-NATS bullet): this is topology 3. It uses the external-NATS image, added
  through the NATS chart's own `values.yaml`, and connects to `localhost:4222`. Offer
  it, and the separate Deployment (topology 2), each with a single NATS server and
  with a JetStream cluster. **Shutdown ordering is the design point.** Kubernetes gives
  ordinary containers no termination order, but the broker needs NATS through the
  MQTT drain and through the backend close that follows it: `main.go` runs
  `BeginShutdown` to completion, then the deferred `be.Close()` drains and flushes
  the service connection. So NATS must outlive the broker process. Native sidecar
  ordering does this only with NATS as the sidecar, since sidecars stop after the main
  containers; making the broker the sidecar reverses the dependency. Otherwise use a
  preStop hook on the NATS container that waits for the broker to exit.
  Topology 1 (the embedded image, section 4) needs, single or clustered:
  - a StatefulSet with `podManagementPolicy: Parallel`;
  - a headless Service with `publishNotReadyAddresses: true`, so route names resolve
    before any pod is ready;
  - a PVC per pod with `fsGroup: 65532`;
  - a PodDisruptionBudget allowing one disruption;
  - a startupProbe bounding the JetStream wait, during which HTTP is not served;
  - `server_name` from the pod name;
  - the four credential variables and the route credentials
    (`MQTTNB_EMBEDDED_ROUTE_USER/PASS`, `MQTTNB_EMBEDDED_ROUTES`) from Secrets;
  - a NetworkPolicy limiting the route port to the broker pods, and route TLS on an
    untrusted network.

  See `docs/ARCHITECTURE.md` "Embedded NATS".

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

- [ ] Rename the `mqttport_fulltests`, `mqttport_clustertests`, and
  `mqttport_tlstests` build tags and CI references. (`mqttport_wstests` no longer
  exists: §3 "WebSocket" retired the parked file it selected.)

- [ ] Preferred replacements are `mqtt_upstream_suite`,
  `mqtt_cluster_reference`, and `mqtt_tls_reference`.

- [ ] Remove `port`, `ported`, and `porting` as project-identity language from
  the README, CI, lint configuration, comments, and documentation.

- [ ] Remove "bridge" as project-identity language left after the section 1
  rename to `mqtt-nats-broker`: Go comments, operator-visible diagnostics (for
  example "recreate the bridge-owned JetStream state" in `internal/mqtt/sess_manager.go`,
  `orphan_qos2_sweep.go` and `shared_group_verify.go`), test-fixture prose, and an
  explicit disposition of the `test/depproof/run.sh` `--bridge` flag and its
  `bridge_test`/`bridge_run` identifiers. Keep "bridge" as a verb and as MQTT
  broker-bridging terminology. The census as of the rename is
  `git grep -n -i -w bridge`, excluding `docs/findings/`, `CHANGELOG.md` and
  `docs/PORTING.md`; finished roadmap narrative stays as written.

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

- [ ] Every unconditional alpha blocker is complete.

- [x] The supported HA scope is documented: multi-frontend (section 0.2,
  decided 2026-08-10). Its blockers are unconditional, so the gate above
  covers them.

- [ ] Every release-verification investigation is closed or has produced a
  confirmed finding with an explicit release disposition.

- [ ] Every post-alpha candidate has an explicit disposition; an unchecked
  post-alpha item does not by itself block the alpha.

- [ ] A repository documentation check proves every canonical `F-###` dossier
  appears exactly once in the active or resolved finding index and that the
  roadmap's closed total matches the index.

- [ ] The full test, race, lint, conformance, Paho, container, and deployment
  gates pass with no unexplained failures or skips.

- [ ] CI is green from a clean checkout on every supported architecture.

- [ ] MQTT 3.1.1 and 5.0 conformance baselines have no unexplained regression.

- [ ] The scratch image and Helm chart install successfully in a clean cluster.

- [ ] TLS rollover, WebSocket, authentication, authorization, migration, backup,
  restore, upgrade, rollback, and shutdown procedures are documented and tested.
  **Backup and restore are per-BUNDLE**: all ten broker-owned `$MQTT_*` streams are
  one recreation unit, taken and restored together from one point in time, since
  startup refuses a proper subset and refuses members carrying different recovery
  epochs (F-066). Skew WITHIN one incarnation is undetectable, so the procedure has to
  guarantee the coherent instant rather than rely on the check.

- [ ] Release artifacts use the final product name, `mqtt-nats-broker`,
  consistently (section 1, "Product name").

## 8. Post-alpha candidates and scope cleanup

Nothing here blocks the alpha: the alpha release gate requires only that every item has an
explicit disposition. It sits after the gate so that document order reaches it only once
the alpha work in sections 0 to 7 is done.

- [ ] **Report INPROCESS-REFUSAL to nats.go and nats-server, after the alpha release.**
  Filed 2026-10-24 by the user's decision. A connect the server refuses over an
  in-process connection waits out nats-server's 10 s write deadline and reports `io:
  read/write on closed pipe` instead of the refusal. In embedded mode, a wrong MQTT
  credential is therefore refused about 10 s late. The mechanism, the source locations
  and the witness are in `docs/known-upstream-issues.md` (INPROCESS-REFUSAL); the residual
  is stated in `docs/CONFORMANCE.md`. The user reads, verifies and writes the reports
  personally, because the NATS maintainers do not want AI-written issues. The witness
  (`~/.claude/lanes/embedded-nats-image/upstream/inprocess_refusal_test.go`, against
  nats-server v2.15.1 and nats.go v1.54.0) and the stack notes are the material for that.
  Before reporting, re-run the witness against the then-current pin and check both
  projects' `main` and issue trackers: a fix upstream turns this into a pin bump. Not an
  alpha blocker: the refusal still happens, only late.

- [ ] **Sweep the documentation corpus for stale F-072 status claims, by MANIFEST rather
  than by search.** Filed 2026-09-10 by the F-097/F-088 split filing, whose holistic review
  found one more instance of this class in each of four consecutive rounds. **Not an alpha
  blocker**: it is documentation consistency about existing findings, not a newly
  demonstrated broker defect, which is why it sits here beside the comment-corpus sweep
  rather than in §0.3.

  **The authoritative status, stated here so this bullet cannot itself misdirect planning:
  F-072 LANDED 2026-09-09. F-097 precedes F-088. Both remain unconditional alpha blockers.**
  This deferral must **not** become another prerequisite delaying F-097.

  **Why it is filed rather than finished, and this is the whole point of the bullet.**
  Four
  successive boundaries were drawn and each proved a strict subset of the class:

  | Attempt | Boundary used | How it failed |
  |---|---|---|
  | 1 | the phrasings already seen | four survivors did not match them |
  | 2 | every LINE containing `F-072` | three survivors sat in paragraphs containing no such string |
  | 3 | five F-072-**owning** sections, read end to end | four more owning regions were named |
  | — | a fifth enumeration by the same author | the enumeration **is** the defect |

  **The lesson, in the form the ruling gave it: an author-selected list of relevant
  regions
  does not prove a superset.** A complete file manifest supplies the finite outer boundary;
  semantic review still supplies the evidence. `feedback_sweep_boundary_must_be_a_superset`
  and `feedback_frozen_close_set_ephemeral`.

  **The method this sweep must use.** Freeze a revision and generate a **mechanical
  manifest
  of all tracked documentation under `docs/`**. Read **every** document in that manifest and
  record one of three dispositions per document: reviewed / no relevant claim / correction
  needed. **Search results assist discovery; they do not define inclusion.** For each claim
  found, distinguish **historical diagnosis**, **delivered protection**, **current
  mechanism** and **still-open obligation** — correcting by qualification rather than by
  deletion, so rejected designs and valid residuals survive. Acceptance is the reconciliation
  of status, dependencies, mechanism descriptions and acceptance obligations across that
  corpus, followed by a holistic review.

  **Nine coverage seeds, which OVERLAP and span eight files. They are seeds, not a
  census**,
  and their actual dispositions belong to the sweep: `docs/ROADMAP.md`'s F-072 bullet;
  `docs/findings/release-investigations.md`'s F-072 entry; `docs/ARCHITECTURE.md`'s F-072
  sections; `docs/findings/README.md`'s F-072 entry; `docs/CONFORMANCE.md`'s F-072 sections;
  `docs/findings/shared-qos2.md`'s F-050 material; `docs/findings/ha-arbitration.md`'s
  F-087/F-088/F-097 material; `docs/findings/persisted-identity-census.md`'s unsent-marker
  section; and `docs/CONFORMANCE.md`'s restore-gate residual. **Note that the filing change
  already touched several of these** — it corrected F-050 and F-087/F-088 material and
  introduced F-097 — so they are not untouched pre-existing text and must not be audited on
  that assumption.

  **Already corrected by the filing change, and not to be re-done blindly:** the F-072
  entries in `docs/findings/README.md` and `docs/findings/release-investigations.md`;
  F-072's own `[x]` roadmap bullet, whose pre-closure diagnosis and design paragraphs are
  now labelled historical; `docs/ARCHITECTURE.md`'s rebind-race DUP consequence (the race
  itself STANDS and was deliberately not deleted); and `docs/CONFORMANCE.md`'s restore-gate
  first-transmission DUP assertions and its affine paragraph. **Deliberately NOT retracted
  anywhere, and still open:** the lost prompt NAK, the AckWait delay, the ordering residual,
  the displaced-broker window, and F-088/F-097 themselves.

  Evidence and the round-by-round record in
  `~/.claude/lanes/replacing-allocation-lifecycle/` (`review-ledger.md` rounds H4-H7,
  `DECISIONS.md` C3).

- [ ] **Sweep the comment corpus for six false-claim families, indexed by CLAIM rather
than
  by phrasing.** Filed 2026-08-26 by the pre-CONNACK-signal item's tenth holistic review round,
  which ruled: *"Stop iterating on this code item after recording them in a separate
  corpus-sweep finding"* and *"No further implementation review round is justified by the
  evidence; the code itself clears the commit gate."* **Not an alpha blocker** — every instance
  is a comment or a doc sentence, and the same round confirmed the code sound.
  *Why it is its own item.* Nine holistic rounds fixed instance after instance and the
  families
  kept producing more, because they were being enumerated by **grep pattern** — and a claim
  written five different ways is not greppable. The families are:
  **(1) the lifetime-restoration fiction** — comments saying `abortAcceptedResume` puts
  the
  session lifetime back. It does not: it abandons the restore gate and calls
  `mqttAbortAcceptedConnect`. What protects the lifetime is ORDERING, because
  `applyRequestedLifetime` runs after the earlier refusal sites. Some of these comments name
  mutations that **cannot be written** (`restoreRefusedResumeLifetime` does not exist), which
  is the `feedback_mutation_run_must_compile` failure in prose.
  **(2) the F-084 ownership inference** — verdict comments asserting that a rejected CAS,
  an
  absent record or a gone record means another owner holds the session. It means the attempted
  write did not land. This subset is F-084's distinction and is what can mislead later
  Will/disposal work into treating a concession as a demonstrated takeover.
  **(3) refusal-signal contradictions** — comments still saying a pre-CONNACK refusal
  writes no
  packet, or that no packet can identify an unrestored filter, when the code three lines later
  builds that filter's key into the Reason String the v5 failure CONNACK carries.
  *Seed locations, from the review that filed this — a SAMPLE and explicitly not a
  census:*
  family 1 at `internal/mqtt/sess_resume_reconcile.go:407` and
  `internal/mqtt/sess_resume_reconcile_e2e_test.go:2696-2706`; family 2 at
  `sess_resume_reconcile.go:283` (`mqttRejectionForeign`, reached by
  `classifyRejectedProbe` from an ABSENT record), `shared_group_arbitrate.go:107` and `:149`,
  `sess_uncertain.go:419-421` and `:858-870`, `mqtt.go:12135` and `:13354`; family 3 at
  `mqtt.go:12878` and `docs/ARCHITECTURE.md:3495-3497`.
  **(4) the unscoped retry premise** — four sites stating that "every MQTT connection
  retries
  the whole initialization" without the scope that makes it true. It holds only until one
  attempt succeeds or `Start`'s own attempt fails and exits: `startMQTT` opens the listener
  before the eager initialization, so a CONNECT in that window drives initialization itself,
  and after any success the CONNECT path re-initializes nothing. Corrected 2026-08-28 by the
  ephemeral-consumer identity item at `mqtt.go` (the watch-provisioning comment),
  `stream_prereqs.go` (two) and `sess_record_watch_test.go`; filed here because the
  family is what a future reader needs, not the four instances.
  **(5) the "$MQTT_rmsgs issues no delete API request" claim** — seven sites, corrected
  by the same item, and a worked example of why this bullet exists: five review rounds
  each found another instance, including one WRAPPED across two lines that no
  line-based grep can match and one phrased passively. It was closed only by a wrap-
  and phrasing-insensitive sweep plus a frozen close set with a res-judicata rule. The
  claim was **already false before that item** (the rollback has always issued
  `CONSUMER.DELETE` against `$MQTT_rmsgs`); what stays true, and is what the corrected
  sites now say, is that the owner issues no MESSAGE delete, which is the only thing
  `DenyDelete` gates.
  **(6) the re-minted-generation safety claim** — added 2026-09-01 by F-036's second
  attempt. `reopenQoS2ReleaseFence` justifies reopening the release fence inside `clear()`
  with "Safe because the incarnation has been re-minted by then, so a newly admitted release
  works on the fresh generation's subjects, which this teardown cannot name." **False in
  both directions.** A PUBREL derives its hold subject BEFORE it reaches
  `beginQoS2ReleaseOp`, so a release admitted after the reopen can name the RETIRED
  generation's subjects; and the fresh generation's subjects are not out of reach either —
  they are exactly what a displaced connection can write to, which is the single-broker
  silent loss F-036's bullet now records. The family, not the one site: any comment
  asserting that a re-mint by itself puts the previous connection out of reach of the new
  incarnation's names. F-036 owns the code fix; this bullet owns the corpus.
  *How to close it:* index by the CLAIM — read every comment that asserts one of the six,
  rather than grepping for the wording that happened to be used. Family 5's history is the
  argument: a grep-shaped sweep will keep returning one more instance. **F-084 cross-links
  family 2
  only**, and stays scoped to successor evidence.
  *A worked precedent, 2026-09-01, and it is NOT a seventh family owed here.* F-036's
  course E
  had its own claim class — "the orphan sweep will later collect an unreferenced affine
  consumer", asserted in production comments, an OPERATOR-VISIBLE error message, two census
  dispositions, four documents and two preserved finding records. It was closed **within that
  change**, so nothing is owed here for it. What is worth reusing is the method, because three
  review rounds each named one more site a hand list had missed and the reviewer then ruled
  that "manual enumeration is the wrong instrument for exact identifiers": a **zero-match
  symbol closure** over the removed identifiers (including test-only ones, or a dead helper
  survives it), plus a **generated prose review set** from a normalized term list with every
  match dispositioned. Term breadth had to be MEASURED rather than argued — an unbounded
  `Q2Aff` matched 3362 lines and made the set unusable, while word-bounded `\bQ2Aff\b` plus
  the durable prefix `Q2AFF` kept it at 761 across 54 files. That is family 5's lesson made
  mechanical, and it is the shape this item should take.

- [x] **Re-census F-033's claim-transition and claim-reap route inventory from the tree.**
  **PARKED with the experimental shared-QoS-2 implementation in chapter P, not done.**
  Filed 2026-09-01 by F-036 course E's second holistic review round, which caught the
  attempt to update the totals by SUBTRACTING from them. **Not an alpha blocker**: it is
  a documentation inventory, and every mechanical claim resting on it (the fenced/unfenced
  split, the F-065 TOCTOU-in-the-primitive argument) is unaffected by the count.
  *Why it needs redoing rather than adjusting:* the per-site totals and line numbers in
  `docs/findings/ha-arbitration.md`'s "What the audit did NOT find" census were
  **already wrong before course E**. Measured at filing: the reap class has **17 direct
  production call sites** — five in `orphan_qos2_sweep.go`, eleven in
  `qos2_shared_affine.go`, and **one in `mqtt.go`'s PUBREL/PUBCOMP discharge path that the
  enumeration never listed at all** — against a stated twelve, and the cited line numbers
  do not resolve (`qos2_shared_affine.go:1727` is blank at `4afa533`). Course E removed
  one reap site and one transition site and recorded only that MECHANICAL delta, marking the
  totals stale rather than restating them.
  *How to close it:* enumerate from the tree, disposition every site, and prefer a check
  that
  cannot drift — the affine consumer delete count in the same census IS verifiable by
  `deleteConsumer(mqttOutStreamName, ...)` and was verified that way, which is the model.
  A lineage or TOCTOU audit must not trust the current numbers.

These do not block the alpha unless investigation shows that ordinary broker
operation can trigger correctness loss or unbounded resource growth.

- [ ] [F-046: Split the in-flight stabilization
follow-ups](findings/inflight-followups.md#f-046).
  Schedule the stale-metadata cleanup and settlement-fence work independently. The one
  alpha-time part, deciding the compatibility-sensitive schema names, is in §1 ("In-flight
  verdict schema names").

- [ ] [F-061: Send a CONNACK before closing an unauthorized
CONNECT](findings/runtime-correctness.md#f-061).
  **Small.** The shim's `authViolation` only closes the socket, so a client whose
  per-client NATS dial is refused never receives CONNACK 0x05 / 0x87 — while
  `mqttProcessConnect` (`connect_process.go`) comments that the CONNACK is enqueued there
  and traces one as sent.
  MQTT 3.1.1 [3.1.4] makes the close a MUST (met) and the CONNACK a SHOULD (not
  met); verify the v5 wording when planning it. Found while reading this path for
  F-051, not caused by it. Both conformance suites pass today (124/124, 106/106),
  which is why it is a post-alpha candidate rather than a blocker; promote it if a
  suite or an operator report contradicts that.

- [ ] F-085: Carry the authorizing NATS account through the transport. **Codex's
  wording, taken verbatim from the consult that closed F-079/F-080/F-081, because
  it is the stopping rule for residual 7 and paraphrasing it would soften the
  atomicity requirement:** *carry the authorizing NATS account with every queued
  service and per-client write, including JSA requests and live fan-out, and
  validate it atomically at final transport emission after reconnect. Same-account
  reconnects must preserve pending work; changed or unknown accounts must withhold
  it without finalizing durable obligations, including QoS 0 Wills.* This is the
  class residual 7 in `docs/CONFORMANCE.md` records, not a single window: the
  pre-write account check is repeated before every durable write and is atomic with
  none of them, and three layers can hold a write in between — nats.go's reconnect
  buffer on the service connection, the JSA send queue (which carries no record of
  the account that authorized each entry), and the per-client reconnect buffers.
  **Removing one layer was tried and reverted**, and the reason bounds any plan
  here: disabling the service reconnect buffer narrowed nothing, because the send
  queue behind it holds writes just the same, and it made a QoS 0 Will fail outright
  on an ordinary blip so the outbox finalized a Will that was never fanned out. The
  hazard needs an operator to repoint credentials mid-flight; that regression needed
  only a blip. Post-alpha because it is a transport change, and the check-not-
  transaction gap is stated rather than hidden.

- [ ] F-086: Prove the Will authorization decision at the two discharge sites that
  no fixture reaches. **Test coverage, not a known broker gap.** The decision is
  captured at CONNECT and read at four sites; a per-site mutation census (forcing each
  site's verdict `true` and running the whole suite) shows the immediate path and the
  delayed-outbox gate each fail their paired fixture, while `mqttPublishOwedWill` (the
  lost-session-gate route) and the uncertainty-parked gate in `sess_uncertain.go`
  survive. Reaching those two needs a record-derived eviction and a session-record
  ambiguity respectively, neither drivable from a connection, so both want a
  seam-driven arm with the service identity established (`svcID.account`/`valid` plus
  the client's `authzSvcAccount`) and `willAuthorized` set on each side. **A first
  attempt was written and deleted**: it passed with a live control and still reached
  the IMMEDIATE site, which the mutation caught — so any arm here is only worth having
  if its own mutation kills it. Stated as CONFORMANCE residual 12 meanwhile.

- [x] [F-049: Provide operator recovery for unreadable affine
claims](findings/shared-qos2.md#f-049).
  **PARKED, NOT FIXED: chapter P.** No experimental-state recovery or migration machinery
  is in alpha scope. Operators must perform a whole-bundle reset before leaving the
  experiment; detecting or repairing incompatible experimental state is unsupported.

- [ ] Consolidate embedded NATS servers across the fork-repair test fixtures. **Test
  infrastructure, not a broker gap.** `sess_fork_repair_test.go` starts one embedded
  server per subtest through `testNewInflightEnv`/`runNATS` — roughly 37 for the file —
  and that suite weight is the leading (not conclusively attributed) explanation for a
  pre-existing timing-proxy test becoming flaky, which was resolved by replacing that test
  with a deterministic barrier seam (F-027's implementation record has the full evidence
  trail). Sharing one server per top-level test would cut ~25 of them, but it is NOT a
  harmless count optimization; the constraints a plan must answer are recorded there: a
  fresh `Server`+account-manager+connection+node name per subtest (a shared asm lets
  `dropPersistObservations` installs clobber each other, and the hard-coded `node1` would
  make managers read each other's `Origin` writes as local), per-subtest quit channels so
  one sibling's cleanup cannot stop the shared server, a durable-name audit within each
  sharing group, and lingering watch/retained consumers accumulating in the shared streams
  up to the 5-minute inactivity threshold. Any fixture change re-runs F-027's full
  mutation table, since shared state can mask a discriminator whose assertions are
  unchanged.

- [ ] Force two session-record proposals into the clustered pre-proposal window.
  **Test infrastructure, not a broker gap, and deliberately not a §0.2 blocker.**
  **Motivation changed 2026-08-17, item NOT retired.** Its subject is the SAME-TERM
  clustered admission/apply window, and that window still exists — the in-process
  per-subject map refusing the second same-base write with 10164 is exactly what it
  targets. What §0.2 retired is the CROSS-ELECTION consequence that used to be attributed
  to it (the same-base boundary in `docs/CONFORMANCE.md`). Retiring this bullet would need
  its own audit of what a test here could still observe.
  `TestMQTTTakeoverCreateRaceReplicated` asserts the property on a real Replicas=3
  stream — one winner, a definite loser, the winner's bytes surviving — but a barrier
  only makes both goroutines runnable, so it cannot hold one proposal between clustered
  admission and apply and release a second into that window. The mechanism itself is
  owned and tested upstream (`jetstream_batching.go:777-782`, with
  `jetstream_batching_test.go:1576-1586` and `jetstream_cluster_4_test.go:5158-5164`),
  which is why this is a limit on what a test HERE can observe rather than a
  conformance residual. Closing it needs a seam in nats-server, so an upstream issue is
  the likelier route than anything in this repository.

- [ ] **`reapRegisterWithVerdictGated`'s `nonce` parameter is dead.** Filed 2026-09-06 by
  the F-090 disposition. The body gates on `ref.matchesRecord(cur)` and forgets against
  `cur.Nonce` from its own fresh load (`internal/mqtt/inflight_register.go`), so the
  parameter is never read, while every caller passes one — which reads as authority the
  code does not use, and is why an earlier F-090 plan revision's mutation M12 was invalid.
  Either remove the parameter and fix its callers, or state in the comment why it is kept.
  Scoped out of the F-090 disposition deliberately: that change was docs and comments only,
  and a signature change touching every caller belongs with the suite run it deserves.

- [ ] After alpha, write an ADR for or against a schedule-backed delayed-Will timer.
  A schedule may publish only into its own stream, so the ADR must preserve F-031's
  atomic `$MQTT_sess` record/acceptance fact and must not claim cross-stream
  cancellation. The broker, not the scheduler, remains responsible for MQTT publish
  semantics. See the
  [platform audit](findings/nats-platform.md#message-schedules-post-alpha-will-outbox-spike).
