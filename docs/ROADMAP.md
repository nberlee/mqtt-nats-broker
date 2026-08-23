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

**A scope lever, recorded rather than taken (2026-08-18).** Nine of this section's open
items sit on the session-affine shared-subscription QoS 2 path: F-045, F-047, F-048, F-049,
F-050, and F-063 through F-066 from F-033's audit. That is the largest single block of
release-blocking work, and it belongs to one OPTIONAL MQTT 5 feature — declining shared
subscriptions is protocol-legal, via reason code `0x9E` on CONNACK and SUBACK. Moving it out
of first-alpha scope would retire that block in one decision instead of nine.
**Not taken:** shared QoS 2 remains alpha scope and those nine remain blockers — eight since
F-063 closed on 2026-08-19; the count above is the one the decision was weighed against, and
is left as recorded. Recorded
here so the option is visible when the cost of the block is next weighed, and because
F-034's audit was expected to behave like F-033's — which it did, on 2026-08-22. Taking it
would need a README change (the
feature is promised there) and a `docs/CONFORMANCE.md` statement; it is a different axis
from the multi-frontend HA decision, which stays as decided.

The 35 closed findings are indexed in the [finding dossiers](findings/README.md).
Of those, 31 are retained in the
[resolved-findings archive](findings/resolved.md); F-027, F-029, F-030 and F-031 remain
in the mixed-status [HA arbitration dossier](findings/ha-arbitration.md) beside their
open follow-ups. The entries below are the active release view; their linked dossiers
are canonical for mechanism, evidence, rejected designs, tests, and residuals.

### 0.1 Remove pre-release persistence compatibility

Do these before further MQTT-core implementation. The bridge has no released
persisted-state contract, so carrying these paths through subsequent session,
retained-message, and Will changes would make every change maintain and test
formats that no user release depends on. Each checkbox is one commit.

- [x] [F-056: Remove inherited retained-message compatibility](findings/resolved.md#f-056).
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
  touches it — so it persists until an operator deletes the message or recreates the
  stream, which is what the diagnostic prescribes. Permanence has a running cost, and
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
cross-instance HA promise. Every item in this subsection is therefore an
unconditional alpha blocker. The lineage work was designed against an observed
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
  excluded until F-050 closes** because the success CONNACK precedes shared restoration.
  `docs/CONFORMANCE.md` states every bound and the cap-forfeiture residual.
- [x] [F-029: Arbitrate simultaneous takeovers from one base](findings/ha-arbitration.md#f-029).
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
  post-check windows, the same bound the affine reaper has; `$MQTT_teardown` recreated
  alone loses every open fence. A finish that outruns the survivor broker's own
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
  recreate (the errors offer no in-place fix, and `$MQTT_sess` is coupled to
  `$MQTT_teardown` — and, at the time, to `$MQTT_origin`, which the write-lineage audit
  has since deleted).

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
  affine-pair failure skips affine claim reconciliation and affine consumer deletion
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
  generation-retirement watermark names in a required `rwid` field, for the interval
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
  creates of one interval different parents, and an out-of-band deletion leaves a
  watermark naming the last RETIRED record rather than the deleted one, so a create
  following it matches neither the record that was deleted nor its parent. All four are
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
  bullet); `WID`/`PWID` stamping at all three writers; `RWID`, now with **no reader**,
  pinned until F-036 and the vanished-record vocabulary decide; the create arm's
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
  the bound-rejection foreign arm calls `mqttDiscardWillOnTakeover` and the forked arm
  deliberately does not, so reporting this route as a takeover would cancel a delayed Will
  that is still owed and skip the repair capsule
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
  correcting them is part of filing it. - [x] [F-062: Bound the inbound QoS 2 hold purge
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
  enumeration watermark. - [x] [F-033: Add write lineage to shared QoS 2
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
  reaches only when that session is resident while its record is absent. The
  different-generation arm is **settled**, and a lost sweep-delete response is a bounded
  delay that converges. The four in-code "F-033, unaudited" markers across three sites
  now state the answer, and three further sites that asserted something the audit
  falsified are corrected too. Holistic review then found one more, filed as **F-066**:
  the claim key identifies one source message only within one incarnation of
  `$MQTT_msgs`, and recreating that stream is a procedure a startup refusal instructs. -
  [x] [F-063: Prove a copy was retained before acknowledging its
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
  premises are distinct, and what this item's refusal strands is F-064's to resolve. - [
  ] [F-064: Reconcile assignments independently of owner
  resume](findings/ha-arbitration.md#f-064). **Alpha blocker. Partly landed; the box
  stays open deliberately.** `buildSessionRefs` keys `refs.affine` on `ps.Q2AffGen`
  alone, so a live matching record makes the sweep skip **every** claim of that
  generation, and `recoverQ2AffClaims` — the only writer of `Lost` — runs solely on the
  owner's own resume. **What landed:** on-behalf restoration of a destroyed affine
  consumer. A contender taking a group redelivery rebuilds the winner's durable from its
  record and materializes onto it, so the stall is closed **without the owner for
  assignments that never reached `materialized`**. Neither gate below is addressed — the
  remedy bypasses both, because a redelivery carries the payload and the sweep does not.
  **What did not, and why the box is not ticked:** - *The stall for a `materialized`
  assignment whose copy is gone* — residual 8. That population takes a different
  dispatcher arm and still waits for its owner. Re-storing there was designed and
  **refuted twice**: the copy CAS and the claim CAS are on different streams, so no
  claim-side fence orders a contender's store against the winner's authorization of the
  original PUBLISH, and the replacement copy can become a second PUBLISH for an exchange
  already past PUBREC. The route back to this is the PUBREL-carried claim reference in
  the `Lost`-truthfulness item below, because that PUBREL is durable and account-wide —
  but note the direction: it gives PUBREL → claim, and a contender needs claim → PUBREL,
  which is a reverse lookup that must be costed before it is promised. - *The
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
  keeps the box open:** residual 8, and W-c above. Both are argued rather than pending,
  and both have a named route: residual 8 through the `Lost`-truthfulness item's
  PUBREL-carried claim reference, W-c through nothing currently available. - [ ]
  **Report a delivered shared QoS 2 message truthfully instead of as `Lost`.** Split out
  of F-064; not a conformance defect, so not an alpha blocker. *The finding:*
  `materialized ∧ ¬AffineDone ∧ copy absent` is reported `Lost` for a message the client
  answered with PUBREC, because `mqttProcessPublishReceived` stores the correlated
  PUBREL before it reaches `mqttFinishQ2AffineDelivery`. *Why it is not a deviation:*
  `Lost` releases the group delivery without re-offering the message, so nothing is
  redistributed. Writing `Discarded` instead is **not** an `[MQTT-4.8.2-4]` violation
  for this population either — the PUBREL is persisted independently and replayed, so
  the handshake completes — it is simply untruthful. The conformance objection applies
  only to the undifferentiated shape, where nothing proves the client was reached. *Five
  refuted mechanisms, with the code behind each:* gating the in-flight register
  transitions on the write (duplicate PUBLISH); issuing the write inline at the
  classification site (per-record round trips inside the pre-CONNACK barrier); an
  in-memory carrier from classification to `recoverQ2AffClaims` (does not survive broker
  displacement, while the `Retired` register does); a retry for an unproven write
  (`mergeProven` forces `AffineDone` false forever once `Lost` is on the record, so no
  retry terminates); and an unresolved-joins set in a *suppressing* role (redundant with
  the durable `AffineDone` the recovery arm already reads — but see the constraint
  below, which narrows that refutation). *Binding constraints on any design:* the writer
  must call `reapQ2AffClaim` itself, since discharging it by ordering is unavailable —
  the recovery-and-reap route is skipped whenever affine consumer restoration fails. The
  pass must run **before** `recoverQ2AffClaims`, and an unproven advancement must
  suppress that function's `Lost` transition for the affected claim, or `mergeProven`
  prevents any later `AffineDone` from ever sticking; the suppressing set is redundant
  only once `AffineDone` has been *successfully persisted*. And PUBCOMP must not destroy
  the only proof, which needs the equivalent of `secureRetiredSource`'s
  persist-before-ack. *The design to evaluate first:* make the correlated PUBREL carry
  the `Q2AffClaim` subject. The association is in hand at PUBREC immediately before the
  PUBREL is stored, and a post-CONNACK pass over those PUBRELs could advance the named
  claims directly. The ordering property that makes it safe is read-loop serialization —
  no PUBCOMP can be processed until `mqttProcessConnect` returns — **not** "before
  PUBREL redelivery is enabled", which is false. It adds one persisted header, so it
  needs its own compatibility and F-066 analysis, which is why it is a separate item. *A
  residual any register-based design inherits:* an authoritatively absent consumer
  resolves `mqttInflightSourceLost`, not Unknown, so the retire branch is forced while
  the durable is gone; once the consumer exists — including after on-behalf restoration
  recreated it for some other claim — the same absent copy resolves `Complete` and the
  register is dropped, destroying the only record joining the PUBREL's `(Dur, Seq)` to
  the claim. - [x] **A saved shared filter that fails restoration at CONNECT is silently
  dropped.** **This was F-050 filed twice** — same mechanism, same consequence, same
  dossier — and closing it ticks both boxes; see §0.3's F-050 entry for what landed.
  Kept here rather than deleted because this filing is the one that carried the F-064
  review's constraint on any fix: failing the CONNECT *after* the CONNACK is ungraceful,
  so it publishes the Will and can delete a zero-expiry session. The remedy respects that
  by not acknowledging at all — restoration moved BEFORE the CONNACK, where the existing
  pre-CONNACK abort discipline already discards the Will and leaves the lifetime
  ungranted. - [ ] **Prove a shared group durable exists when a resume restores its
  membership.** Surfaced by F-050's plan review, which falsified the obvious remedy.
  `processSharedJSConsumer`'s resume fall-through subscribes to the recorded deliver
  subject and stops, so a group durable deleted out of band — a concurrent leaver's
  last-holder race, an operator, an account reset — leaves a live subscription on a
  subject nothing publishes to, with no error anywhere. It is NOT F-050's: F-050 is about
  a per-filter FAILURE being dropped, and this reaches the same accepted-but-dead state
  with no failure at all. **The obvious fix is refuted**: shared consumer configurations
  are deliberately not persisted (`SharedSubs` is the authority and group-owned configs
  are filtered out of `Cons`), so the fall-through's `cc` is a stale in-memory config from
  an earlier join, and re-issuing it with `ActionCreateOrUpdate` would overwrite a live
  group durable's `AckWait`/`MaxAckPending`/storage **for every other member**. A remedy
  must derive a canonical current configuration, separate "present and valid" from
  "authoritatively absent, recreate", and answer what a stale broker recreating the
  durable after a newer owner removed the membership does. This is why F-050 does not
  claim to prove an end-to-end delivery path. - [ ] **The 3.1.1 forced leave's
  post-CONNACK destructive exit.** Also surfaced by F-050's review, and deliberately not
  folded into it. The forced leave removes a resuming 3.1.1 client's persisted shared
  memberships and releases their group consumers, and it runs AFTER the success CONNACK;
  its save failure returns an error there, so the client holds an acknowledged session and
  a closed connection. Moving it before the CONNACK was tried in F-050's plan and
  refuted: it converts an UNACKNOWLEDGED CONNECT attempt into a permanent destructive
  mutation of session state, which is strictly worse. It is none of F-050's five restore
  exits. - [ ] **Generalise the pre-CONNACK refusal signal.** F-050 tells a v5 client why
  its restoration refused the CONNECT (0x83, naming the filter), because a permanently
  unrestorable filter would otherwise be an endless silent close with no hint that a clean
  start is the recovery. The other pre-CONNACK refusals — register recovery, the
  reconciliation verdicts, the lifetime grant — still close silently. **F-061 does not own
  this**: it is specifically the authorization path, and F-050's review corrected an
  earlier draft that attributed general failure signalling to it. **Small.** - [x] [F-065:
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
  indeterminate, is bounded by ordinary teardown, a later orphan-sweep consumer
  deletion, or the gate's superseded-incarnation disposal, and is otherwise retained.
  Stated in `docs/CONFORMANCE.md`. Also refuted and recorded so none returns: a
  `storing` phase marker, an owner-independent disposal on claim absence, a reaper-side
  cleanup, and two compensation designs keyed on which CAS failed. - [ ] [F-066:
  Generalise cross-stream identity coupling](findings/ha-arbitration.md#f-066). **Alpha
  blocker.** Falsified premise: *recreating one stream is a self-contained recovery
  procedure*. Durable records persist identities other streams issue — sequences,
  consumer durables, subjects — and `stopAll` names one stream at a time. The dossier
  carries a **verified sample of cross-stream identities — explicitly NOT a census.**
  Four attempts to enumerate the population were each falsified by review, every one a
  filter masquerading as a sweep, so the completeness claim is dropped and the
  enumeration was deferred to F-034, which owns `$MQTT_inflight`. **F-034 landed it on
  2026-08-22** — see [the persisted-identity
  census](findings/persisted-identity-census.md) — so the population is now enumerated
  rather than sampled. That does **not** discharge this finding's completeness deferral:
  the census's field-level span is mechanically enforced, but its interpretation-surface
  half is a drift GUARD over a hand-maintained root set, keyed by unqualified function
  name, so it catches real drift without proving closure. What the sample establishes:
  **three** epoch couplings exist (`$MQTT_teardown` via stamped stream metadata,
  `$MQTT_willq` twice via `OriginSessEpoch` and `ScanEpoch`), all against `$MQTT_sess`,
  and the pattern was never generalised — not even across `$MQTT_sess` itself, since
  `$MQTT_inflight`'s `ClaimSeq` references it and is uncoupled. Every confirmed
  reference to `$MQTT_msgs`, `$MQTT_out`, `$MQTT_qos2assign` and `$MQTT_inflight` is
  uncoupled. The remedy is a generalisation, not a fourth bespoke coupling. On the
  affine path the VERIFIED AFFECTED SUBSET — not a proven closed unit, and F-034's
  census does not settle that either, for the reason above — is **four** streams —
  `$MQTT_msgs`, `$MQTT_qos2assign`, `$MQTT_out`, `$MQTT_inflight` — and recreating a
  proper subset loses accepted messages, delivers an **OLD payload** for a new message,
  or NAK-loops orphaned copies forever. Per-subject CAS cannot help; the semantic
  identity is what fails (`feedback_cas_generation_identity`). Rows outside the affine
  path are named, not audited here: `$MQTT_inflight`'s own coupling is F-034's. - [x]
  [F-034: Add write lineage to outbound in-flight
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
  a real DEFECT and an alpha blocker**, not a fixture premise — though **explicitly not
  an MQTT conformance violation**: the specification permits reordering on a shared
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
  the binding that does is the register's own source. - [x] **Slice A of the arbitration
  design: the session-record watch.** Ownership changes only by a write to the client's
  record subject, so the observation channel is now a per-broker ephemeral consumer on
  `$MQTT_sess` instead of the at-most-once `$MQTT.JSA.*.SP.*.*` notification, and a
  quiet session still costs no write per interval. The watch is lossy by construction,
  so convergence rests on the PAIR: a committed-read backstop on the periodic pass
  re-reads each resident session's record, feeds the same trigger when it MOVED, arms
  the ownership marker when it is GONE (the shape no sequence comparison can see), and
  re-fires any trigger a refusal left undecided — which also closes "a refused eviction
  can be lost entirely". The eviction worker gained a digest arm so a watch delivering
  this broker's OWN ambiguous landing can never evict the rightful client over its own
  write, and refuses equally when an unusable load leaves nothing to compare. Residuals
  in `docs/CONFORMANCE.md`. - [ ] [F-036: Revalidate quiet resident-session
  ownership](findings/ha-arbitration.md#f-036). **Was: very large; now the remainder of
  one shape.** Slice A above converges every MOVED record, bound sessions included. What
  is left is the DELETED record on a BOUND session: arming the ownership marker hands it
  to the record-rejection settlement, whose unbound half refuses while a client is
  attached and whose bound half runs pre-dispatch on that connection's own readLoop — so
  a client that negotiated Keep Alive 0 and then goes silent still holds a session
  another broker tore down, which is precisely the failure this finding names. Closing
  it needs a bound-session disposal that does not depend on future client traffic, and
  that fix has to answer why taking the shared client-ID gate for a bound session is
  safe here when it is not for a concurrent SUBSCRIBE (`docs/ARCHITECTURE.md`, "A
  definite record-CAS rejection is outstanding work"). Found by the Slice A holistic
  review. **Scope WIDENED 2026-08-17 by F-032's audit, from the absent-record tail to
  "the previous owner keeps serving, or is misread as gone, after ownership moved".**
  Two manifestations were added, both consequences of this finding's own condition
  rather than new findings: (1) `arbitrateQoS2ReleaseClaims` enumerates hold subjects
  ONCE, so a hold on a subject ABSENT from that snapshot is never arbitrated, and a peer
  that has not yet observed the ownership marker delivers it **after the session was
  discarded** — a delivery that should not have happened, not a loss, and deliberately
  not something F-062's purge cutoff addresses. That cutoff, now landed, WIDENS this
  manifestation: a hold enumerated but above the watermark is spared too, so the
  population that survives a teardown and stays serviceable by the peer includes those
  as well as never-enumerated ones. (A replacement on an already-enumerated subject used
  to be reached by name and destroyed — F-062 mechanism 1 — and is now spared, which
  removes the loss and leaves this direction.); (2) the orphan sweep runs under no
  teardown claim and infers liveness from this broker's residents plus persisted records
  only, so a live remote generation whose record is absent is invisible and its hold is
  condemned — including when a bridge-authored teardown was what deleted that record. A
  fix must answer both, not only the silent-Keep-Alive-0 case, and should stage the
  post-enumeration ordering explicitly. **`RWID` may be the predicate this needs.** The
  retirement watermark's `RWID` is now the only durable statement of *"this specific
  record was retired"* — record-absence is ambiguous, and the generation test needs a
  surviving record to compare, whereas `ackWID == RWID` is positive proof that does not
  wait for client traffic. That is an unwritten design, not a promise. **If this item
  turns out NOT to need it, and neither does "give a vanished record its own disposal
  vocabulary" below, then whoever closes the LATER of the two should CONSIDER removing
  `RWID`** — the field, its validator row, and the valid arm of the record-absent create
  re-pointed at the session-interval register identity, which can serve it. The
  consideration is what is prescribed, not the removal. **Updated by the write-lineage
  audit**: the create's valid arm no longer reads `RWID` at all, and the
  session-interval register that sentence offered as the alternative no longer exists,
  so the field now has **no reader** — this item and the vanished-record one are the
  only candidates left. The default is still to keep it until the evidence is in:
  deleting one unread field is small, and re-adding a required field to a permanent
  record is not. - [ ] Make ephemeral-consumer cleanup guaranteed rather than
  best-effort. A failed manager initialization now rolls back both per-broker ephemeral
  consumers, including a create whose response was lost, so retries no longer ACCUMULATE
  them. What is left is the rollback delete itself failing or being lost: the stray
  consumer is then reaped by its five-minute inactivity threshold, which bounds lifetime
  rather than cardinality. **Small.** Close it with one unguessable process identity
  that derives a stable consumer name AND delivery subject per account/purpose, so a
  retry repeats the same consumer configuration. Add consumer metadata for owner,
  purpose, process instance and schema; `InactiveThreshold` remains crash cleanup. Do
  not use a user-configurable node name that two live brokers could share. See the
  [platform
  audit](findings/nats-platform.md#stable-ephemeral-consumer-identities-and-metadata). -
  [ ] Give a vanished record its own disposal vocabulary. The deleted-record arm reports
  0x8E "session taken over by a new connection", which is right about the ownership fact
  and wrong about the cause when the deletion was expiry-driven: no new connection need
  exist. **Small.** A third `mqttDisposalVocabulary` value, not a new verdict. **This is
  the second of the two items that decide `RWID`'s future**, and for the same reason:
  telling an expiry-driven vanishing apart from a takeover needs positive evidence that
  THIS record was retired, which is what `RWID` states. If neither this item nor F-036
  above needs that predicate, whoever closes the later of the two should **CONSIDER
  removing `RWID`** — see the note on F-036 for what that involves and why the default
  until then is to keep it.

### 0.3 Confirmed alpha blockers

These are concrete correctness, data-loss, race, or liveness defects in features
the README currently reports as supported. They remain blockers independently of
the HA-scope decision except where the dossier explicitly requires a remote
takeover.

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
- [ ] [F-043: Fix the `removeSessFromFlappers` data race](findings/runtime-correctness.md#f-043).
  **Small to medium.** Reproduce both call sites and verify lock ordering first.
- [ ] [F-072: Derive the outgoing DUP flag from MQTT retransmission](findings/release-investigations.md#f-072).
  **Alpha blocker.** `trackPublish` sets DUP from the JetStream delivery count
  (`dcount > 1`) and that value reaches the wire through `mqttPubFlagDup`. MQTT-3.3.1-3
  ties the outgoing DUP flag to whether **the outgoing PUBLISH** is a retransmission, and
  a copy offered then refused before `trackPublish` produced no PUBLISH at all — so the next
  offer is still a FIRST transmission and must carry DUP=0. F-071's single-publisher capture
  shows seven of eight messages arriving with DUP=1 on their first client-visible PUBLISH.
  Found by review of that capture; I had recorded the same observation as "legitimate",
  which was a conclusion about the protocol drawn from what the implementation does.
  The fix tracks whether THIS allocation has already put a PUBLISH on the wire.
  **TWO deterministic tests are required, and the second is what forces a durable fix.**
  (1) a pre-`trackPublish` claim/materialization refusal, then DUP=0 on the first PUBLISH;
  (2) a seam after `storeInflightRegister` confirms and before `mqttEnqueuePublishMsgTo`
  runs, then restart, recover, re-offer, and DUP=0 on the first client-visible PUBLISH.
  Test (1) alone is satisfied by transient state while the crash path still emits DUP=1, so
  a fix passing only (1) has NOT closed this. Neither may be load-sensitive.
- [ ] [F-071: Preserve same-topic ordering for affine QoS 2 deliveries](findings/release-investigations.md#f-071).
  **Alpha blocker, and a defect rather than a flake — but NOT a spec violation, and the
  basis is weaker than three earlier drafts of this bullet claimed.** MQTT-4.6.0-5 is
  conditional on an *Ordered Topic*, MQTT-4.6.0-6 covers only **Non-shared** Subscriptions,
  and §4.8.2 does not extend it, so the specification permits reordering here.
  `docs/CONFORMANCE.md` makes no global ordering promise either. What exists is a **stated
  implementation property** in `docs/PORTING.md` — "each topic-filter+QoS stream rides a
  single ordered carrier ... on a single dispatch goroutine" — which
  sits under *Testing strategy* justifying why three upstream ordering tests were REWRITTEN,
  and is therefore load-bearing for them. The affine path breaks that mechanism by delivering
  copies through a `$MQTT_Q2AFF_*` consumer on `$MQTT_out`: a SECOND carrier. So the blocker
  status rests on that property plus the judgement that silently reordering one client's
  same-topic QoS 2 messages is unacceptable here regardless of the spec. **Disagreeing with
  that judgement means reclassifying to a post-alpha product-path anomaly — not
  weakening the assertion that found it.**
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
  **Do not weaken the ordering assertion — STRENGTHEN it.** `TestQ2AffineOrderingPreserved`
  publishes from a fresh Client per message, so it asserts an ordering the spec does not
  require and would catch a single-publisher violation only by coincidence; a correct fixture
  publishes from one Client. What is also owed is instrumentation correlating source
  sequence, copy sequence, claim phase, callback entry and enqueue.
- [ ] [F-035: Bound every courtesy DISCONNECT write](findings/runtime-correctness.md#f-035).
  **Medium.** One watchdog must cover mutex acquisition and socket writing at
  every server-initiated DISCONNECT site.
- [ ] [F-041: Persist the MQTT 5 DISCONNECT expiry override](findings/session-and-will.md#f-041).
  **Medium.** The durable record and resume decision must agree even after an
  ambiguous write.
- [ ] [F-039: Restore shared membership during SUBSCRIBE rollback](findings/runtime-correctness.md#f-039).
  **Small.** Independent of the F-030 term-fence work: this is a missing in-memory
  `sharedSubs` restoration, not a JetStream CAS failure.
- [ ] [F-045: Release stranded shared pending state at membership removal](findings/runtime-correctness.md#f-045).
  **Medium.**
- [x] [F-050: Refuse or retry an incomplete shared-subscription restore](findings/shared-qos2.md#f-050).
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
  while the DUP derivation is F-072's**, the same marker is lost for a shared durable even
  without that race because the ordinary close purges it, a timed-out affine wait falls
  into F-072's population, and the displaced-broker window has no upper bound. **A
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
- [ ] [F-028: Preserve deferred disconnect state during foreign disposition](findings/session-and-will.md#f-028).
  **Medium.** Split the parked-disconnect loss and retained-continuation cleanup
  into separately testable changes.
- [ ] [F-037: Make the rejected finite-disconnect stamp crash-safe](findings/session-and-will.md#f-037).
  **Medium.**
- [ ] [F-038: Prevent takeover races from publishing or dropping the wrong Will](findings/session-and-will.md#f-038).
  **Large.** Treat this as durable Will-intent/outbox design, not a local close-path
  patch.
- [ ] [F-047: Keep affine assignments when a SUBSCRIBE packet fails](findings/shared-qos2.md#f-047).
  **Medium.**
- [ ] [F-048: Arbitrate affine cleanup before discarding assignments](findings/shared-qos2.md#f-048).
  **Medium to large.** Design and test F-047 and F-048 as one transactional-cleanup
  cluster.
- [ ] [F-059: Arbitrate retained-index application by stream sequence](findings/runtime-correctness.md#f-059).
  **Medium.** The volatile retained index applies stream events and local seeds
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
- [ ] [F-060: Report a failed retained store to the publisher](findings/runtime-correctness.md#f-060).
  **Small to medium.** A PUBLISH whose RETAIN side failed to store is reported as
  successful, contrary to [MQTT-3.3.1-5]. Decide the contract for QoS 0 —
  which has no acknowledgement to withhold — alongside QoS 1 and 2.
- [ ] **Reclaim CONNECT acceptance facts, or bound them.** **Medium.** Every accepted
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
  wrong.** Within ONE incarnation a fact at stream sequence `S` can only ever qualify
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

### 0.4 Release-verification investigations

These entries preserve observations without pretending the implementation scope is
known. Capture complete logs, obtain a deterministic reproduction or identify the
failed test, and then either close the investigation or create a new confirmed
finding with acceptance criteria.

- [ ] [F-040: Reproduce reconnect versus DISCONNECT expiry-zero](findings/session-and-will.md#f-040).
  **Investigation.** A confirmed mechanism is a protocol blocker.
- [ ] [F-042: Identify the unexplained full-suite failure](findings/release-investigations.md#f-042).
  **Investigation.** Do not truncate suite output.
- [ ] [F-044: Determine whether the phase-2 shutdown observation is real](findings/release-investigations.md#f-044).
  **Investigation.**
- [ ] End-to-end phase-aware lease ownership for the delayed-Will publish worker.
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
- [ ] Make `TestQ2AffineResumeDoesNotRechargeAckBudget`'s reconnect loop tolerate a closed
  connection. **Test infrastructure, not a broker gap, and MEASURED as pre-existing.**
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

- [ ] Cover retained convergence for a sourced `$MQTT_rmsgs` topology.
  **Investigation.** `docs/ARCHITECTURE.md` declares a cross-account sourced
  retained stream a supported topology, but the only test of it lives in the
  unbuilt `mqttport_clustertests` reference file, so nothing running exercises a
  record arriving through a source with a subject transform. Establish whether a
  native test can stand it up; if it can, the test is release verification. Include
  TTL expiry and any F-059 delete marker through the source: source/mirror marker and
  TTL behavior differs, so the unsourced test is not a substitute.

### 0.5 Post-alpha candidates and scope cleanup

These do not block the alpha unless investigation shows that ordinary broker
operation can trigger correctness loss or unbounded resource growth.

- [ ] [F-046: Split the in-flight stabilization follow-ups](findings/inflight-followups.md#f-046).
  Decide the compatibility-sensitive schema naming before alpha; schedule the
  stale-metadata cleanup and settlement-fence work independently.
- [ ] [F-061: Send a CONNACK before closing an unauthorized CONNECT](findings/runtime-correctness.md#f-061).
  **Small.** The shim's `authViolation` only closes the socket, so a client whose
  per-client NATS dial is refused never receives CONNACK 0x05 / 0x87 — while
  `mqtt.go` comments that the CONNACK is enqueued there and traces one as sent.
  MQTT 3.1.1 [3.1.4] makes the close a MUST (met) and the CONNACK a SHOULD (not
  met); verify the v5 wording when planning it. Found while reading this path for
  F-051, not caused by it. Both conformance suites pass today (124/124, 106/106),
  which is why it is a post-alpha candidate rather than a blocker; promote it if a
  suite or an operator report contradicts that.
- [ ] [F-049: Provide operator recovery for unreadable affine claims](findings/shared-qos2.md#f-049).
  Keep basic diagnostics in alpha scope; automatic quarantine or repair may follow
  alpha if broker-owned state cannot create the malformed record.
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
  What is left is not enforceable and is documented instead: recreating `$MQTT_sess`
  discards every client's acceptance history along with the records, and a delayed Will
  enqueued beforehand then fires (`docs/CONFORMANCE.md`).

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
- [ ] Introduce a narrow JetStream transport boundary and migrate ordinary
  stream/consumer CRUD, info, publish-ack and double-ack operations incrementally to
  `nats.go/jetstream`. Preserve per-client authorization connections and a thin raw
  adapter for atomic-batch framing and unsupported endpoints. Spike ordered consumers
  for the two observation consumers only after the session-lineage deletion audit
  (below), and
  adopt them only if failure semantics and measurements improve. See the
  [platform audit](findings/nats-platform.md#staged-use-of-the-high-level-jetstream-client).

### Dead-code and reachability cleanup

Each checkbox below is intentionally scoped as one reviewable commit. Complete
them in order where a later lint change depends on an earlier cleanup.

- [ ] [F-052: Connect MQTT option validation to production startup](findings/dead-code.md#f-052).
  Adapt or retire the upstream validator, and make its tests exercise the real
  standalone startup boundary.
- [x] [F-053: Remove independent dead declarations](findings/resolved.md#f-053).
  Delete the unreferenced constants, errors, leaf wrappers, singular shared-holder
  helper, superseded no-sequence message lookup, and the two orphaned JetStream-name
  compatibility helpers found by the final reference scan. No live call site changed.
- [x] [F-053: Remove obsolete upstream integration clusters](findings/resolved.md#f-053).
  Delete the unused configuration-reload, embedded-routing, and retained-permission
  rescan paths with the imports and shim state they strand. The standalone broker
  now carries no dormant embedded-server account/reload mechanism.
- [ ] [F-054: Move test-only helper APIs out of production files](findings/dead-code.md#f-054).
  Keep live hook points, but place tagged-test setters and convenience methods in
  the test build where possible.
- [ ] [F-054: Reconcile the constant-state v5 capability](findings/dead-code.md#f-054).
  Derive it from supported configuration or collapse its impossible false branch.
- [ ] [F-054: Isolate the parked WebSocket path](findings/dead-code.md#f-054).
  Keep the future transport milestone explicit without carrying an apparently
  selectable non-nil WebSocket path in current production flow.
- [ ] [F-054: Isolate upstream-only QoS compatibility switches](findings/dead-code.md#f-054).
  Keep reference-test behavior without presenting the switches as reachable
  production configuration.
- [ ] [F-055: Re-enable reachability lint for `mqtt.go`](findings/dead-code.md#f-055).
  Replace the file-wide exclusions with default- and full-suite analysis plus
  narrowly documented exceptions.

### Performance and resource audits

- [ ] Audit every JetStream query and request pattern inherited from the early
  NATS Server MQTT implementation against current JetStream APIs and behavior.
  Look for avoidable request/reply round trips, repeated stream or consumer
  lookups, serial operations that can be batched or run concurrently, full
  scans where direct lookups are available, and polling that can be replaced by
  push or event-driven behavior. Keep the leader-routed management GET for
  correctness-sensitive loads: Direct multi/batch Get lacks read-after-write
  coherence. The [platform audit](findings/nats-platform.md#deliberate-non-adoptions)
  records this and the pinned-consumer, KV/counter, pause/reset, and fast-batch
  non-adoptions so the audit starts from decisions rather than rediscovering them.
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

### NATS platform follow-ups

- [ ] Enable `js_ack_fc_v2` in clustered CI and prove the bridge's dual-layout ACK
  parser and ACLs accept the domain/account-aware subjects announced as the v2.15
  default. This is a compatibility test, not a production feature toggle.
- [ ] After alpha, write an ADR for or against a schedule-backed delayed-Will timer.
  A schedule may publish only into its own stream, so the ADR must preserve F-031's
  atomic `$MQTT_sess` record/acceptance fact and must not claim cross-stream
  cancellation. The broker, not the scheduler, remains responsible for MQTT publish
  semantics. See the
  [platform audit](findings/nats-platform.md#message-schedules-post-alpha-will-outbox-spike).

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
- [ ] Release artifacts use the final product name consistently.
