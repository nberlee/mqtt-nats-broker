# Roadmap

This document is the authoritative checklist for the first public alpha. Each entry
names its GitHub issue when it has one. A finding's analysis, evidence, rejected
approaches, review rounds and dispositions live in that issue (see the
[GitHub issues](https://github.com/nberlee/mqtt-nats-broker/issues?q=is%3Aissue), label
`finding`; the former dossiers are archived under label `audit`), not here. A checked
item is one line: what was done, its date and its issue when it has one.

Until every item classified as an alpha blocker is complete, do not push release
artifacts, publish container images or Helm charts, or create an alpha tag.
Conditional blockers become release blockers when their corresponding feature is
included in the supported alpha scope. Investigations block only when they confirm
a defect or leave a release-gate failure unexplained.

The sections are ordered by dependency. Within a section, implementation may
proceed in parallel where the work does not overlap. A new correctness, security,
interoperability or data-loss finding is filed as a GitHub issue first (its number is
the stable identifier, and the issue carries its release classification), and gets a
roadmap entry naming that issue before feature work continues.

## 0. Resolve known findings

- **[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) row 5,
  unit C: a checkpoint within the amended recovery boundary, accepted with
  [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) and
  [issue #108](https://github.com/nberlee/mqtt-nats-broker/issues/108) on 2026-10-08.**
  Unit A's retained-capacity wake remains preserved; unit B remains stopped.
  Generation retirement discharges old work without changing new allocations. The
  abandonment, disposal, recovery and delayed-completion rules are stated in
  `docs/ARCHITECTURE.md`, "A restored delivery denied on resume is abandoned".
  P-12 f6 remains an accepted residual: displaced incomplete recovery may require
  a later successful resume; returning storage alone does not restore progress.
  The original no-reconnect requirement remains unmet.

The remaining unit C follow-ups are **NONBLOCKING**:

- C76-WAKE-STRESS: combine timer, callback and shutdown final-pass contention
  beyond the single-flight and retained-owner checks.
- C76-SCOPE-SAMPLING: sample additional QoS, cold-recovery, expiry, oversize,
  affine and indeterminate entrances sharing the abandonment-exclusion boundary;
  the full cross-product is not claimed.
- T-C34 equivalent snapshots: add equivalent crash snapshots beyond the distinct
  intent, materialization, live-token, source-ACK and unreadable-outcome cuts.

- [x] [issue #113: Give every recovered reservation a lifecycle
  owner](https://github.com/nberlee/mqtt-nats-broker/issues/113): a complete successful
  resume releases absent recovered reservations under fences. The N-7.2 marker
  replacement licence is kept nonce-only.

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
fixed. [issue #51](https://github.com/nberlee/mqtt-nats-broker/issues/51),
[issue #52](https://github.com/nberlee/mqtt-nats-broker/issues/52),
[issue #53](https://github.com/nberlee/mqtt-nats-broker/issues/53),
[issue #68](https://github.com/nberlee/mqtt-nats-broker/issues/68),
[issue #74](https://github.com/nberlee/mqtt-nats-broker/issues/74) and
[issue #75](https://github.com/nberlee/mqtt-nats-broker/issues/75), plus any
affine-only branch of a wider
finding, are gathered under [the dedicated post-alpha chapter](#p-shared-subscription-qos-2-post-alpha--maybe-someday).
Other roadmap work is proved against the supported QoS-0/1 shared path and the ordinary
QoS-2 path. It may preserve the experimental path when cheap, but it must not grow scope,
reject a sound general fix, or fail an alpha gate merely to keep that path working. An
experimental regression is recorded in that chapter for the later investment decision.

- [x] **Enforce the alpha shared-subscription QoS ceiling.** MQTT 5 grants a shared QoS 2
  request QoS 1 (Maximum QoS and Shared Subscription Available unchanged; 3.1.1 treats
  `$share/...` as a literal filter). Shared QoS 2 sits behind a programmatic test option
  the production command does not expose; its affine suite needs the
  `mqttport_experimental_shared_qos2` tag and runs in a non-gating CI job. Its state must
  be reset before running with the gate disabled; detection, automatic downgrade,
  migration, enabling, disabling, upgrade, rollback, backup and restore are unsupported.

Closed findings are indexed in the
[finding dossiers](https://github.com/nberlee/mqtt-nats-broker/issues/144) and the
[resolved-findings
archive](https://github.com/nberlee/mqtt-nats-broker/issues/137);
[issue #29](https://github.com/nberlee/mqtt-nats-broker/issues/29),
[issue #31](https://github.com/nberlee/mqtt-nats-broker/issues/31),
[issue #34](https://github.com/nberlee/mqtt-nats-broker/issues/34) and
[issue #35](https://github.com/nberlee/mqtt-nats-broker/issues/35) remain
in the mixed-status [HA arbitration
dossier](https://github.com/nberlee/mqtt-nats-broker/issues/131) beside their
open follow-ups.

### 0.1 Remove pre-release persistence compatibility

Do these before further MQTT-core implementation. The broker has no released
persisted-state contract, so carrying these paths through subsequent session,
retained-message, and Will changes would make every change maintain and test
formats that no user release depends on. Each checkbox is one commit.

- [x] [issue #60: Remove inherited retained-message
compatibility](https://github.com/nberlee/mqtt-nats-broker/issues/60).
  The empty-body tombstone alone carries deletion; an undecodable record is rejected with
  a log and evicted.

- [x] [issue #62: Reject generationless delayed-Will records](https://github.com/nberlee/mqtt-nats-broker/issues/62).
  Every decoded `$MQTT_willq` record is validated and bound to its subject; a refused
  record is never claimed, published, cancelled or deleted and persists until an operator
  deletes the message. Each claim scan re-reads every refused record (`O(N)` gets per scan
  per broker), so a large enough backlog can push a scan past its interval and delay
  valid due Wills.

These cleanups do not remove MQTT wire compatibility or the operator migration
guide from NATS Server's embedded MQTT listener.

### 0.2 The supported HA scope: multi-frontend

Decided 2026-08-10: the first alpha supports multiple MQTT frontends against a
replicated JetStream cluster, keeping the README's horizontally scalable,
cross-instance HA promise. Every item in this subsection that is reachable in the
supported feature set is therefore an unconditional alpha blocker. The 2026-09-02
shared-QoS-2 scope decision is orthogonal: affine-only findings and branches are parked in
chapter P, while shared QoS 0/1 and ordinary QoS 2 remain subject to this HA bar. The
premise that stale stream-leader work could propose after a fast election is retired
([issue #34](https://github.com/nberlee/mqtt-nats-broker/issues/34),
closed 2026-08-16) by the `test/depproof/` route inventory and the NATS maintainer's
confirmation
([`nats-server#8469`](https://github.com/nats-io/nats-server/issues/8469#issuecomment-5304075553)).
No further lineage is to be added on the strength of the old premise without checking it
against that inventory.

- [x] Decide and document the first-alpha HA support level: multi-frontend HA stays in
  alpha scope. Decided 2026-08-10.

- [x] [issue #29: Repair a recognised session-record fork](https://github.com/nberlee/mqtt-nats-broker/issues/29):
  three-way merge of the record delta against the shared parent, re-asserted by the
  client's reconnect through the accept-save CAS; bounded (not shared-subscription
  membership), bounds and residual in `docs/CONFORMANCE.md`.

- [x] [issue #31: Arbitrate simultaneous takeovers from one
base](https://github.com/nberlee/mqtt-nats-broker/issues/31): every session-record
  write is conditional, base 0 included, with a bounded stale-base re-drive.

- [x] **The claim-won deletion window**
  ([issue #32](https://github.com/nberlee/mqtt-nats-broker/issues/32)) **and the
  PUBREL orphan class**: a permanent per-client generation-retirement watermark on
  `$MQTT_teardown`, epoch-coupled to `$MQTT_sess`, plus a PUBREL orphan sweep class.

- [x] **Sweep the rollout-qualified residuals out of the docs.** Rewritten without
  rollout qualification; a record pairing a QoS 2 `SharedSubs` entry with an empty
  `Q2AffGen`, or a shared QoS outside 0..2, is refused, and an affine-pair failure
  suppresses only the affine half of the orphan sweep.

- [x] **Cross-term create × create recognition**
  ([issue #33](https://github.com/nberlee/mqtt-nats-broker/issues/33)).
  Superseded 2026-08-16 by the write-lineage audit (residual R-create).

- [x] **A durable identity for an interval with no retirement history**
  ([issue #33](https://github.com/nberlee/mqtt-nats-broker/issues/33)'s R0).
  Superseded and deleted by the write-lineage audit; its refutations, in
  [issue #33](https://github.com/nberlee/mqtt-nats-broker/issues/33),
  bind any proposal of a durable interval identity.

- [x] [issue #34: Add write lineage to the Will outbox](https://github.com/nberlee/mqtt-nats-broker/issues/34).
  Closed 2026-08-16 by disproving the premise (`make depproof` canary, also a CI job on
  any change to `go.mod`, `go.sum` or the harness;
  [`nats-server#8469`](https://github.com/nats-io/nats-server/issues/8469#issuecomment-5304075553));
  post-start stream-configuration drift stays outside it.

- [x] **Audit the session write-lineage machinery for deletion, obligation by
  obligation.** The create side is deleted (a create is parentless, residual R-create),
  fork classification and repair are kept, and `$MQTT_origin` is refused at startup.

- [x] **Prove or retire the accepted-CONNECT atomic-batch write path.** Proved 2026-08-17:
  batch identity is carried in the stream entry encoding and enforced at apply, so a FIFO
  prefix split of the accept batch applies nothing (its two limits are in
  `docs/ARCHITECTURE.md`). Consequence audited the same day: the authored-record fork
  machinery is RETAINED, because an unconditional write to the record subject still
  reaches the forked verdict; the same-base boundary is stated once, in
  `docs/CONFORMANCE.md`, and a stale comment found later is an ordinary defect, not a
  reopening. [issue #34](https://github.com/nberlee/mqtt-nats-broker/issues/34).

  - [x] [issue #36: Add write lineage to inbound QoS 2
    state](https://github.com/nberlee/mqtt-nats-broker/issues/36). Audited 2026-08-17: no
    lineage was needed, the upstream contract is inherited by citation from
    [issue #34](https://github.com/nberlee/mqtt-nats-broker/issues/34), and the audit
    produced [issue #66](https://github.com/nberlee/mqtt-nats-broker/issues/66).

  - [x] [issue #66: Bound the inbound QoS 2 hold purge and the discard marker it
    leaves](https://github.com/nberlee/mqtt-nats-broker/issues/66). Done 2026-08-18: the
    hold purge takes a proven cutoff and has no unbounded form, the `discarded` marker
    carries a `Fence`, and the awaited arm never purges (a vacancy licenses only
    re-competition; removing that restores the loss). The per-write `0 < Fence <
    markerSeq` coherence check was designed and deleted; do not re-propose it.

  - [x] [issue #37: Add write lineage to shared QoS 2
    assignments](https://github.com/nberlee/mqtt-nats-broker/issues/37). Closed 2026-08-18
    as an audit: no lineage was needed; it produced
    [issue #67](https://github.com/nberlee/mqtt-nats-broker/issues/67),
    [issue #68](https://github.com/nberlee/mqtt-nats-broker/issues/68) and
    [issue #69](https://github.com/nberlee/mqtt-nats-broker/issues/69), and holistic
    review added [issue #70](https://github.com/nberlee/mqtt-nats-broker/issues/70).

  - [x] [issue #67: Prove a copy was retained before acknowledging its
    source](https://github.com/nberlee/mqtt-nats-broker/issues/67). Done: every route
    converges on one presence check and the source ack is authorized by
    `sourceAckAuthorized`; by explicit decision the guarantee is conditioned on a fresh
    but non-atomic observation, not bound or atomic.

  - [x] [issue #68: Reconcile assignments independently of owner
    resume](https://github.com/nberlee/mqtt-nats-broker/issues/68). PARKED, NOT FIXED:
    removed from the alpha gate by the 2026-09-02 shared-QoS-2 scope decision and gathered
    in chapter P, with on-behalf restoration and part of the accumulation half landed.
    Residual 8 (a `materialized` assignment whose copy is gone waits for its owner) and
    W-c (the nonterminal `claimed` population) stay open there; W-c's sweep-side
    settlement is refuted. This box is not an executable alpha item and later alpha lanes
    must not be redirected into it; the next research (which record states residual 8's
    population occupies) starts only after an explicit decision to invest in shared QoS 2.

  - [x] **Report a delivered shared QoS 2 message truthfully instead of as `Lost`.** Done
    2026-08-30, split out of
    [issue #68](https://github.com/nberlee/mqtt-nats-broker/issues/68): `PubRecProven`,
    proven by a stored PUBREL carrying a bound `(claim subject, copy subject)` pair, is
    the only fact that outranks `Lost`, and generic `AffineDone` deliberately does not. It
    does not close [issue #68](https://github.com/nberlee/mqtt-nats-broker/issues/68).

  - [x] **A saved shared filter that fails restoration at CONNECT is silently dropped.**
    Closed with [issue #54](https://github.com/nberlee/mqtt-nats-broker/issues/54), which
    is the same finding filed twice: restoration moved before the CONNACK, because failing
    the CONNECT after the CONNACK publishes the Will and can delete a zero-expiry session.

  - [x] [issue #78: Prove a shared group durable exists before restoring its
    membership](https://github.com/nberlee/mqtt-nats-broker/issues/78). Done 2026-08-23:
    the restore classifies each group durable before and after `processSubs`. That is
    configuration evidence, never ownership evidence, which
    [issue #79](https://github.com/nberlee/mqtt-nats-broker/issues/79) supplies.

  - [x] [issue #79: Arbitrate ownership after any shared group durable
    create](https://github.com/nberlee/mqtt-nats-broker/issues/79). Done: a CONNECT that
    installed, repaired or adopted shared interest proves ownership with one classified
    record write, `proveSharedMembershipOwnership` (the trigger is restored interest, not
    a create), and an authoritatively absent durable is recreated;
    [issue #80](https://github.com/nberlee/mqtt-nats-broker/issues/80) closed the live
    half.

  - [x] [issue #80: Arbitrate a live SUBSCRIBE's shared interest before it
    serves](https://github.com/nberlee/mqtt-nats-broker/issues/80). Done: a live SUBSCRIBE
    proves membership ownership before anything it touched serves; its QoS 1/2, PUBREL and
    affine deliveries wait, shared QoS 0 is dropped, and plain QoS 0 is deliberately
    exempt. Follow-up coverage accepted as non-blocking and not claimed done: the four ack
    arms at the ownership-CAS boundary, the resume fall-through chain, a shared QoS 0 drop
    (needs a v5 fixture), PUBREL and affine inside the window, a mixed packet, disposal
    ordering with an exactly-one-DISCONNECT oracle, forked-keeps-the-Will in both forms,
    and the eventual-Foreign arm (an ambiguous proof the tail resolves to Foreign), which
    needs a second broker, so the single-broker fixture reaches the unclassified arm only.
    A replicated second-broker oracle (a rejected ownership CAS, Foreign disposes) and a
    single-broker oracle (the unclassified arm) landed.

  - [x] **`sharedDurablesHeldByOthers` fails OPEN on an undecodable record.** Done; the
    holder scan was since deleted by
    [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107)'s shared-group
    lifecycle transfer (`9530f86`, 2026-09-22). A group consumer's fate is the group
    record's arbitrated holder count on `$MQTT_shared`, a durable is withheld where no
    readable, valid lifecycle authority licenses its deletion, and the undecodable-record
    refusal stands at the five remaining `$MQTT_sess` ingresses.

  - [x] [issue #84: Release a failed shared filter's own state independently of the
    last-holder answer](https://github.com/nberlee/mqtt-nats-broker/issues/84). Done: a
    failed shared filter discharges its own state, keyed on the durable and deliver
    subject rather than `sess.cons`, unless a surviving membership still owns the durable.
    Since [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107)'s transfer
    the consumer goes to the group record's verdict execution.

  - [x] **Release a shared UNSUBSCRIBE's and a SUBSCRIBE rollback's own state
    independently of the last-holder answer**
    ([issue #85](https://github.com/nberlee/mqtt-nats-broker/issues/85)). Done, widened to
    the SUBSCRIBE rollback: the fence asks whether a membership that survives the
    operation still reaches the durable, over an explicit `mqttSharedSurvivors` set, and
    nothing may discriminate a shared sid from a plain one lexically. The two planned
    resolver-continuation tests were not written, so the surviving-key premise across the
    ownership gate is argued, not tested.

  - [x] **The 3.1.1 forced leave's post-CONNACK destructive exit.** Done: the leave stays
    after the CONNACK (moving it before stays refuted) and is now a convergence obligation
    that continues only on proof, which also closes the target mismatch
    [issue #85](https://github.com/nberlee/mqtt-nats-broker/issues/85) deferred and
    [issue #49](https://github.com/nberlee/mqtt-nats-broker/issues/49)'s forced-leave
    instance.

  - [x] **Generalise the pre-CONNACK refusal signal.** Done for all three disposing sites
    in the post-accept-save, pre-success-CONNACK window; earlier exits still refuse
    without a CONNACK. A future change must not undo it: the CONNACK never reads a
    disposal vocabulary, `preConnackRefusalFor` has no `default`, and the disposal is the
    caller's so the signal can precede it.

  - [x] [issue #86: Prove a successor before claiming a
    takeover](https://github.com/nberlee/mqtt-nats-broker/issues/86). Done 2026-08-27: no
    persisted shape proves a successor, so `mqttDisposalTakenOver` is reachable only from
    the in-process same-client-ID replacement, and every record-derived route reports
    `mqttDisposalOwnershipUnsettled` (`0x89`), which mints nothing. A future change must
    not undo the single reader, the explicit fork bit, the Will action chosen by licence
    rather than by vocabulary, phase 2's position, and the absence of a re-park.

  - [x] [issue #69: Re-read copy absence after the reaper's own SourceAcked
    transition](https://github.com/nberlee/mqtt-nats-broker/issues/69). Done: the reaper
    reads the copy after its own last write, and a claim's incarnation nonce scopes its
    copy; the guarantee is re-scoped, since the copy withdrawal is a compensation, not an
    arbitration.

  - [x] [issue #70: Make the ten broker-owned streams one recreation
    unit](https://github.com/nberlee/mqtt-nats-broker/issues/70). Done 2026-09-01 under
    the 2026-08-31 owner ruling: a count preflight refuses a partial bundle and a
    per-stream epoch stamp refuses a member from another incarnation, so recreating one
    stream is refused. The by-construction generalisation stays abandoned, and the
    table-callback ordering is a fact of the code, read not guarded.

  - [x] [issue #38: Add write lineage to outbound in-flight
    registers](https://github.com/nberlee/mqtt-nats-broker/issues/38). Closed 2026-08-22
    as an audit plus a census: no lineage is owed; the drain release deletes by
    identity-conditioned exact sequence and subject, the teardown reap is bounded by its
    enumeration watermark, and the census is [the persisted-identity
    census](https://github.com/nberlee/mqtt-nats-broker/issues/134).
    [issue #70](https://github.com/nberlee/mqtt-nats-broker/issues/70)'s completeness
    deferral is not discharged. It produced
    [issue #71](https://github.com/nberlee/mqtt-nats-broker/issues/71) through
    [issue #76](https://github.com/nberlee/mqtt-nats-broker/issues/76), and its live-owner
    residue went to [issue #40](https://github.com/nberlee/mqtt-nats-broker/issues/40).

  - [x] **Slice A of the arbitration design: the session-record watch.** Done: a
    per-broker ephemeral consumer on `$MQTT_sess` observes a moved record (answered
    `0x89`, not `0x8E`), and convergence rests on the pair with a committed-read backstop
    on the periodic pass. Residuals in `docs/CONFORMANCE.md`.

  - [x] [issue #40: Revalidate quiet resident-session
    ownership](https://github.com/nberlee/mqtt-nats-broker/issues/40). Done 2026-09-03 in
    two units: the incarnation completion lease (`226e5f8`, 2026-09-02), then the
    two-dimensional PUBREL classifier, the licensed retirement of a bound session whose
    record was deleted, and a sweep that retains live session-generation state without a
    matching retirement watermark. `discardAbsentWinnerClaim` is deliberately ungated
    under the supported-scope ruling, and recording `Q2AffGen` in the watermark is
    refused. Any change that moves a detach earlier must re-check every predicate that
    reads `sess.c` to decide liveness.

  - [x] Make ephemeral-consumer cleanup guaranteed rather than best-effort. Done: both
    per-broker ephemeral consumers derive their name and delivery subject from a
    per-`Server`-run identity, and a rollback delete is licensed by a confirmed create
    only. See "Per-run ephemeral consumer identity" in ARCHITECTURE.md; residuals in
    CONFORMANCE.md and the [platform
    audit](https://github.com/nberlee/mqtt-nats-broker/issues/132).

  - [x] Give a vanished record its own disposal vocabulary. Closed as refuted 2026-08-29,
    and `RWID` was removed: no conjunction of surviving durable fields is
    occurrence-exact, and the exact reader (a claimed predecessor sequence on the marker
    and watermark) was refused as disproportionate, not as blocked. A future exact reader
    needs `RetiredSeq` rather than a restored `RWID`.

### 0.3 Confirmed alpha blockers

These are concrete correctness, data-loss, race, or liveness defects in features
the README currently reports as supported. They remain blockers independently of
the HA-scope decision except where the dossier explicitly requires a remote
takeover.

**Filing rule (2026-10-12 convergence triage).** A pre-existing finding met by another
item's work does NOT open a new box in §0.3 or §0.4; every such box sends document-order
selection backwards. It goes to its destination instead: an accepted behavioral gap to a
stated residual in `docs/CONFORMANCE.md`, a verification observation to its own GitHub
issue with a one-line box under §2 "Intermittent reds", an audit to §2, with a one-line note
in the item that met it. The exceptions are Will or message LOSS
on a common path, data corruption, and security, which get a §0.3 box. A finding whose
dangerous mechanism is still unexcluded stays attached to its existing investigation until
it is classified: "pre-existing" establishes provenance, not safety. Accepting a residual
never licenses changing a test assertion.

- [x] [issue #81: A denied publish is accepted, stored and
  delivered](https://github.com/nberlee/mqtt-nats-broker/issues/81): one
  `$SYS.REQ.USER.INFO` snapshot on the client's own connection at CONNECT is authorized
  against every effective subject a publication creates before its first effect; a
  denial is PUBACK/PUBREC `0x87` on MQTT 5 and a closed connection on 3.1.1. Service-owned
  paths apply a live ACL change only after MQTT reconnect. Security prerequisite the
  broker cannot check: an MQTT identity must be denied publish on `$MQTT.>`.

- [x] [issue #82: The authenticated identity's NATS account is never
  bound](https://github.com/nberlee/mqtt-nats-broker/issues/82): the identity's account
  must equal the service connection's, fetched before the listener opens; anything unknown
  refuses, an absent permissions object means unrestricted, and a NATS disconnect
  invalidates the snapshot and closes the MQTT connection.

- [x] [issue #83: A subscribe deny overlapped by a wildcard is not enforced on
  delivery](https://github.com/nberlee/mqtt-nats-broker/issues/83): service-owned QoS 1/2,
  retained and affine deliveries enforce the snapshot's subscribe denies, queue-aware. An
  indeterminate verdict withholds without acknowledging; a proven deny is acked terminally
  on a plain subscription, handed back on the paced refusal backoff by a shared group, and
  recorded `AffineDone` and acked on the affine route. A live ACL change applies there
  only after MQTT reconnect.

- [x] [issue #55: Bound a connection that never sends CONNECT](https://github.com/nberlee/mqtt-nats-broker/issues/55).
  An unconditional CONNECT deadline bounds every accepted connection; `-connect-timeout` /
  `MQTTNB_CONNECT_TIMEOUT` (default 5s) is refused at `<= 0`.

- [x] [issue #47: Fix the `removeSessFromFlappers` data race](https://github.com/nberlee/mqtt-nats-broker/issues/47).
  The removal holds `asm.mu` and runs only while the close still owns the session; the
  gate-less route leaves the jail alone.

- [x] [issue #89: Rebuild every in-flight register rewrite from the authoritative
  reread](https://github.com/nberlee/mqtt-nats-broker/issues/89). Done 2026-08-30: a
  retargeted CAS starts from the record it just read and mutates only the fields the
  operation owns, and `Retired` dominates `Cancelled` at every reader.

- [x] [issue #91: A PUBREL transmission must hold its packet identifier until its
  PUBCOMP](https://github.com/nberlee/mqtt-nats-broker/issues/91). Done 2026-08-30: the
  send registry holds the identifier until the write has returned and every PUBREL sent
  under it has been PUBCOMPed; presence in the registry is not a reservation.

- [x] **[issue #93](https://github.com/nberlee/mqtt-nats-broker/issues/93): a
  retained QoS 1/2 send does not reserve its packet identifier.** Done 2026-08-31; since
  [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) 7c a retained
  QoS 1/2 delivery is an admitted exchange with its own register, so its identifier
  survives a restart. Named, not absorbed:
  a retained frame on a rolled-back subscription keeps its `pendingPublish` entry until
  the next resume, and the `trackPublishRetained() == 0` downgrade delivers at QoS 0 where
  [MQTT-3.8.4-6] / Spec5 [3.8.4-8] require the delivered QoS to EQUAL min(published,
  granted).

- [x] **[issue #94](https://github.com/nberlee/mqtt-nats-broker/issues/94): a
  stale stored PUBREL can be transmitted under an identifier another
  exchange owns.** Closed 2026-09-05: no slot-backed PUBREL is transmitted without first
  atomically acquiring a non-revocable action on its owner slot (`action CAS -> proof ->
  socket write`), and a record naming a different exchange is refused, never
  acknowledged. An abandoned action holds its identifier for unbounded time, with no
  lease, TTL or steal (`docs/CONFORMANCE.md`). Scope ruling 2026-09-02: the fix closes the
  ordinary, retained, drain and supported recovery paths; an affine-only failure goes to
  chapter P. A `Claim` with no `ClaimSeq` is legal (a zero sequence loses every ordering
  contest); only a sequence with no claimant is refused.

- [x] **[issue #95](https://github.com/nberlee/mqtt-nats-broker/issues/95): the
  UNSUBSCRIBE drain leaves a recovered PUBLISH entry reserving its
  identifier.** Closed 2026-09-05 with
  [issue #94](https://github.com/nberlee/mqtt-nats-broker/issues/94): the drain's ordered
  handoff installs the same-nonce PUBREL owner and the source suppression, then
  compare-deletes the PUBLISH entry and its guarded `cpending` mapping. The suppression is
  installed, as **already durable**, only when the acquired action's slot is past PUBREC
  (that slot is the durable statement); on the drain-intent path no retirement is
  installed: the drain state machine owns that source.

- [x] **[issue #96](https://github.com/nberlee/mqtt-nats-broker/issues/96):
  adopting a drain erases an unfinished past-PUBREC intent.** Done 2026-09-05: a
  past-PUBREC intent (`st.intent`, unqualified) holds the drain against adoption and, at
  fork repair, refuses the `live` and `absent` rows; a drain retires only when durably
  empty. It closes the adoption route only (residuals in `docs/CONFORMANCE.md`).

- [x] **Drain retirement does not prove disposal before releasing the sid.** Closed
  2026-09-06: `retireDrainLocked` releases the sid and closes the delivery path under one
  `sess.mu` hold. A delete reaching a consumer returned to service is closed only for this
  drain object within this resident session object's lifetime; the fork-sibling exposure
  is pre-existing (residuals in `docs/CONFORMANCE.md`). Design constraint: a
  session-scoped retired-**name** set is object-scoped in fact and cannot establish the
  fork guarantee.

- [x] **An adopted drain with no delivery subscription silently stops delivering.**
  Closed 2026-09-06 for delivery READINESS only: a QoS > 0 SUBSCRIBE meeting such a drain
  reclaims it on the SUBSCRIBE's own terms or answers the filter `0x80`. Not claimed:
  correct completion of every adopted exchange, any change to adoption's failure or
  persistence behavior, or atomicity across duplicate occurrences of one filter in a
  packet; its other obligations are split into three bullets of their own.

- [x] A CONNECT restore converts a QoS 0 membership over a live `Cons` entry into a drain
  before any teardown, so its outstanding exchanges complete. Done 2026-09-09. Accepted
  limitation, stated in `docs/CONFORMANCE.md`: a restore-converted drain that completes a
  QoS 1 exchange and then definitely fails the save removing its claim retains the claim,
  the durable consumer and the source message indefinitely.

- [x] An adopted durable is named continuously (`Drains`), so a failed filter no longer
  strands it and no sibling sweep deletes it; the CONNECT restore route is closed by the
  item above. Done 2026-09-09.

- [x] An exchange's QoS is a property of the exchange, carried on its pending entry and
  never recomputed from the subscription: a subscription's maximum is an input to the
  first transmission only ([MQTT-3.8.4-3], [MQTT-4.3.3-1], [MQTT-4.4.0-1]).

- [x] A QoS 1/2 → QoS 0 re-SUBSCRIBE converts the consumer into a drain rather than
  destroying outstanding exchanges; a stream-imposed consumer inactivity limit keeps the
  destructive teardown.

- [x] [issue #101: Recovery classification follows adopted slot state and ordered PUBREL
  evidence](https://github.com/nberlee/mqtt-nats-broker/issues/101): recovery routes
  every slot by the state of the body it adopted and reads per-exchange evidence after the
  register it judges.

- [x] [issue #102: Resend reconciliation frees Receive Maximum capacity without client
  completion](https://github.com/nberlee/mqtt-nats-broker/issues/102): source absence
  frees capacity only for an exchange proven unsent.

- [x] Two routes released a Receive Maximum charge without client completion; neither
  now releases on the broker's inability to continue (the teardown purge releases only a
  proven-unsent entry, a retained send releases at its abandonment). Done 2026-09-10
  ([issue #103](https://github.com/nberlee/mqtt-nats-broker/issues/103)).

- [x] A recovery reservation over a source that can never be re-offered is released when
  its unsent marker proves non-transmission. Done 2026-09-11
  ([issue #104](https://github.com/nberlee/mqtt-nats-broker/issues/104)).

- [x] [issue #100: No cross-broker authority over an incarnation's in-flight
  identifiers](https://github.com/nberlee/mqtt-nats-broker/issues/100): every release
  names its authority through one refusing table, and replacement is outside the release
  class; done over the supported boundary 2026-09-13. The experimental affine disposer's
  non-resident arm stays independently licensed and belongs to section P.
  Fixed on the way: [issue #105](https://github.com/nberlee/mqtt-nats-broker/issues/105)
  (a staged cancellation verdict admitted as persisted) and
  [issue #106](https://github.com/nberlee/mqtt-nats-broker/issues/106)'s verdict
  retirement after a delete that removed nothing.

- [x] Prompt reclamation of a held identifier exclusion: a bounded re-attempt of the same
  absence proof (`internal/mqtt/exclusion_reclaim.go`), with
  [issue #106](https://github.com/nberlee/mqtt-nats-broker/issues/106)'s reclamation half.
  Not an alpha blocker.

- [x] [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107):
  every supported outbound route (ordinary QoS 1/2, shared-group QoS 1, retained QoS 1/2)
  admits, acquires, acknowledges, restores and retires through the outbound obligation
  authority, so removing the last durable representation cannot leave a peer entitled to
  serve the obligation; "no peer entitled" is never reduced to "no peer observed alive".
  Accepted with
  [issue #108](https://github.com/nberlee/mqtt-nats-broker/issues/108) by the
  integrated gate on 2026-10-08. The affine cleanup family and its
  [issue #109](https://github.com/nberlee/mqtt-nats-broker/issues/109) premise remain
  chapter P's ("issue #107's affine completion work"). If enrolled production generations
  carrying an affine cleanup duty become reachable before issue #109 lands, this split
  stops being acceptable; clearing that population is chapter P's promotion item's
  business, not an alpha obligation.

**[issue #109](https://github.com/nberlee/mqtt-nats-broker/issues/109) was
relocated, unresolved, to chapter P on 2026-09-20**, under the ruling that
session-affine shared-subscription QoS 2 is not an alpha release feature. It is
experimental-only: not alpha-selectable, and not an alpha prerequisite. Its defect stands
and its evidence is preserved intact at
[issue #109, in chapter P](#issue-109-affine-claim-admission-is-fenced-only-in-memory).

- [x] [issue #108](https://github.com/nberlee/mqtt-nats-broker/issues/108):
  absorbed into [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107);
  accepted with it on 2026-10-08.

- [x] [issue #108](https://github.com/nberlee/mqtt-nats-broker/issues/108): the
  custody ledger is the one authority for capacity and identifier release: completion,
  occupancy and observation finished stay separate facts, neither occupancy nor the end of
  observation establishes completion, and identity checks limit which obligation a release
  affects but never supply release authority. Accepted cost: a peer-deleted register, or a
  delete with an ambiguous result, reserves one identifier and one Receive Maximum charge
  until the generation is discarded. Accepted with
  [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) on 2026-10-08.

- [x] [issue #99: Terminal-completion evidence and reconciliation across allocation
  replacement](https://github.com/nberlee/mqtt-nats-broker/issues/99): durable
  lineage-bound completion survives allocation replacement, never releases another
  exchange, and drives durable and memory convergence after finite storage failure without
  another offer or resume; reopening allocation after an incomplete recovery keeps its
  next-complete-resume bound (`docs/CONFORMANCE.md`). The lineage is correlation, not
  proof of one continuing obligation: inheritance alone authorizes no release, and an
  identifier is never released on an observed absence. Closed 2026-10-11 with
  [issue #90](https://github.com/nberlee/mqtt-nats-broker/issues/90).

- [x] **A closed-but-present delivery subscription under a live `cons[sid]`.** Closed by
  making a deferred QoS downgrade revocable within its packet (`docs/ARCHITECTURE.md` "A
  deferred QoS downgrade is revocable within its packet"; residuals in
  `docs/CONFORMANCE.md`).

- [x] **A destroyed QoS 0 downgrade left the record naming its consumer.** Closed by
  releasing the reference at the packet's final-update boundary; the builder's
  membership-presence predicate stays, since a positive-QoS predicate loses an adopted
  drain's live exchange (`docs/ARCHITECTURE.md` "UNSUBSCRIBE drain"; residuals in
  `docs/CONFORMANCE.md`).

- [x] **A quarantined packet identifier has no live-session release.** Closed 2026-10-11:
  [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) 7b deleted the
  quarantine for every class, so nothing is left to release; the three holds with no live
  release are stated in `docs/CONFORMANCE.md`. Side findings, not worked: the
  "Shared-register takeover arbitration excludes unreadable records" residual in
  `docs/CONFORMANCE.md` and the "LOST arbitration" passage in `docs/ARCHITECTURE.md` still
  describe the deleted resume claim contest, and mutation M26 no longer fails
  `TestMQTTInflightUnsupportedSharedShapeIsRefusedBeforeTheContest`.

- [x] [issue #76: Derive the outgoing DUP flag from MQTT
retransmission](https://github.com/nberlee/mqtt-nats-broker/issues/76).
  DUP is derived from transmission: the register and its `unsent` marker commit in one
  atomic batch, and the marker's confirmed delete records send intent before the socket
  write (present = proven-unsent, DUP=0; absent = possibly-attempted, DUP=1, identifier
  held). Done 2026-09-09.

- [x] [issue #90: A transmitted delivery's cancellation must not free its packet
  identifier](https://github.com/nberlee/mqtt-nats-broker/issues/90): a cancellation is
  not evidence of non-transmission; the `unsent` marker is the only such evidence, and
  late acknowledgements complete the exchange across resume. Closed 2026-10-11 with
  [issue #99](https://github.com/nberlee/mqtt-nats-broker/issues/99). Not changed here:
  `claimRecoveredInflight`'s reap of a slot-only tombstone on the `Cancelled` bit alone, a
  chapter P question.

- [x] [issue #121: Cancellation must survive pending-fence
  disposal](https://github.com/nberlee/mqtt-nats-broker/issues/121): an ordinary pending
  fence of the serving generation is disposed of only under a licence naming that
  generation and tenure, with no admission of its nonce whose ladder has not returned, and
  after a confirmed `fence-pending-disposal` authority advance. Done 2026-10-12 for the
  supported classes; the experimental affine remainder is chapter P's. P-14#1 stays
  orthogonal: `releaseCancelledReservation` retires the nonce verdict outside the
  settlement owner, on an exit that writes nothing; any design that keys verdict
  retirement on settlement must route it through that owner.

- [x] [issue #75: Preserve same-topic ordering for affine QoS 2
deliveries](https://github.com/nberlee/mqtt-nats-broker/issues/75).
  Parked, not fixed: post-alpha shared-QoS-2 work in chapter P (scope decision
  2026-09-02). Do not weaken `TestQ2AffineOrderingPreserved`; strengthen it to publish
  from one Client, and add instrumentation correlating source sequence, copy sequence,
  claim phase, callback entry and enqueue.

- [x] [issue #39: Bound every courtesy DISCONNECT
write](https://github.com/nberlee/mqtt-nats-broker/issues/39).
  `writeSocket` installs `mqttWriteDeadline` on every write and clears it before
  returning; this bounds only writes to a non-reading socket. T6 (gate-held routes through
  their producers) and the TCP half of T9 are deferred, not dropped.

- [x] Anchor a delayed Will's `FireAt` to the observed close, not to consumption (one
  connection field stamped by `mqttOweWill` on both owed arms and consumed with the Will),
  and stop `dischargeDeferredWill` diverting an already-due delayed Will around the
  acceptance-fact scan; the takeover half was split to the next item. Residuals in
  `docs/CONFORMANCE.md` under residual 8.

- [x] A takeover destroys only an ARMED Will: `mqttSettleWillOnTakeover` leaves an
  owed-now Will to the teardown and consumes an owed-delayed one into the outbox with its
  close-anchored deadline, and the deferred-disconnect park's take moved inside its
  commit. Done 2026-10-12. A disposal meeting both a parked action and a live client still
  completes only one of the two Wills:
  [issue #127](https://github.com/nberlee/mqtt-nats-broker/issues/127), kept as a
  `docs/CONFORMANCE.md` residual rather than a roadmap item.

- [x] `subsBarrier.wait` waits only for the `processSubs` entries live at its sample, so
  later SUBSCRIBEs can no longer extend its generation; deliberately no elapsed-time
  bound, because abandoning the wait trades a delay for an interest-retention loss
  (residual in `docs/CONFORMANCE.md`); a double `leave` panics rather than releasing
  another entrant. Done 2026-10-12.

- [x] Persist the MQTT 5 DISCONNECT expiry override as an obligation of the accepted
  connection (`mqtt.lifetimeOverride`), applied at the close and on every park and
  discharge branch; [issue #45](https://github.com/nberlee/mqtt-nats-broker/issues/45). Do
  not re-propose anchoring the expiry deadlines earlier without first enumerating all six
  consumers of `disconnectedAt` and proving the ordering against a nanosecond `FireAt`.
  Residuals in `docs/CONFORMANCE.md`.

- [x] Restore shared membership (`sharedSubs`, `rapSubs`, `subIDs`) on a SUBSCRIBE
  rollback's definite-failure arm only;
  [issue #43](https://github.com/nberlee/mqtt-nats-broker/issues/43). No exhaustive
  sole-producer proof is claimed, so the sweeps, unions and derivations that tolerate a
  membership mismatch are kept.

- [x] Release stranded shared pending state at membership removal: UNSUBSCRIBE's no-`cons`
  branch by [issue #85](https://github.com/nberlee/mqtt-nats-broker/issues/85), the 3.1.1
  forced leave by §0.2's forced-leave item; `purgeSharedPending` remains the backstop for
  a session that simply disconnects;
  [issue #49](https://github.com/nberlee/mqtt-nats-broker/issues/49).

- [x] Refuse, not retry, an incomplete shared-subscription restore: restoration runs
  before the success CONNACK, and a filter it cannot install, or a malformed persisted
  shared key, refuses the CONNECT. Closed 2026-08-23;
  [issue #54](https://github.com/nberlee/mqtt-nats-broker/issues/54). Residuals in
  `docs/CONFORMANCE.md`.

- [x] Accepted residual, not fixed (2026-10-12 convergence triage): a foreign discharge of
  an ambiguously parked close performs no session save and no expiry scheduling; stated in
  `docs/CONFORMANCE.md` ("What the discharge does NOT carry is the session's expiry");
  [issue #30](https://github.com/nberlee/mqtt-nats-broker/issues/30).

- [x] Accepted residual, not crash-safe (2026-10-12 convergence triage): the rejected
  finite-disconnect stamp, reached only by a finite close whose save is rejected followed
  by a crash before any settlement write lands; stated in `docs/CONFORMANCE.md` beside the
  rejected-close residuals;
  [issue #41](https://github.com/nberlee/mqtt-nats-broker/issues/41).

- [x] Accepted residual for arm (a) (2026-10-12 convergence triage); arms (b) and (c)
  closed by [issue #86](https://github.com/nberlee/mqtt-nats-broker/issues/86): a remote
  takeover racing an ordinary finite close can cause one unwanted immediate Will, never a
  lost Will; retracting it needs a durable Will intent (Will-outbox scope). Stated in
  `docs/CONFORMANCE.md` under "A rejected close still publishes its Will";
  [issue #42](https://github.com/nberlee/mqtt-nats-broker/issues/42).

- [x] Parked, not fixed: chapter P. Keep affine assignments when a SUBSCRIBE packet
  fails; [issue #51](https://github.com/nberlee/mqtt-nats-broker/issues/51).

- [x] Parked, not fixed: chapter P. Arbitrate affine cleanup before discarding
  assignments; design and test
  [issue #51](https://github.com/nberlee/mqtt-nats-broker/issues/51) and
  [issue #52](https://github.com/nberlee/mqtt-nats-broker/issues/52) as one
  transactional-cleanup cluster.

- [x] Arbitrate retained-index application by stream sequence against the observer's
  watermark W, and repair a lost application by resetting the observer from W+1;
  `SubjectDeleteMarkerTTL` declined (it needs `AllowRollup`). Done 2026-10-12;
  [issue #63](https://github.com/nberlee/mqtt-nats-broker/issues/63). Residuals in
  `docs/CONFORMANCE.md`.

- [x] An accepted publication whose retained update the store did not confirm never gets
  final success, per QoS; the broker never re-attempts a retained store on its own. Done
  2026-10-12; [issue #64](https://github.com/nberlee/mqtt-nats-broker/issues/64).
  Residuals in `docs/CONFORMANCE.md`.

- [x] Reclaim CONNECT acceptance facts, or bound them: decided 2026-10-12 as an accepted
  residual, neither reclaimed nor bounded; the proposed reclamation rule (recorded in
  [issue #133](https://github.com/nberlee/mqtt-nats-broker/issues/133)) is unsound and
  must not be implemented. The exposure is stated in `docs/CONFORMANCE.md`; general
  reclamation of these classes is a §2 audit item, not an alpha precondition.

- [x] Retire a live generation's outbound history behind a proven contiguous floor `Lo`
  and deletion watermark `Del`, every reader reading `[Lo, Hi]`. Done 2026-10-13;
  [issue #114](https://github.com/nberlee/mqtt-nats-broker/issues/114). Total storage
  stays unbounded and never-ending obligations are carried (`docs/CONFORMANCE.md`).

- [x] Decide the server-side consumers-per-stream default cap: option (a), a deployment
  prerequisite (`default_max_consumers: -1`), decided 2026-10-12; the deployment floor was
  raised to 2.15.0 for it on 2026-09-09. The broker still cannot read the server default,
  so the `docs/CONFORMANCE.md` residual stays;
  [issue #97](https://github.com/nberlee/mqtt-nats-broker/issues/97).

- [x] Run the external conformance environments on `nats:2.15.0` with
  `default_max_consumers: -1`, asserted on the effective limit by
  `test/conformance/run.sh`'s `check_nats_floor` (which decodes `/varz` and requires
  `python3`). Done 2026-09-20.

- [x] Name a retirement's affine target whole or not at all
  (`mqttAffineRetireDescriptor`), so a clean start over a live QoS 2 session is no longer
  refused; the validator stays strict. Fixed 2026-10-05;
  [issue #115](https://github.com/nberlee/mqtt-nats-broker/issues/115).

- [x] Moved to §2 "Performance and resource audits" (2026-10-12 convergence triage):
  sixteen unspaced bounded re-attempt loops, each to be classified before any is
  converted; moving it implies no loop is harmless;
  [issue #116](https://github.com/nberlee/mqtt-nats-broker/issues/116).

- [x] An acknowledged QoS 2 PUBLISH is not sent again: the acknowledgement marks the open
  section `answered`, and `trackPublish` declines a source held by a PUBREL-phase entry.
  Done 2026-10-13; [issue #117](https://github.com/nberlee/mqtt-nats-broker/issues/117).

- [x] Keep a PUBREL attempt's PUBCOMP unit only for a frame that may have left
  (`possiblyEmitted`, given back by `endPubRelSend`). Done 2026-10-13;
  [issue #118](https://github.com/nberlee/mqtt-nats-broker/issues/118).
  `TestMQTTDrainBarrierFailsClosed`'s send-hold red is not closed by it
  ([issue #136](https://github.com/nberlee/mqtt-nats-broker/issues/136)).

- [x] Bound a connected client's claimed packet length at its fixed header, before any
  body is buffered, with linear partial buffering. Done 2026-10-14;
  [issue #122](https://github.com/nberlee/mqtt-nats-broker/issues/122).

- [x] Accepted residual (2026-10-12 convergence triage): a displaced connection's send
  intent after certification can strand one identifier and Receive Maximum charge per
  exchange until generation discard; disclosed in `docs/CONFORMANCE.md`. A fix, if taken
  up, is an ordering design with the witness's assertions inverted deliberately;
  [issue #123](https://github.com/nberlee/mqtt-nats-broker/issues/123).

- [x] Accepted residual (2026-10-12 convergence triage): a displaced resident subscription
  churns refused admissions while its eviction is delayed, shown only under the fixture's
  hold; stated in `docs/CONFORMANCE.md`. A fix, if taken up, needs a deterministic
  witness of the churn and of the starvation, and a design that stops or bounds a
  displaced session's delivery processing while preserving the successor's service;
  [issue #124](https://github.com/nberlee/mqtt-nats-broker/issues/124).

- [x] Accepted residual (2026-10-12 convergence triage): a drain finalize retried with no
  connection leaves the retired drain's consumer until resume settlement or generation
  disposal; disclosed in `docs/CONFORMANCE.md`. A fix, if taken up, needs an ownership
  design for the disposal decision on connectionless retries, plus a deterministic
  witness of in-session disposal after the failure;
  [issue #126](https://github.com/nberlee/mqtt-nats-broker/issues/126).

- [x] Refuse userinfo in every configured `-nats` URL at startup, so URL credentials no
  longer replace passthrough clients' own; usage text no longer prints environment values.
  Done 2026-10-25; [issue #129](https://github.com/nberlee/mqtt-nats-broker/issues/129).

- [x] Answer a clean-start CONNECT without waiting on its predecessor's teardown: the
  gate wait is event-driven, bounded by the ownership deadline and answered 0x89; a clean
  close releases the gate once its retirement is durably owned and the successor displaces
  the marked record; the orphan sweep is also licensed by a discarding closure. Done
  2026-11-01; [issue #155](https://github.com/nberlee/mqtt-nats-broker/issues/155).

### 0.4 Release-verification investigations

These entries preserve observations without pretending the implementation scope is
known. Capture complete logs, obtain a deterministic reproduction or identify the
failed test, and then close the investigation, accept it as a residual, or move it to its
own GitHub issue with a box under §2 "Intermittent reds". Only a confirmed loss,
corruption or security mechanism becomes a new §0.3 finding (the filing rule at the top of
§0.3). This section keeps only observations whose dangerous mechanism is still unexcluded;
pure test-infrastructure and unattributed flake observations go to §2 "Intermittent reds",
whose boxes satisfy the alpha gate only by an explicit accepted closure or a confirmed
finding's disposition, and never waive green CI. The earlier verification ledger is closed
and kept as an archive
([issue #136](https://github.com/nberlee/mqtt-nats-broker/issues/136)); the entries moved
there below are recorded in it.

- [x] A withheld marker beside a started exchange took a freed slot's NAK and was
  budgeted twice at resume (the I5 drop was unaffected); a marker now names only an
  unmapped source. Fixed 2026-11-02; [issue #162](https://github.com/nberlee/mqtt-nats-broker/issues/162).

- [x] Reproduce and fix a reconnect racing its own DISCONNECT with Session Expiry 0: the
  takeover persists the bound connection's ending override before its resume decision.
  Fixed 2026-10-14; [issue #44](https://github.com/nberlee/mqtt-nats-broker/issues/44).

- [x] Identify the unexplained full-suite failure: closed as unidentifiable, not refuted
  (truncated log, not reproduced in 20 runs); do not truncate suite output. Closed
  2026-10-12; [issue #46](https://github.com/nberlee/mqtt-nats-broker/issues/46).

- [x] Determine whether the phase-2 shutdown observation is real: moved to the
  [verification ledger](https://github.com/nberlee/mqtt-nats-broker/issues/136) as
  nonblocking on present evidence. Moved 2026-10-12;
  [issue #48](https://github.com/nberlee/mqtt-nats-broker/issues/48).

- [x] Stop a re-SUBSCRIBE that changed No Local re-judging owned messages: No Local is
  decided by a per-message stamp taken at ownership. Fixed 2026-10-14;
  [issue #119](https://github.com/nberlee/mqtt-nats-broker/issues/119).

- [x] Moved `TestMQTTRedeliveryAckWait`'s intermittent failure to the
  [verification ledger](https://github.com/nberlee/mqtt-nats-broker/issues/136) on
  2026-10-12; traced and closed by the §0.5 QoS 1 AckWait item on 2026-11-04;
  [issue #120](https://github.com/nberlee/mqtt-nats-broker/issues/120).

- [x] Moved `TestDispatcherIsFairUnderContinuousHints`'s untraced intermittent failure
  to the verification ledger; closure is a traced mechanism, then a dispatcher fix with
  this test as its witness or a measurement correction justified by that trace. Never
  loosen the assertion; treat it as pre-existing when comparing suites. Moved 2026-10-12;
  [issue #125](https://github.com/nberlee/mqtt-nats-broker/issues/125).

- [x] Accept the delayed-Will worker's missing lease renewal as a residual: a claimant
  stalling past its lease during subscription restoration can duplicate a Will. A fix, if
  taken up, renews the claim through dispatch, the barrier wait, publication and the
  terminal commit, with renewal and finalization serialized; treats renewal failure by
  phase (before the irreversible fan-out an unconfirmed renewal stops the worker, after it
  the worker continues through barrier, store and terminal reconciliation); claims no
  exactly-once; and does not bound the barrier, which trades the duplicate for a loss. Its
  verification is a two-broker regression holding one broker's barrier past a shortened
  lease (`mqttTestSubsBarrierHeldHookV`) and asserting the subscriber receives the Will
  exactly once. Accepted 2026-10-12.

- [x] Retired `TestMQTTOriginFirstEverCreateRaceReplicated`'s rejection-count assertion by
  deletion with the session-interval register (write-lineage audit), not fixed.

- [x] Moved the two `AbortsThePassOnATransientFactOutage` fixtures to the
  [verification ledger](https://github.com/nberlee/mqtt-nats-broker/issues/136), with the
  caveat that the Claimant half stays suspect rather than dismissed until each half runs
  alone under injected concurrency or the assertion counts reads for this pass only. Moved
  2026-10-12.

- [x] Parked `testQ2AffAwaitMaterialized`'s nil dereference of a reaped claim with the
  experimental shared-QoS-2 suite in chapter P, not fixed.

- [x] Parked `TestQ2AffineResumeDoesNotRechargeAckBudget`'s reconnect-loop EOF with the
  experimental shared-QoS-2 suite in chapter P, not fixed.

- [x] Accept retained convergence for a sourced `$MQTT_rmsgs` topology as a coverage gap,
  stated as untested in `docs/CONFORMANCE.md`; this does not close
  [issue #63](https://github.com/nberlee/mqtt-nats-broker/issues/63). Accepted 2026-10-12.

- [x] Parked the affine contender fixture's rival-create premise with the experimental
  shared-QoS-2 test infrastructure in chapter P, not fixed;
  [issue #74](https://github.com/nberlee/mqtt-nats-broker/issues/74).

- [x] Moved paging of the shared `$MQTT_out` consumer enumeration to §2 "Performance and
  resource audits" (done there). Moved 2026-10-12.

- [x] Closed `TestSharedDenyDoesNotConsumeForAllowedMembers` as a fixture premise: nothing
  is lost, and a late allowed joiner's wait has no finite deadline while a denied member
  stays in the group (residual 11 in `docs/CONFORMANCE.md`). Closed 2026-10-14.

- [x] Attributed the `TestMQTTInflight*` family's intermittent failures per member: a
  parked PUBACK release no longer runs on the delivery goroutine (fixed), and a QoS 1
  re-offer dispatched before its acknowledgement landed is an accepted residual; the
  remaining reds are in the verification ledger. Attributed 2026-10-15.

- [x] Fixed the Paho `test_keepalive` and `test_subscribe_options` regressions on the
  local rig; the first CI run on a runner is the confirmation still owed, and a runner
  that fails either test is a new filing. Closed 2026-10-15.

- [x] Moved the deterministic fixture-reach failure of
  `TestMQTTExclusionReclaimWireSafetyUnderADisplacedWriterCreate` to the verification
  ledger. Moved 2026-10-12; filed by
  [issue #121](https://github.com/nberlee/mqtt-nats-broker/issues/121).

- [x] Classified the remaining deterministic failures at `db63905`: stale fixtures
  retargeted, and a live QoS 1 delivery's send-section acquisition is a foreground
  authority operation with a budget of one attempt more than the five background writers
  (`mqttForegroundAcquireAttempts`), spaced by the contention wait (fixed); the rest are
  in the verification ledger. Classified 2026-10-16.

- [x] Fix three broker defects the ledger reds traced: a PUBREL exchange decided complete
  is not admitted again, a terminal cleanup acts on the observation it reads, and PUBACK
  re-attempts a contended terminal release. Fixed 2026-10-16.

- [x] Carried the four unexplained reds of the batching item's verification into the
  [verification ledger](https://github.com/nberlee/mqtt-nats-broker/issues/136), all
  accepted as nonblocking on present evidence; a recurrence gets its own issue and a box
  under §2 "Intermittent reds", citing its archived entry, and any untagged `-race` failure
  reopens this box, its full output investigated without presuming the same cause. Carried
  2026-10-30.

- [x] Classified Paho `test_subscribe_options` on a slow store: the missing retained message
  arrives late (serial durable replay contending on the client's authority record), not
  lost; accepted as a latency residual in `docs/CONFORMANCE.md`, not fixed. Classified
  2026-11-02; [issue #156](https://github.com/nberlee/mqtt-nats-broker/issues/156).

### 0.5 Small pre-alpha fixes

Added 2026-10-26 at the user's request: three gaps `docs/CONFORMANCE.md` states that are
small to close and should not ship in the alpha. One item, because each is a contained
change to a refusal or shutdown path and none needs a new mechanism. It is an alpha
blocker: the gate's "every unconditional alpha blocker" box covers it.

- [x] Close three small stated gaps: admission enforces the CONNECT deadline by the clock
  ([issue #77](https://github.com/nberlee/mqtt-nats-broker/issues/77)), a refused CONNECT
  is answered with a CONNACK by the user's decision
  ([issue #65](https://github.com/nberlee/mqtt-nats-broker/issues/65)), and closure
  certificates are joined at graceful shutdown. Done 2026-10-26.

Added 2026-11-04 at the user's request: supported-path findings that no unchecked box
carried, so document order never reached them. Each is an alpha blocker.

- [x] Traced and fixed the two QoS 1 waits for AckWait: a detaching connection's delivery
  subscription is drained and a predecessor's offer is recorded or NAKed
  ([issue #170](https://github.com/nberlee/mqtt-nats-broker/issues/170)); the
  `TestMQTTRedeliveryAckWait` map failure was fixed by 21eca60 and its stalled-PUBACK
  read is fixed ([issue #120](https://github.com/nberlee/mqtt-nats-broker/issues/120)).
  Done 2026-11-04.

- [x] Deleted the drain's persisted register sequence instead of refusing a shape: a
  pre-field record and a current zero were byte-identical, and nothing read the field.
  Done 2026-11-04 ([issue #71](https://github.com/nberlee/mqtt-nats-broker/issues/71)).

- [x] Removed the `nonce` parameter `reapRegisterGatedOutcome` never read, from it,
  `reapRegisterWithVerdictGated` and their callers: the door matches on the reference and
  deletes against its own load. Done 2026-11-05
  ([issue #172](https://github.com/nberlee/mqtt-nats-broker/issues/172)).

Added 2026-11-05 at the user's request: an embedded rolling upgrade that does not
converge. An alpha blocker: the embedded StatefulSet is a supported deployment.

- [x] MQTT initialization retries availability failures within the server run, without a
  budget (decided against), and `/healthz` is bound before the embedded server starts, so
  neither an unanswered request nor a probe cuts recovery short. Done 2026-11-05
  ([issue #177](https://github.com/nberlee/mqtt-nats-broker/issues/177)).

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
  is quarantined behind `mqtt_experimental_shared_qos2` and its CI job is non-gating;

- do not repair an affine-only regression opportunistically. Append it to the single
  completion item below, with the responsible general change and failing test recorded.

Historical closed findings remain closed as evidence about the implementation that exists;
they do not impose a promise that later alpha work preserve that implementation unchanged.

### Fallout from issue #94's admission half, filed here by the supported-scope rule

[issue #94](https://github.com/nberlee/mqtt-nats-broker/issues/94)'s admission half
(closed 2026-09-04) left **two experimental affine failures** in
`make experimental-shared-qos2-tests`, measured against HEAD `ae30b56` (four failures at
HEAD, six in the tree). Both are affine-only, so by the 2026-09-02
supported-scope rule they are appended here rather than widening that item.

- **`TestQ2AffForgedRegisterCannotCreditAnotherClaim`.** The affine disposer cannot
  delete a slot in `pubrel_ready`, because the phase gate refuses
  `mqttSlotDeleteComplete` from every past-PUBREC state — deliberately, since a ready slot
    is the identifier's live durable
  claimant. The disposer needs the same **advance-then-delete** treatment recovery got:
  a proof that the exchange is finished licenses an advance to `pubrel_terminal`, and
  the advance licenses the delete. Neither step is sound alone.

- **`TestQ2AffDuplicatePubRelCallbackDischargesBeforeAcking`.** An affine duplicate
  whose slot is absent is no longer acknowledged, so it redelivers forever. The
  supported-path stance is FAIL-CLOSED: a record carrying a valid SOURCE CORRELATION with
  a definitely-absent slot is REFUSED, not acknowledged (an absent slot cannot prove the
  exchange is over; a register can be released by the drain path while the exchange is
  still in `publish`). It is genuine residue that may occupy `$MQTT_out` until a complete
  terminal scan or operator removal (R8). An affine duplicate carries an affine reference
  and no source correlation, so it is likewise refused; a retained exchange's PUBREL is
  never stored on `$MQTT_out`, so a stored record with no owner slot is refused and left
  unacknowledged whatever its shape.

Both are consequences of the phase gate and the admission scope working as specified on the
supported paths, not of an affine defect newly introduced. Neither is an alpha blocker.

### Fallout from #100's resident-reaper authority enforcement, filed here by the same rule

[issue #100](https://github.com/nberlee/mqtt-nats-broker/issues/100)'s attempt 3
(2026-09-13) leaves **twelve new experimental affine failures**, measured against a
clean HEAD `3544918` with the same test binary, the same `TestOrphanSweep` filter and
`-test.parallel 2`; they reproduce identically at `-test.parallel 1`, so they are not a
parallelism artefact.

**The cause, and it is the general change working as specified.** The affine disposer's
RESIDENT arm still asks for `mqttSlotDeleteComplete` through the consolidated gated door.
C-IV requires proven-unsent authority for that disposition, and arm 4 excludes a `Retired`
record unconditionally — retirement is backed by the client's own PUBREC, so
non-transmission is false by construction. Every register this disposer handles is retired
by the sweep's own predicate. Precisely: the reaper still reports success for an absent,
replaced or moved observation; what it can no longer do is **delete the unchanged retired
occupant** these fixtures ask it to delete. The NON-RESIDENT arm is unaffected — its guards
are the same three HEAD applied — so only the resident half regressed.

**What that costs, disclosed rather than minimised**:
[issue #68](https://github.com/nberlee/mqtt-nats-broker/issues/68)'s disposal of
a settled affine
triple no longer happens for a RESIDENT session, so the durable register and the in-memory
resident retirement both accumulate, one pair per exchange, exactly the growth
[issue #68](https://github.com/nberlee/mqtt-nats-broker/issues/68) closed.

**Affine-only by REACHABILITY, not by test tag.** The sweep's predicate requires
retirement and both affine references; supported subscription admission lowers a shared
QoS 2 request to QoS 1 before provisioning, and `cmd/mqtt-nats-broker/serve.go:44` fixes
the experimental gate off. A defect exposed by experimental tests would still block if it
touched supported states; this one does not.
[issue #100](https://github.com/nberlee/mqtt-nats-broker/issues/100) therefore stays
**closed over the supported boundary**, with that qualification load-bearing.

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

They are kept AS THEY ARE, deliberately: each one is now evidence of the lost
[issue #68](https://github.com/nberlee/mqtt-nats-broker/issues/68) disposal
behaviour, and rewriting them to the new outcome would erase the only record of what was
given up. Note also that HEAD's own baseline failure in this filter,
`TestOrphanSweepPubRelSurvivesAnUnreadableAffineInventory`, is **absent** from the tree's
run — unexplained, recorded as such, and NOT claimed as a repair.

- [ ] **[issue #92](https://github.com/nberlee/mqtt-nats-broker/issues/92): the
  orphan sweep strands a void verdict held only by a non-resident
  session's record.** The stranding branch is
  `disposeSettledAffineRegister`'s non-resident arm, at its
  `deleteInflightRegister` call (`internal/mqtt/orphan_qos2_sweep.go`), whose only
  production route requires `mqttQ2AffSettleableRegister` and therefore a register carrying
  both `Q2AffClaim` and `Q2AffCopy`. The supported default never creates that pair
  (`ExperimentalSharedQoS2` false at `cmd/mqtt-nats-broker/serve.go:44`; the
  `f.shared && f.qos == 2 && !c.mqtt.experimentalSharedQoS2` arm in `mqtt.go`
  lowers a shared QoS 2 request to QoS 1 before provisioning), so the population is
  experimental-only. The sweep's other register delete (`:1848`) cannot strand a verdict:
  it requires a proven generation retirement, and by then the record carrying
  `VoidedRegs` is already gone.
  [issue #89](https://github.com/nberlee/mqtt-nats-broker/issues/89) gave the
  affine sweep a retention rule keyed on
  what the REGISTER carries; a verdict living only in a non-resident session's persisted
  `VoidedRegs` is invisible to a walk that reads registers rather than records, so such
  a register is still deleted and its verdict is retired only at generation retirement —
  one nonce per occurrence, **cumulative over a generation**, which is NOT a claim that the
  aggregate is bounded. A corrected design (a verdict set carried on the sweep's existing
  account-wide record walk, plus a budgeted act-time reread that fails closed to retain) is
  drafted and reviewed once; it is a **mitigation, not a closure**, and leaves a residual.

- [ ] **[issue #98](https://github.com/nberlee/mqtt-nats-broker/issues/98): the
  affine owner's normal create can inherit a stream-imposed inactivity
  timer without noticing.** **Affine-only, which is why it is filed here rather than in
  §0.3:** both call sites of `ensureQ2AffineConsumerSubscription` are affine-gated
  (`if f.shared` at SUBSCRIBE, `if q2AffGen != _EMPTY_` on restore,
  `internal/mqtt/mqtt.go`).
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

  The decision itself is
  [issue #173](https://github.com/nberlee/mqtt-nats-broker/issues/173): retire the
  experiment, keep it parked, or invest. It is the owner's, made after the first alpha
  release.

  The starting inventory is:

  - [issue #51](https://github.com/nberlee/mqtt-nats-broker/issues/51) and
    [issue #52](https://github.com/nberlee/mqtt-nats-broker/issues/52), the
    transactional SUBSCRIBE-cleanup cluster;

  - [issue #53](https://github.com/nberlee/mqtt-nats-broker/issues/53),
    unreadable-claim diagnostics and recovery;

  - [issue #68](https://github.com/nberlee/mqtt-nats-broker/issues/68),
    including residual 8 and W-c;

  - [issue #74](https://github.com/nberlee/mqtt-nats-broker/issues/74) and the
    two parked affine fixture
    failures in §0.4;

  - [issue #75](https://github.com/nberlee/mqtt-nats-broker/issues/75), affine
    same-topic ordering;

  - the parked
    [issue #37](https://github.com/nberlee/mqtt-nats-broker/issues/37)
    transition/reap census;

  - **[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107)'s
    affine completion work**, relocated here on 2026-09-20 and described under its
    own heading below;

  - every affine-only branch or regression accumulated from
    [issue #40](https://github.com/nberlee/mqtt-nats-broker/issues/40),
    [issue #76](https://github.com/nberlee/mqtt-nats-broker/issues/76),
    [issue #94](https://github.com/nberlee/mqtt-nats-broker/issues/94)/
    [issue #95](https://github.com/nberlee/mqtt-nats-broker/issues/95), or
    later supported-path work. Their non-affine acceptance remains with their original
    finding; only the experimental delta belongs here;

  - **[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107)
    cutover 7b, reduced slot-only test coverage.** Removing the shared-contest and
    ordinary custody-transfer tests removed incidental killers for the experimental affine
    contest and transfer arms that remain: displacement and unfenced-resume refusal,
    older-claim takeover, consumed-close handling, claim ordering, once-per-delivery
    transfer and post-transfer acquisition. Censused edges keep verified killers, and the
    existing affine tests are unchanged and green. When this item disposes of the class,
    restore direct slot-only coverage for the surviving arms or delete those arms with the
    mechanism. This is an evidence gap, not a demonstrated behaviour regression.

  - [issue #166](https://github.com/nberlee/mqtt-nats-broker/issues/166): the
    experimental `TestQ2AffineConsumerDeletesAreInventoried` fails closed on two
    consumer deletes whose stream it cannot resolve statically,
    `mqttSession.deleteRetiredConsumer` (source retirement, `1f9501f`) and
    `mqttAccountSessionManager.deleteRecoveryConsumer` (recovery-owner retirement,
    `764e22c`). Whether either can reach an affine `$MQTT_out` consumer is open; a guard
    red, not a demonstrated behaviour regression.

  Before promotion, re-audit the current code rather than assuming this inventory stayed
  complete. Settle the affine persisted-data model and stream topology first, then declare
  the compatibility boundary that begins with the first release which actually supports
  shared QoS 2. Experimental records from earlier builds remain outside that boundary and
  may require a reset; promotion does not retroactively create a migration promise.

### issue #107's affine completion work

Under the 2026-09-20 ruling that session-affine shared-subscription QoS 2 is not an
alpha release feature, only the affine half of
[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) is carried here.
[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) itself was accepted
on 2026-10-08: its authority, recovery directory, inventory, resource authority and
retirement machinery are alpha infrastructure, and so is its supported conversion set.
Only the affine remainder is chapter P. The affine half is the cleanup family the
recovery pass executes end to end. Recovery transfers cleanup for
`mqttResourceAffineConsumer` and withholds every other resource kind
(`internal/mqtt/outbound_duty_exec.go:125`), and a closure enrols that resource only when
an affine target exists (`internal/mqtt/outbound_close_enrol.go:108`).

- [ ] **Complete the affine cleanup transfer, under BOTH missing premises.** Neither premise
  holds today and neither is an alpha prerequisite.

  1. **An admission closure that accounts for every previously admitted
     producer** - [issue #109](https://github.com/nberlee/mqtt-nats-broker/issues/109).
     `beginQ2AffOp` admits against LOCAL session
     state and the claim CAS never contends with the generation's durable closure, so a
     mover paused before that CAS can install a fresh undiscarded claim after the pass's
     final verification read.
  2. **Authoritative enumeration completeness** —
     **[issue #110](https://github.com/nberlee/mqtt-nats-broker/issues/110)**,
     an upstream data-layer defect
     tracked in `docs/known-upstream-issues.md` (nats-server#8632). Only the affine
     application of it is this item's; the general freshness, completeness, incarnation and
     boundedness requirements
     are the data layer's to satisfy and await the upstream correction, not broker-side
     work.

  **What is demonstrated today** is execution of the executor over the fixture populations
  the existing restart tests exercise, conditional on both premises. It may not be presented
  as an independently safe production cleanup transfer, and affine recovery is not claimed
  demonstrated unconditionally.

  **Acceptance this still owes**, preserved so a future attempt does not re-derive it: commit
  closure through the intended path, authorize and perform metadata release while cleanup
  remains outstanding, lose all transient state, then let a restarted broker discover and
  complete the cleanup from persisted evidence alone — with the oracle being the terminal
  duty and the completion certificate, never "the consumer is gone", which a synchronous delete
  that ran anyway satisfies equally. Removing the evidence or the recovery reachability must
  make that witness FAIL.

  **Unresolved populations**, stated in `docs/CONFORMANCE.md`: a delayed affine create
  landing after certification, and a creation over a CLOSED predecessor.

- [ ] **[issue #72](https://github.com/nberlee/mqtt-nats-broker/issues/72)'s
  affine remainder: the raw affine enqueue is not revalidated at the
  write.** [issue #72](https://github.com/nberlee/mqtt-nats-broker/issues/72)
  has no supported-path form. An ordinary,
  shared QoS 1 or retained QoS 1/2 frame re-enters `beginSendLocked` and is written only
  through `writeAuthorizedFrame`, which consumes its acquisition for the live holder on
  the writing connection's own binding; `mqttEnqueuePreparedPublishMsgTo` refuses such a
  frame without its acquisition. An acquired action executing on its original, still-open
  connection after the session retires is permitted by
  [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107)'s
  contract, not a defect.
  What remains is the experimental affine route (`qos2_shared_affine.go`):
  `storeInflightRegister` confirms the register, `beginSendLocked` opens the send section
  from the pending entry, and the frame is then queued RAW (a slot-only frame requires no
  action), so nothing at the write consumes an acquisition or revalidates the generation,
  the binding or the subscription's closure. It closes with the affine route's conversion
  onto the authority.

### issue #121's affine remainder, filed here by the supported-scope rule

- [ ] **A cancelled experimental affine delivery can still land after its pending fence is
  disposed of.**
  [issue #121](https://github.com/nberlee/mqtt-nats-broker/issues/121) closed the
  supported classes with an entitlement-fenced authority advance before fence disposal.
  The slot-only class has no authority to advance: its fence is written by
  `storePendingFenceCAS` and its admission by `storeInflightRegister`'s plain CAS, and it
  opens no custody admission, so neither the advance nor the ladder-liveness check
  applies. **Not claimed fixed.** Counterexamples a fix must survive: keying disposal on
  ledger settlement fails for an affine admission paused after its `stillOurs` check
  while the cancellation fences (a disposer sees an empty ledger and the create-only pair
  lands after the fence), and for an ambiguous affine pair that commits after its
  placeholder was abandoned; and an ambiguous affine attempt followed by a successful
  retry. Stated in `docs/CONFORMANCE.md` ("A cancelled experimental affine delivery can
  still be served after its pending fence is disposed of"). It closes with the affine
  route's conversion onto the authority, like
  [issue #72](https://github.com/nberlee/mqtt-nats-broker/issues/72)'s remainder above.

### issue #109: affine claim admission is fenced only in memory

**Unresolved**, and outside the alpha under the user's 2026-09-20 ruling that
session-affine shared-subscription QoS 2 is not an alpha release feature. Both orderings
of admission against closure are unresolved.

Why it is not an alpha prerequisite: the premise
[issue #109](https://github.com/nberlee/mqtt-nats-broker/issues/109) supplies is
consumed only by the
affine half of
[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107)'s recovery
pass. Recovery transfers cleanup for
`mqttResourceAffineConsumer` alone (`internal/mqtt/outbound_duty_exec.go:125`), and a
closure enrols that resource only when an affine target exists
(`internal/mqtt/outbound_close_enrol.go:108`). Production never reaches the affine mover,
because admission downgrades a shared QoS 2 filter to QoS 1 first (`internal/mqtt/mqtt.go:9508`).

- [ ] **[issue #109](https://github.com/nberlee/mqtt-nats-broker/issues/109):
  affine claim admission is fenced only in memory, so a paused mover can
  install an undiscarded claim after its generation is closed.** `beginQ2AffOp`
  (`qos2_shared_affine.go`) admits a shared QoS 2 move against LOCAL session state alone —
  the session mutex and its in-memory `q2AffClosing`, `q2AffReady`, `q2AffGen` and
  `q2AffConsumer` — and `moveStartedSharedQoS2` then creates the assignment claim with a
  subject-only create-only CAS that never contends with the generation's durable closure.
  `closeQ2AffAndDrain` supplies quiescence only for a teardown that actually runs on the
  owner. The reachable schedule is cross-broker: the old owner is alive with a mover already
  admitted and paused, a successor drives the durable handoff that closes the generation
  remotely, so the old owner's local closing flag is never set, and the mover resumes and
  writes a fresh UNDISCARDED claim.

  **What this invalidates.**
  [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107)'s
  recovery pass asserts readiness on a completed claim
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
  affine half of
  [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107)'s recovery
  pass, and the integrated change needs its own holistic review with a
  deterministic cross-broker paused-mover test covering BOTH orderings of admission against
  closure.

## U. Upstream data-layer defects (tracked, not ours)

These were [issue #110](https://github.com/nberlee/mqtt-nats-broker/issues/110),
[issue #111](https://github.com/nberlee/mqtt-nats-broker/issues/111) and
[issue #112](https://github.com/nberlee/mqtt-nats-broker/issues/112) - defects
in the JetStream data layer, not in this
broker, moved out of section 0 by the boundary ruling of 2026-09-21. They are no longer
carried as roadmap items: the standing record, with the real-server witnesses, the
upstream references and the unblock conditions, is `docs/known-upstream-issues.md`. The
boundary ruling itself is restated there. Each closes when upstream ships a correction and
the pin moves onto it under the ordinary reviewed-bump procedure; none is an alpha blocker.
At the v2.15.1 pin (2026-10-11) the store functions all four cite are byte-identical to
v2.15.0 and every witness still reproduces, so none closed with that bump; at the v2.15.2
pin (2026-10-27) they are byte-identical again, so none closed with that one either.

- [x] Retire the [issue #111](https://github.com/nberlee/mqtt-nats-broker/issues/111)
  point-read absence census: tests, manifests and dossier deleted; its two broker-contract
  checks survive as `internal/mqtt/jsa_msgget_transport_test.go`. Not a broker repair,
  and #111 is not reinstated. Done 2026-10-15.

## 1. Settle release decisions

These decisions affect binary behavior, package names, images, charts,
documentation, and release artifacts. Make them before creating those surfaces.

### Product name

- [x] Decide the product name: `mqtt-nats-broker`. Decided 2026-10-17.

- [x] Rename the product to `mqtt-nats-broker` (2026-10-17): module path, binary, image,
  persisted consumer metadata keys (renamed, not migrated) and operator defaults. The
  `MQTTNB_` environment prefix is kept, with no aliases.

### Durable enforcement of the session/marker coupling

- [x] Decide whether to durably enforce the `$MQTT_sess` + marker coupling at startup:
  answered by deleting `$MQTT_willcancel`;
  [issue #70](https://github.com/nberlee/mqtt-nats-broker/issues/70) enforces the epoch on
  all ten broker-owned streams and refuses a proper subset at startup (landed 2026-09-01).

### In-flight verdict schema names

- [x] Rename the in-flight verdict surfaces
  ([issue #50](https://github.com/nberlee/mqtt-nats-broker/issues/50)):
  `VoidedRegs` (`voided_regs`) and `voidedRegNonces`, with no migration and no dual-key
  reader; a record carrying `cancelled_regs` decodes with no verdicts. Decided and done
  2026-10-17.

### Default stream replica count

- [x] Decide what `-stream-replicas 0` means: the count is required with no default, a
  count below 1 is refused, and every existing stream must carry it; the chart's
  external topology requires it. Decided and done 2026-11-05
  ([issue #171](https://github.com/nberlee/mqtt-nats-broker/issues/171)).

### Memory-limit strategy

- [x] Adopt `github.com/KimMachineGun/automemlimit` v1.0.0 for the Go memory limit;
  mechanism and limits in `docs/ARCHITECTURE.md` and `docs/CONFORMANCE.md`. Decided and
  done 2026-10-17.

### Contracts in place by the first release

Added 2026-11-06 at the user's request. Every interface one broker, or a later version,
reads gets its version and its written rules before the first release: without a version
from day one, a later release has to infer what wrote a record. Each item is an alpha
blocker.

**The alpha guarantees no version compatibility; the guarantee starts at beta.** During
alpha a breaking change is decided case by case, weighing what compatibility would cost
to build against how destructive the break is for an operator (a refusal with recreate
guidance, a purge, a migration). From beta, the contracts below are kept. The migration
machinery itself waits for the first breaking change that needs one; until a second
format exists there is nothing to migrate. Until the first release, the compatibility
boundary's "no compatibility" rule stands, and this policy is written into it as its
"Release phases" and "The persisted format".

- [x] **Persisted format.** Every broker-owned stream carries `mqtt_nats_broker_format`
  (format `1`) beside the recovery epoch, and a missing or other value is refused at startup
  before the shape is judged; the format's definition, change rules and golden fixtures
  (`internal/mqtt/testdata/storeformat/`, regenerated until format 1 freezes at the first
  release) are in `docs/ARCHITECTURE.md` "The persisted format". Done 2026-11-06.

- [x] **Broker-to-broker protocol.** `mqtt_nats_broker_format` versions the protocol too:
  the store, peers' decision rules, the live `Nmqtt-*` envelope and derived names; same
  version coexists, a bump is an offline transition; channels pinned by
  `broker_protocol_channels_test.go` (`docs/ARCHITECTURE.md` "The broker-to-broker
  protocol"). Done 2026-11-06.

- [x] **NATS dependency floor.** Contract (version, JetStream API level 5, settings) in
  `docs/ARCHITECTURE.md` "Compatibility boundary", read from the linked module; startup
  refuses the dialed server's version and header support and the meta leader's API level.
  Done 2026-11-08.

- [x] **Configuration surface.** One settings table defines every flag and `MQTTNB_`
  variable, read strictly; `internal/config/testdata/surface.txt` pins names, kinds and
  defaults, and `values.schema.json` the chart's values; no deprecation path before the
  first release (`docs/ARCHITECTURE.md` "The configuration surface"). Done 2026-11-08.

- [x] **Permission contract.** One definition (`internal/mqtt/permission_contract.go`) of
  the MQTT identities' denies, the service identity's grant per auth-passthrough mode and
  domain, and the no-response-permissions rule; shipped ACLs checked against it, a runtime
  witness under the exact grant, a `$`-root census; the JetStream reply token no longer
  breaks on a dotted host name (`docs/ARCHITECTURE.md` "The permission contract"). Done
  2026-11-08.

- [ ] **Operational interface.** Freeze what `/healthz` and `/readyz` mean (including
  their bodies), the exit codes, and the JSON log fields operators can alert on.

## 2. Make the MQTT core maintainable

- [x] Split `internal/mqtt/mqtt.go` along stable responsibilities, move-only: `mqtt.go`
  plus 27 files named by responsibility (`docs/ARCHITECTURE.md` "Source layout of the
  protocol core"). Done 2026-10-17.

- [x] Keep behavior unchanged during the split, proven mechanically per extraction batch;
  the source guards now scan every production file (`censusProductionFiles`,
  `censusFuncSource`), so a future move cannot narrow them.

- [x] Preserve provenance for code derived from NATS Server: every split file keeps the
  NATS Authors' Apache-2.0 header, and NOTICE, `internal/mqtt/doc.go`, `CONTRIBUTING.md`
  and `docs/PORTING.md` name the file family.

- [x] Do not preserve file layout for an upstream diff: maintainability of this broker is
  the controlling requirement, and upstream fixes are forward-ported by symbol.

- [x] Decided 2026-10-17 not to adopt a `nats.go/jetstream` transport boundary: `mqttJSA`
  stays the MQTT operation layer, and any future library request on the service connection
  must reply under a reserved prefix and keep per-client authorization
  ([issue #132](https://github.com/nberlee/mqtt-nats-broker/issues/132)).

### Dead-code and reachability cleanup

Each checkbox below is intentionally scoped as one reviewable commit. Complete
them in order where a later lint change depends on an earlier cleanup.

- [x] Connect MQTT option validation to production startup: one `ValidateOptions`, run by
  `Start` and by the command before NATS or the health port. Done 2026-10-17
  ([issue #56](https://github.com/nberlee/mqtt-nats-broker/issues/56)).

- [x] Remove independent dead declarations
  ([issue #57](https://github.com/nberlee/mqtt-nats-broker/issues/57)).

- [x] Remove the obsolete upstream configuration-reload, embedded-routing and
  retained-permission rescan paths
  ([issue #57](https://github.com/nberlee/mqtt-nats-broker/issues/57)).

- [x] Move test-only helper APIs out of production files. Done 2026-10-18
  ([issue #58](https://github.com/nberlee/mqtt-nats-broker/issues/58)).

- [x] Collapse the constant-state v5 capability (`srvMQTT.v5Enabled` deleted). Done
  2026-10-18 ([issue #58](https://github.com/nberlee/mqtt-nats-broker/issues/58)).

- [x] Delete the parked WebSocket placeholder path from production flow. Done 2026-10-18
  ([issue #58](https://github.com/nberlee/mqtt-nats-broker/issues/58)).

- [x] Remove the upstream-only QoS and `MaxProtocolVersion` switches: a reference test
  does not justify a mode no deployment can select; 3.1.1 and 5.0 are accepted, anything
  else refused, and `$MQTT_msgs` and `$MQTT_rmsgs` require `AllowMsgTTL` on every broker.
  Done 2026-10-18 ([issue #58](https://github.com/nberlee/mqtt-nats-broker/issues/58)).

- [x] Delete the option fields production never reads: `Options` and `MQTTOpts` hold only
  what the broker reads. Done 2026-10-19
  ([issue #58](https://github.com/nberlee/mqtt-nats-broker/issues/58),
  [issue #137](https://github.com/nberlee/mqtt-nats-broker/issues/137)).

- [x] Re-enable reachability lint for the protocol core: no file is excluded from any
  analyzer, `make lint-go` runs four clean passes, and the exceptions are four
  `//nolint:unused` named zero values. Done 2026-10-19
  ([issue #59](https://github.com/nberlee/mqtt-nats-broker/issues/59)).

- [x] Remove the JetStream request leftovers the query audit found; every JS API reply is
  `$MQTT.JSA.<node id>.<kind>.<nuid>`. Done 2026-10-20
  ([issue #132](https://github.com/nberlee/mqtt-nats-broker/issues/132)).

- [x] Delete `-consumer-replicas`: consumers on the interest-retention streams inherit the
  stream's replica count, and the flag now fails startup as undefined. Done 2026-10-25.

- [x] Correct the code comments the dead-text cut found stale, by semantic family; #154's
  retained-identifier residual dropped (no population reaches it). Done 2026-11-02
  ([#147](https://github.com/nberlee/mqtt-nats-broker/issues/147),
  [#149](https://github.com/nberlee/mqtt-nats-broker/issues/149),
  [#150](https://github.com/nberlee/mqtt-nats-broker/issues/150),
  [#152](https://github.com/nberlee/mqtt-nats-broker/issues/152),
  [#153](https://github.com/nberlee/mqtt-nats-broker/issues/153),
  [#154](https://github.com/nberlee/mqtt-nats-broker/issues/154)).

- [x] Give the withheld-marker recordings discriminating tests (the refused-restore test
  already asserted its marker; the timed-out verdict and a detached delivery now assert
  theirs) and delete the unreachable per-subscription phase-gate wait. Done 2026-11-02
  ([issue #158](https://github.com/nberlee/mqtt-nats-broker/issues/158)).

- [x] Decided fork repair keeps excluding shared-subscription membership: it names ledger
  acquisitions, not a grant term; the loss is a stated residual. Done 2026-11-02
  ([issue #159](https://github.com/nberlee/mqtt-nats-broker/issues/159)).

- [x] Pin `commitUnsub`'s resolver token on the real shared and plain UNSUBSCRIBE release
  paths: a verdict the release newly stages must be credited before the resolver's transaction
  closes, or a crash can strand its identifier. Done 2026-11-03
  ([issue #160](https://github.com/nberlee/mqtt-nats-broker/issues/160)).

- [x] Corrected the comments naming identifiers that no longer exist, by census over every
  Go file (stale doc heads and stranded docs included), and stated the teardown purge as
  the obligation it discharges rather than a late-cancellation history. Done 2026-11-03
  ([issue #161](https://github.com/nberlee/mqtt-nats-broker/issues/161),
  [issue #167](https://github.com/nberlee/mqtt-nats-broker/issues/167)).

### Performance and resource audits

- [x] Classify every bounded re-attempt loop (`bounded_reattempt_census_test.go`) and give
  the admission ladder `mqttForegroundAcquireAttempts` with a wait after a commit-stage
  loss. Done 2026-10-19
  ([issue #116](https://github.com/nberlee/mqtt-nats-broker/issues/116)).

- [x] Convert the five foreground authority loops the classification owed, each announced
  and on the foreground budget; the bound's limits are in `docs/CONFORMANCE.md`. Done
  2026-10-20 ([issue #116](https://github.com/nberlee/mqtt-nats-broker/issues/116)).

- [x] Eliminate the shared `$MQTT_out` consumer enumeration: PUBREL orphans are discovered
  through bounded ownership-record reads (`mqttPubRelSweepAllowance`), and `consumerList`
  pages by distinct names. Done 2026-10-20.

- [x] Audit every JetStream query and request pattern, as a record: one disposition per
  pattern; the leader-routed management GET stays for correctness-sensitive loads, and the
  service connection's library replies use `$MQTT.inbox`. Done 2026-10-20
  ([issue #132](https://github.com/nberlee/mqtt-nats-broker/issues/132)).

- [x] Settle the JetStream API error contract on no responders: a `Status` reply is an
  ambiguous `ApiError`, and a double ack fails on it. Done 2026-10-20
  ([issue #132](https://github.com/nberlee/mqtt-nats-broker/issues/132)).

- [x] Evaluate pipelining the inbound QoS 1/2 ack paths. Decided 2026-10-21 by the user:
  GO, adaptive per connection, with no per-installation switch unless the adaptive design
  cannot meet the bound; 8416's dedup/discard protocol is rejected
  ([issue #132](https://github.com/nberlee/mqtt-nats-broker/issues/132),
  [issue #145](https://github.com/nberlee/mqtt-nats-broker/issues/145)).

- [x] Implement the inbound acknowledgement pipeline, adaptive per connection
  (`docs/ARCHITECTURE.md` "The inbound acknowledgement pipeline"). Done 2026-10-21
  ([issue #132](https://github.com/nberlee/mqtt-nats-broker/issues/132)).

- [x] Benchmark the JetStream paths (`make bench-jspaths`); a query path changes only when
  measurements show an improvement without weakening MQTT semantics, authorization,
  consistency, or HA behavior. Done 2026-10-22
  ([issue #132](https://github.com/nberlee/mqtt-nats-broker/issues/132)).

- [x] Audit reclamation of the per-CONNECT permanent records: all three classes stay
  retained; reopening needs a protocol that makes every delayed Will durably
  discoverable, fences every live writer and reader of all three classes, and covers them
  together. Done 2026-10-23
  ([issue #133](https://github.com/nberlee/mqtt-nats-broker/issues/133)).

- [x] Scan memory and resource lifetimes: seven defects fixed, the eighth a residual in
  `docs/CONFORMANCE.md`. Done 2026-10-23
  ([issue #138](https://github.com/nberlee/mqtt-nats-broker/issues/138)).

- [x] Add churn and soak tests (`make resource-soak`), the baseline being each resource's
  expected value. Done 2026-10-23
  ([issue #138](https://github.com/nberlee/mqtt-nats-broker/issues/138)).

- [x] Remove the measured causes of the QoS 1 delivery cap of about 5 msg/s
  (`make bench-outbound`); the per-subscriber ceiling is a residual in
  `docs/CONFORMANCE.md`. Done 2026-10-26
  ([issue #132](https://github.com/nberlee/mqtt-nats-broker/issues/132)).

- [x] Raise the per-subscriber delivery ceiling by changing the delivery protocol; stated
  contract change: a corrupted cached duty is not re-detected by the identifier walk. Done
  2026-10-27 ([issue #132](https://github.com/nberlee/mqtt-nats-broker/issues/132)).

- [x] Admit several deliveries per subscription in one batch: batched admission and
  acquisition, boundary reads prefetched, and a retiring incarnation stopping its delivery
  callback's work. Batching back-to-back acknowledgement advances was deferred 2026-10-29;
  the completion carrying the exchange it already read and the per-PUBACK authority read
  stay unbuilt. The standalone atomic-batch latency (file storage) is a candidate upstream
  performance report. Done 2026-10-30
  ([issue #132](https://github.com/nberlee/mqtt-nats-broker/issues/132)).

- [x] Brought per-session memory under control: a disconnected client is released while its
  session stays (session state names connections by weak pointer; finished retained work
  releases them), the read buffer follows traffic from 512 bytes, `-max-ack-pending`
  sets the QoS 1/2 push window (a scale, not a backlog bound) and `-pprof-addr` serves
  loopback profiles. Done 2026-11-05
  ([#174](https://github.com/nberlee/mqtt-nats-broker/issues/174),
  [#175](https://github.com/nberlee/mqtt-nats-broker/issues/175),
  [#176](https://github.com/nberlee/mqtt-nats-broker/issues/176)).

- [x] A slow CONNECT names its steps: a CONNECT reaching 250 ms to its CONNACK logs one
  diagnostic line with every step's duration. A clean close's continuation walks its
  inbound QoS 2 holds at a provisional width of 8 once the gate is released, which cut
  the measured CONNECT slowdown during a predecessor's reclamation; adopters and the
  sweep keep width 32 (`docs/ARCHITECTURE.md`). Done 2026-11-07.

- [ ] Overlap a clean-start CONNECT's independent JetStream reads. Its requests run one
  at a time (about 22 reads beside three durable commits); the reads that do not depend
  on each other can be issued together, shortening the CONNECT on slow storage and the
  number of serial waits a stalled store can hit.

### Connection close

- [x] Carry the error-DISCONNECT linger to the inbound-pipeline worker's failure arm, by
  handing the close to the reader. Done 2026-10-30
  ([issue #132](https://github.com/nberlee/mqtt-nats-broker/issues/132)).

### Outbound retransmission

- [x] Stop retransmitting an unacknowledged delivery on the same MQTT 5.0 connection
  (PUBREL in scope); by decision, MQTT 3.1.1 keeps the inherited retry every AckWait.
  Done 2026-10-31.

### Intermittent reds

- [x] Repair three pre-existing intermittent reds: two fixture repairs, and foreground
  priority held by one coordinator per client identifier. Done 2026-11-01.

- [x] `TestMQTTReconcileRegisterIdentityInvariant`'s lost third CONNECT was a fixture: an
  unfenced close let the next CONNECT take over, and the flapper jail closed the one
  after. Every plain close before a same-ID reconnect in that file is now fenced, and the
  tests that lacked the fence also fail on a takeover; the refused-resume test there waits
  for its withheld delivery before reconnecting. Done 2026-11-03
  ([issue #164](https://github.com/nberlee/mqtt-nats-broker/issues/164)).

- [x] `TestWillPendingArmAbortsThePassOnATransientFactOutage` and
  `TestMQTTWillOutboxReplicatedTerminalDoneStands` were fixtures: a claim loop's late initial
  pass acted on the seeded records. The two-broker environment now awaits both initial passes,
  and every Will test that seeds a record a scan could act on keeps the loops' ticks out.
  Done 2026-11-03 ([issue #165](https://github.com/nberlee/mqtt-nats-broker/issues/165),
  [issue #169](https://github.com/nberlee/mqtt-nats-broker/issues/169)).

- [x] `TestMQTTInboundPipeRegistrationsAndJoin`'s shutdown-abandoned store was a fixture:
  its drop seam swallowed every request, so a Will claim scan caught in the window held
  shutdown past the store's own timeout. The seam now drops only that store, whose wait
  outlasts a healthy shutdown, and the server is released only after shutdown completes.
  Done 2026-11-03 ([issue #168](https://github.com/nberlee/mqtt-nats-broker/issues/168)).

- [x] Three more reds were test defects, each reproduced on demand: the peer-witness
  recorder dropped a transition whose reply was lost though it landed, and now decides it
  by its outcome record; the WebSocket Pong wait parsed a half-written frame as final; and
  the three `MaxConn = 1` tests ran in parallel, where another test's dial to a reused port
  took the one slot, so they run sequentially. Done 2026-11-07.

### NATS platform follow-ups

- [x] Move the pin and the floor to nats-server 2.15.2 and the toolchain to Go 1.27; the
  chart's optional NATS dependency and the shared-pod example override the server image
  to `synadia/nats-server:2.15.2-alpine3.22`. Done 2026-10-27.

- [x] Enable `js_ack_fc_v2` in clustered CI (`TestMQTTAckReplyLayoutV2Replicated`); a
  service grant positional on the v1 layout silently denies every v2 acknowledgement
  (`docs/ARCHITECTURE.md` "Recommended NATS permissions").

## 3. Add MQTT transports

### TLS and certificate rollover

- [x] Support MQTT TLS and mutual TLS as tested production listener modes.

- [x] Expose TLS on a second listener, or document why a single configurable listener is
  sufficient. Decided one listener: `-port` selects it in either mode, with no implicit
  port change when TLS flags are set, and TLS deployments use `-port 8883`. The ruling
  that 1883 and 8883 cannot be served at once was reversed on 2026-10-25 by the item
  below.

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

- [x] Prove the reload's resource lifetime: superseded configurations, client-CA pools and
  the watcher are released, repeated reloads under connection churn leave memory,
  goroutines and descriptors without sustained growth, and the reload churn runs in
  `make resource-soak`. Done 2026-10-23 with the six items above as `internal/tlsreload`
  (`docs/ARCHITECTURE.md` "TLS material reload").

- [x] Serve plaintext and TLS MQTT at once, for example 1883 and 8883. `-port` keeps its
  meaning (plaintext, or TLS when `-tls-cert`/`-tls-key` are set), so the "no implicit
  port change" rule holds, and `-plain-port` adds a plaintext listener beside TLS. Done
  2026-10-25 (`docs/ARCHITECTURE.md` "Plaintext MQTT beside TLS").

### WebSocket

- [x] Implement MQTT over WebSocket and secure WebSocket.

- [x] Define listener, path, origin, proxy-header, timeout, and TLS settings.

- [x] Add native integration tests for MQTT 3.1.1 and 5.0 over WebSocket.

- [x] Replace or retire the parked upstream WebSocket reference tests once equivalent
  behavior is covered. Done 2026-10-24 with the three items above: `-ws-port` serves MQTT
  over WebSocket and WSS (`docs/ARCHITECTURE.md` "MQTT over WebSocket"), and the parked
  `mqtt_ws_ref_test.go` and its build tag are deleted.

## 4. Build the runtime image

- [x] Build the runtime image `FROM scratch`, as the external-NATS image: the static
  binary and the CA bundle only. Done 2026-10-24 (`docs/ARCHITECTURE.md` "The runtime
  image").

- [x] Copy the system CA bundle into the runtime image, at
  `/etc/ssl/certs/ca-certificates.crt`; a private NATS CA is a mounted file named by
  `SSL_CERT_FILE`. Done 2026-10-24.

- [x] Run as a numeric non-root user with a read-only root filesystem: `USER 65532:65532`,
  and `-nats-creds` takes an absolute path since `HOME` is unset. Done 2026-10-24.

- [x] Verify outbound TLS connections to NATS, mounted MQTT TLS Secrets, health probes,
  graceful shutdown, and signal handling: `test/imagesmoke` (`make image-smoke-external`).
  Done 2026-10-24.

- [x] Build and smoke-test linux/amd64 and linux/arm64 images in CI: the `image-external`
  job, on a native runner per platform. Done 2026-10-24.

- [x] **Embedded NATS: an in-process JetStream mode and a second production image**, by
  the user's decision of 2026-10-24: the broker with NATS embedded, as its own Deployment
  with NATS elsewhere, or as an extra container in the NATS chart's pods, each
  single-server and clustered. `-nats-embedded-config` and the `embedded` image target
  (tags suffixed `-embedded`). Done 2026-10-24.

## 5. Package for Kubernetes

### Kubernetes manifest

- [x] Replace JSON-like flow mappings and sequences in the example manifest with readable
  block-style YAML: the kustomize tree `docs/deploy/kubernetes`. Done 2026-10-24.

- [x] Set the effective Go memory limit to 90 percent of the container memory limit,
  derived by the binary from `resources.limits.memory`, not carried in the manifest or a
  chart. Done 2026-10-17.

- [x] Include secure defaults, probes, resource requests and limits, graceful termination,
  and optional TLS Secret mounts (`docs/ARCHITECTURE.md` "The example Kubernetes
  deployment"). Done 2026-10-24.

### Helm chart

- [x] Add a production Helm chart for the broker: `charts/mqtt-nats-broker`, customizable
  the way the official NATS chart is, and a bare install is one embedded broker, both by
  the user's decision. Done 2026-10-25 (`docs/ARCHITECTURE.md` "The Helm chart").

- [x] Include Deployment, Service, TLS mounts, probes, resources, security context,
  autoscaling options, PodDisruptionBudget, and graceful termination. Done 2026-10-25.

- [x] Support an external NATS/JetStream cluster. **Done 2026-10-25**: `topology:
  external` with `broker.natsURLs`.

- [x] Add the official NATS chart as an optional dependency (`condition: nats.enabled`); a
  clustered dependency is refused without authenticated routes. Done 2026-10-25.

- [x] Expose the dependency configuration under `nats:` so users can place
  normal NATS chart values there without translating every field. **Done 2026-10-25**.

- [x] Offer the broker as an extra container in the official NATS chart's pods:
  `docs/deploy/nats-chart-shared-pod`. Shared-pod startup is unordered, a stated residual
  in `docs/CONFORMANCE.md`. Done 2026-10-25.

- [x] Add Helm lint, template, install, upgrade, rollback, and optional-NATS tests to CI:
  `make helm-check` and `make helm-smoke`. Done 2026-10-25.

- [x] Package and publish the chart as an OCI artifact in GitHub Container Registry only
  as part of an alpha release: release wiring, nothing published until the alpha tag, and
  the GHCR push first exercised by that tag. Done 2026-10-25.

## 6. Document migration and security

- [x] Write a migration guide from NATS Server's embedded MQTT listener to this standalone
  broker: `docs/MIGRATION.md`; nothing persisted migrates. Done 2026-10-25.

- [x] Cover listener cutover, DNS or load-balancer changes, expected downtime, rollback,
  and verification, in `docs/MIGRATION.md`.

- [x] State explicitly whether sessions, retained messages, QoS state, and delayed Wills
  can be migrated. Do not imply persisted-state compatibility where none exists. None can
  (`docs/MIGRATION.md`).

- [x] Explain service credentials, anonymous-client defaults, and per-client
  authentication passthrough separately: `docs/AUTHENTICATION.md`.

- [x] Provide NATS user/password and JWT/account examples for MQTT authentication and
  publish/subscribe authorization: `docs/deploy/nats-auth/nats.conf` and an operator-mode
  auth callout, each run by a test.

- [x] Document MQTT-topic to NATS-subject mapping and denial of the internal
  `$MQTT.>` namespace. See "Reserved `$MQTT.>` namespace" in
  `docs/ARCHITECTURE.md`; the migration guide should link it rather than restate
  it.

- [x] Include Kubernetes Secret and live TLS rollover examples: `docs/AUTHENTICATION.md`,
  "Kubernetes".

## 7. Finish provenance and release documentation

### Remove porting-era terminology

- [x] Rename the `mqttport_fulltests`, `mqttport_clustertests`, and `mqttport_tlstests`
  build tags and CI references. Done 2026-10-25.

- [x] Preferred replacements: `mqtt_fulltests`, `mqtt_cluster_reference`,
  `mqtt_tls_reference` and `mqtt_experimental_shared_qos2`, with no aliases
  (`CONTRIBUTING.md`, "Test suites and build tags").

- [x] Remove `port`, `ported`, and `porting` as project-identity language; kept are
  "forward-port" as the verb for syncing a fix and every networking sense.

- [x] Remove "bridge" as project-identity language, keeping it as a verb and as MQTT
  broker-bridging terminology; renaming the checkout directory and the GitHub repository
  is the owner's step from section 1.

- [x] Keep technically necessary networking terms such as listener port, `containerPort`,
  and port numbers.

- [x] Replace `docs/PORTING.md` before alpha with concise provenance documentation:
  `docs/PROVENANCE.md`, with no redirect stub.

- [x] Keep attribution in NOTICE and the README after removing the porting
  narrative. NOTICE is unchanged; the README's attribution also links
  `docs/PROVENANCE.md`.

### Release documentation

- [x] Remove the pre-release changelog before the first alpha. Done 2026-10-26.

- [ ] Start a changelog with the first published alpha, or generate release
  notes from Git history and pull requests. **Decided 2026-10-26: a hand-written
  changelog.** `CHANGELOG.md` in Keep a Changelog format starts again with a versioned,
  dated entry for the first published alpha, committed before the alpha tag so the tagged
  source carries it. The GitHub release body is that entry, copied at release time:
  `.github/workflows/release.yml` sets no body and generates none. Generated release notes
  are not adopted, because GitHub builds them from merged pull requests and this
  repository's history is direct commits. Open until the alpha is published.

- [x] Replace the README's duplicate roadmap with a short link to this file.

- [ ] Make version-controlled files under `docs/` the source of truth for a
  GitHub Wiki. **Implemented 2026-10-26; open until the first publication is verified.**
  `docs/WIKI.md` is the Home page and its `## Pages` list is the manifest; `cmd/wikigen`
  renders the listed documents, splitting one over the 400,000-byte page budget at its
  sections, rewriting links to pages, anchors and commit-pinned repository URLs, and
  `TestRepositoryDocsRenderAsWiki` fails `make test` on a broken link, a missing anchor,
  Markdown outside the published subset (judged by a GFM parse: no raw HTML,
  reference-style links or footnotes, plain headings) or an oversized section
  (`docs/ARCHITECTURE.md`, "The wiki is generated from docs/").

- [ ] Publish the wiki from CI so it cannot drift from repository docs. **Implemented
  2026-10-26; open until the first publication is verified.** The release workflow's
  `wiki` job renders each release tag and `wikigen publish` pushes it, guarded by the
  wiki's `.wiki-publication` record so an older release or a moved tag never replaces
  newer pages; a rejected push is re-judged against what landed. The wiki mirrors the
  newest release, so `docs/` runs ahead of it between releases, and web edits are
  overwritten at the next one. Not verifiable before a release: the push with
  `GITHUB_TOKEN` and GitHub's rendering of the pages, which waits on "Before the first
  push" below and the alpha tag. The wiki repository the release pushes to exists: its
  first page was created in GitHub's web UI by 2026-11-01.

- [ ] Include installation, configuration, architecture, TLS, Helm/Kubernetes,
  authentication and authorization, migration, operations, troubleshooting, conformance,
  and provenance pages. **Implemented 2026-10-26; open with the two items above.** New
  `docs/INSTALLATION.md`, `CONFIGURATION.md`, `TLS.md`, `KUBERNETES.md`, `OPERATIONS.md`
  and `TROUBLESHOOTING.md` take the README's installation, flag table, TLS and WebSocket
  sections (the README keeps a short quickstart and links); the chart README and the two
  deploy READMEs are published as they are.

- [x] Every finding is a GitHub issue, and every reference to one points at its issue: #3
  to #145, label `finding`, and no mapping file (the issue titles are the mapping). Done
  2026-11-01; no identifier carries a finding number since 2026-11-03.

- [x] Cut the dead text from `docs/ROADMAP.md`, `docs/CONFORMANCE.md`,
  `docs/ARCHITECTURE.md` and `docs/known-upstream-issues.md`: each keeps only text that
  describes the current broker or open work, a checked roadmap item is one line, and the
  cut analysis stays in git history, linked from its finding's GitHub issue where one fits.
  Done 2026-11-01.

- [x] Add the clean-start predecessor cleanup to `docs/CONFORMANCE.md`'s callers of the
  teardown reap, with the sweep's closure licence and the displacement arm. Done 2026-11-03
  ([issue #148](https://github.com/nberlee/mqtt-nats-broker/issues/148)).

- [x] Settle what a cold peer resume does with a landed never-sent obligation: withdrawn
  over a gone source, restored and continued over a live one, both arms witnessed. Done
  2026-11-04 ([issue #151](https://github.com/nberlee/mqtt-nats-broker/issues/151)).

### Before the first push

- [x] Nothing is pushed to the GitHub remote until commit signing is set up and the
  branches are cleaned up: one branch, `main`, every commit SSH-signed. Done 2026-10-26.

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

- [x] Every finding is a GitHub issue and the repository holds no finding ID, no
  findings-directory path and no test file named for a finding (section 7, "Release
  documentation"). Done 2026-11-01.

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
  epochs ([issue #70](https://github.com/nberlee/mqtt-nats-broker/issues/70)).
  Skew WITHIN one incarnation is undetectable, so the procedure has to
  guarantee the coherent instant rather than rely on the check.

- [ ] Release artifacts use the final product name, `mqtt-nats-broker`,
  consistently (section 1, "Product name").

## 8. Post-alpha candidates and scope cleanup

Nothing here blocks the alpha: the alpha release gate requires only that every item has an
explicit disposition. It sits after the gate so that document order reaches it only once
the alpha work in sections 0 to 7 is done.

- [ ] Decide after the alpha release whether a renamed or removed setting gets a deprecation
  period (`docs/ARCHITECTURE.md`, "The configuration surface").

- [ ] **Report INPROCESS-REFUSAL to nats.go and nats-server, after the alpha release.**
  Filed 2026-10-24 by the user's decision. A connect the server refuses over an
  in-process connection waits out nats-server's 10 s write deadline and reports `io:
  read/write on closed pipe` instead of the refusal. In embedded mode, a wrong MQTT
  credential is therefore refused about 10 s late. The mechanism, the source locations
  and the witness are in `docs/known-upstream-issues.md` (INPROCESS-REFUSAL); the residual
  is stated in `docs/CONFORMANCE.md`. The user reads, verifies and writes the reports
  personally, because the NATS maintainers do not want AI-written issues. The witness
  (against nats-server v2.15.1 and nats.go v1.54.0) and the stack notes are the material
  for that.
  Before reporting, re-run the witness against the then-current pin and check both
  projects' `main` and issue trackers: a fix upstream turns this into a pin bump. Not an
  alpha blocker: the refusal still happens, only late.

- [ ] **Sweep the documentation corpus for stale
  [issue #76](https://github.com/nberlee/mqtt-nats-broker/issues/76) status claims, by
  MANIFEST rather than by search.** Filed 2026-09-10 by the
  [issue #99](https://github.com/nberlee/mqtt-nats-broker/issues/99)/
  [issue #90](https://github.com/nberlee/mqtt-nats-broker/issues/90) split filing. **Not
  an alpha blocker**: it is documentation consistency about existing findings, not a newly
  demonstrated broker defect.

  **The authoritative status, stated here so this bullet cannot itself misdirect planning:
  [issue #76](https://github.com/nberlee/mqtt-nats-broker/issues/76) LANDED
  2026-09-09. [issue #99](https://github.com/nberlee/mqtt-nats-broker/issues/99) and
  [issue #90](https://github.com/nberlee/mqtt-nats-broker/issues/90) closed together
  on 2026-10-11.**

  **An author-selected list of relevant regions does not prove a superset.** A complete
  file manifest supplies the finite outer boundary; semantic review still supplies the
  evidence.

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
  and their actual dispositions belong to the sweep: `docs/ROADMAP.md`'s
  [issue #76](https://github.com/nberlee/mqtt-nats-broker/issues/76) bullet;
  [issue #136](https://github.com/nberlee/mqtt-nats-broker/issues/136)'s
  [issue #76](https://github.com/nberlee/mqtt-nats-broker/issues/76) entry;
  `docs/ARCHITECTURE.md`'s [issue #76](https://github.com/nberlee/mqtt-nats-broker/issues/76)
  sections;
  [issue #144](https://github.com/nberlee/mqtt-nats-broker/issues/144)'s
  [issue #76](https://github.com/nberlee/mqtt-nats-broker/issues/76) entry;
  `docs/CONFORMANCE.md`'s
  [issue #76](https://github.com/nberlee/mqtt-nats-broker/issues/76) sections;
  [issue #141](https://github.com/nberlee/mqtt-nats-broker/issues/141)'s
  [issue #54](https://github.com/nberlee/mqtt-nats-broker/issues/54) material;
  [issue #131](https://github.com/nberlee/mqtt-nats-broker/issues/131)'s
  [issue #89](https://github.com/nberlee/mqtt-nats-broker/issues/89)/
  [issue #90](https://github.com/nberlee/mqtt-nats-broker/issues/90)/
  [issue #99](https://github.com/nberlee/mqtt-nats-broker/issues/99) material;
  [issue #134](https://github.com/nberlee/mqtt-nats-broker/issues/134)'s unsent-marker
  section; and `docs/CONFORMANCE.md`'s restore-gate residual. **Note that the filing change
  already touched several of these** — it corrected
  [issue #54](https://github.com/nberlee/mqtt-nats-broker/issues/54) and
  [issue #89](https://github.com/nberlee/mqtt-nats-broker/issues/89)/
  [issue #90](https://github.com/nberlee/mqtt-nats-broker/issues/90) material and
  introduced [issue #99](https://github.com/nberlee/mqtt-nats-broker/issues/99)
  — so they are not untouched pre-existing text and must not be audited on
  that assumption.

  **Already corrected by the filing change, and not to be re-done blindly:** the
  [issue #76](https://github.com/nberlee/mqtt-nats-broker/issues/76)
  entries in
  [issue #144](https://github.com/nberlee/mqtt-nats-broker/issues/144) and
  [issue #136](https://github.com/nberlee/mqtt-nats-broker/issues/136);
  [issue #76](https://github.com/nberlee/mqtt-nats-broker/issues/76)'s own `[x]`
  roadmap bullet (its pre-closure diagnosis and design now live in the issue);
  `docs/ARCHITECTURE.md`'s rebind-race DUP consequence (the race
  itself STANDS and was deliberately not deleted); and `docs/CONFORMANCE.md`'s restore-gate
  first-transmission DUP assertions and its affine paragraph. **Deliberately NOT retracted
  anywhere, and still open:** the lost prompt NAK, the AckWait delay, the ordering residual,
  and the displaced-broker window.

- [ ] **Sweep the comment corpus for six false-claim families, indexed by CLAIM rather
  than by phrasing.** Filed 2026-08-26. **Not an alpha blocker**: every instance is a
  comment or a doc sentence, and the code was confirmed sound. A claim written five
  different ways is not greppable, so the families are enumerated by claim, not by grep
  pattern. The families are: **(1) the lifetime-restoration fiction**: comments saying
  `abortAcceptedResume` puts the session lifetime back. It does not: it abandons the
  restore gate and calls `mqttAbortAcceptedConnect`. What protects the lifetime is
  ORDERING, because `applyRequestedLifetime` runs after the earlier refusal sites. Some of
  these comments name mutations that **cannot be written** (`restoreRefusedResumeLifetime`
  does not exist). **(2) the
  [issue #86](https://github.com/nberlee/mqtt-nats-broker/issues/86) ownership
  inference**: verdict comments asserting that a rejected CAS, an absent record or a gone
  record means another owner holds the session. It means the attempted write did not land.
  This subset is [issue #86](https://github.com/nberlee/mqtt-nats-broker/issues/86)'s
  distinction and is what can mislead later Will/disposal work into treating a concession
  as a demonstrated takeover. **(3) refusal-signal contradictions**: comments still saying
  a pre-CONNACK refusal writes no packet, or that no packet can identify an unrestored
  filter, when the code three lines later builds that filter's key into the Reason String
  the v5 failure CONNACK carries. *Seed locations, a SAMPLE and explicitly not a census:*
  family 1 at `internal/mqtt/sess_resume_reconcile.go:407` and
  `internal/mqtt/sess_resume_reconcile_e2e_test.go:2696-2706`; family 2 at
  `sess_resume_reconcile.go:283` (`mqttRejectionForeign`, reached by
  `classifyRejectedProbe` from an ABSENT record), `shared_group_arbitrate.go:107` and
  `:149`, `sess_uncertain.go:419-421` and `:858-870`, `mqtt.go:12135` and `:13354`; family
  3 at `mqtt.go:12878` and `docs/ARCHITECTURE.md:3495-3497`. **(4) the unscoped retry
  premise**: comments stating that "every MQTT connection retries the whole
  initialization" without the scope that makes it true. It holds only until one attempt
  succeeds or `Start`'s own attempt fails and exits: `startMQTT` opens the listener before
  the eager initialization, so a CONNECT in that window drives initialization itself, and
  after any success the CONNECT path re-initializes nothing. The four known sites were
  corrected 2026-08-28; the family is what remains. **(5) the "`$MQTT_rmsgs` issues no
  delete API request" claim**: false, since the rollback has always issued
  `CONSUMER.DELETE` against `$MQTT_rmsgs`; what is true, and is what the corrected sites
  now say, is that the owner issues no MESSAGE delete, which is the only thing
  `DenyDelete` gates. The seven known sites were corrected by the ephemeral-consumer
  identity item, which needed a wrap- and phrasing-insensitive sweep (one instance was
  WRAPPED across two lines, one phrased passively); the family is what remains. **(6) the
  re-minted-generation safety claim**: `reopenQoS2ReleaseFence` justifies reopening the
  release fence inside `clear()` with "Safe because the incarnation has been re-minted by
  then, so a newly admitted release works on the fresh generation's subjects, which this
  teardown cannot name." **False in both directions.** A PUBREL derives its hold subject
  BEFORE it reaches `beginQoS2ReleaseOp`, so a release admitted after the reopen can name
  the RETIRED generation's subjects; and the fresh generation's subjects are not out of
  reach either: they are exactly what a displaced connection can write to, which is the
  single-broker silent loss
  [issue #40](https://github.com/nberlee/mqtt-nats-broker/issues/40) records. The
  family, not the one site: any comment asserting that a re-mint by itself puts the
  previous connection out of reach of the new incarnation's names.
  [issue #40](https://github.com/nberlee/mqtt-nats-broker/issues/40) owns the code fix;
  this bullet owns the corpus. *How to close it:* index by the CLAIM: read every comment
  that asserts one of the six, rather than grepping for the wording that happened to be
  used. **[issue #86](https://github.com/nberlee/mqtt-nats-broker/issues/86) cross-links
  family 2 only**, and stays scoped to successor evidence. For exact identifiers, use a
  **zero-match symbol closure** over the removed identifiers (including test-only ones, or
  a dead helper survives it), plus a **generated prose review set** from a normalized,
  word-bounded term list with every match dispositioned, its breadth measured rather than
  argued.

- [x] **Re-census [issue #37](https://github.com/nberlee/mqtt-nats-broker/issues/37)'s
  claim-transition and claim-reap route inventory from the tree.** Parked with the
  experimental shared-QoS-2 implementation in chapter P, not done; a lineage or TOCTOU
  audit must not trust the current numbers. The fenced/unfenced split and the
  [issue #69](https://github.com/nberlee/mqtt-nats-broker/issues/69) TOCTOU argument do
  not rest on the count. To close it: enumerate from the tree, disposition every site,
  and prefer a check that cannot drift (the affine consumer delete count, verifiable by
  `deleteConsumer(mqttOutStreamName, ...)`, is the model).

These do not block the alpha unless investigation shows that ordinary broker
operation can trigger correctness loss or unbounded resource growth.

- [ ] [issue #50: Split the in-flight stabilization
follow-ups](https://github.com/nberlee/mqtt-nats-broker/issues/50).
  Schedule the stale-metadata cleanup and settlement-fence work independently. The one
  alpha-time part, deciding the compatibility-sensitive schema names, is in §1 ("In-flight
  verdict schema names").

- [x] [issue #65: Send a CONNACK before closing an unauthorized
  CONNECT](https://github.com/nberlee/mqtt-nats-broker/issues/65): moved to §0.5 on
  2026-10-26 by user decision, one pre-alpha item with
  [issue #77](https://github.com/nberlee/mqtt-nats-broker/issues/77) and the shutdown
  closure certificates.

- [ ] [issue #87](https://github.com/nberlee/mqtt-nats-broker/issues/87): Carry
  the authorizing NATS account through the transport. The stopping rule for residual 7,
  verbatim, because paraphrasing it would soften the atomicity requirement: *carry the
  authorizing NATS account with every queued service and per-client write, including JSA
  requests and live fan-out, and validate it atomically at final transport emission after
  reconnect. Same-account reconnects must preserve pending work; changed or unknown
  accounts must withhold it without finalizing durable obligations, including QoS 0
  Wills.* This is the class residual 7 in `docs/CONFORMANCE.md` records, not a single
  window: the pre-write account check is repeated before every durable write and is atomic
  with none of them, and three layers can hold a write in between — nats.go's reconnect
  buffer on the service connection, the JSA send queue (which carries no record of the
  account that authorized each entry), and the per-client reconnect buffers. Removing one
  layer does not help, which bounds any plan here: disabling the service reconnect buffer
  narrows nothing, because the send queue behind it holds writes just the same, and it
  makes a QoS 0 Will fail outright on an ordinary blip so the outbox finalizes a Will that
  was never fanned out. The hazard needs an operator to repoint credentials mid-flight.
  Post-alpha because it is a transport change, and the check-not-transaction gap is stated
  rather than hidden.

- [ ] [issue #88](https://github.com/nberlee/mqtt-nats-broker/issues/88): Prove
  the Will authorization decision at the two discharge sites that no fixture reaches.
  **Test coverage, not a known broker gap.** The decision is captured at CONNECT and read
  at four sites; a per-site mutation census (forcing each site's verdict `true` and
  running the whole suite) shows the immediate path and the delayed-outbox gate each fail
  their paired fixture, while `mqttPublishOwedWill` (the lost-session-gate route) and the
  uncertainty-parked gate in `sess_uncertain.go` survive. Reaching those two needs a
  record-derived eviction and a session-record ambiguity respectively, neither drivable
  from a connection, so both want a seam-driven arm with the service identity established
  (`svcID.account`/`valid` plus the client's `authzSvcAccount`) and `willAuthorized` set
  on each side. An arm is only worth having if its own mutation kills it: one can pass
  with a live control and still reach the IMMEDIATE site. Stated as CONFORMANCE residual
  12 meanwhile.

- [x] [issue #53: Provide operator recovery for unreadable affine
  claims](https://github.com/nberlee/mqtt-nats-broker/issues/53): parked, not fixed
  (chapter P). No experimental-state recovery or migration machinery is in alpha scope;
  operators must perform a whole-bundle reset before leaving the experiment, and detecting
  or repairing incompatible experimental state is unsupported.

- [ ] Consolidate embedded NATS servers across the fork-repair test fixtures. **Test
  infrastructure, not a broker gap.** `sess_fork_repair_test.go` starts one embedded
  server per subtest through `testNewInflightEnv`/`runNATS`, roughly 37 for the file.
  Sharing one server per top-level test would cut ~25 of them, but it is NOT a harmless
  count optimization; the constraints a plan must answer, recorded in
  [issue #29](https://github.com/nberlee/mqtt-nats-broker/issues/29)'s implementation
  record: a fresh `Server`+account-manager+connection+node name per subtest (a shared asm
  lets `dropPersistObservations` installs clobber each other, and the hard-coded `node1`
  would make managers read each other's `Origin` writes as local), per-subtest quit
  channels so one sibling's cleanup cannot stop the shared server, a durable-name audit
  within each sharing group, and lingering watch/retained consumers accumulating in the
  shared streams up to the 5-minute inactivity threshold. Any fixture change re-runs
  [issue #29](https://github.com/nberlee/mqtt-nats-broker/issues/29)'s full mutation
  table, since shared state can mask a discriminator whose assertions are unchanged.

- [ ] Force two session-record proposals into the clustered pre-proposal window.
  **Test infrastructure, not a broker gap, and deliberately not a §0.2 blocker.** Its
  subject is the SAME-TERM clustered admission/apply window, which still exists: the
  in-process per-subject map refusing the second same-base write with 10164 is exactly
  what it targets. The CROSS-ELECTION consequence once attributed to it is retired (the
  same-base boundary in `docs/CONFORMANCE.md`); retiring this bullet would need its own
  audit of what a test here could still observe. `TestMQTTTakeoverCreateRaceReplicated`
  asserts the property on a real Replicas=3 stream — one winner, a definite loser, the
  winner's bytes surviving — but a barrier only makes both goroutines runnable, so it
  cannot hold one proposal between clustered admission and apply and release a second into
  that window. The mechanism itself is owned and tested upstream
  (`jetstream_batching.go:777-782`, with `jetstream_batching_test.go:1576-1586` and
  `jetstream_cluster_4_test.go:5158-5164`), which is why this is a limit on what a test
  HERE can observe rather than a conformance residual. Closing it needs a seam in
  nats-server, so an upstream issue is the likelier route than anything in this
  repository.

- [ ] After alpha, write an ADR for or against a schedule-backed delayed-Will timer.
  A schedule may publish only into its own stream, so the ADR must preserve
  [issue #35](https://github.com/nberlee/mqtt-nats-broker/issues/35)'s
  atomic `$MQTT_sess` record/acceptance fact and must not claim cross-stream
  cancellation. The broker, not the scheduler, remains responsible for MQTT publish
  semantics. See the
  [platform audit](https://github.com/nberlee/mqtt-nats-broker/issues/132).
