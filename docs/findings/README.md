# Finding dossiers

This directory is the canonical home for technical findings and their planning evidence.
The roadmap carries ordering, release treatment, and completion state; these dossiers
retain failure mechanisms, invariants, rejected approaches, tests, and residual risks.

Stable IDs do not change when a title, priority, or status changes. Plans, tests,
commits, and architecture notes may refer to a finding by its `F-###` identifier.

Since the 2026-09-02 product decision, open findings specific to session-affine shared
QoS 2 remain technically open but are parked under `docs/ROADMAP.md` chapter P. They do
not block the alpha's supported shared QoS 0/1 path, and an affine-only regression in a
general change is accumulated there rather than widening that change.

Cross-cutting JetStream feature decisions that are not independent correctness
findings live in the [NATS platform-adoption audit](nats-platform.md). Its actionable
work is merged into the existing finding or roadmap item whose invariant it serves.

What durable state this bridge holds, and which of its identities another stream
issues, is enumerated once in the
[persisted-identity census](persisted-identity-census.md). It is owned by F-034 and
cited by F-066. Its field-level dispositions are enforced by tests; its
interpretation-surface half is a drift guard rather than a closure proof, so it does
**not** discharge F-066's completeness deferral — which is why F-066 was ultimately
closed at STREAM granularity, where completeness is demonstrable, rather than over the
identities.

Why the per-CONNECT permanent records (acceptance facts, handoff receipts and F-105
transition outcomes) are retained rather than reclaimed is recorded in
[the reclamation audit](per-connect-record-reclamation.md), decided 2026-10-23. It has no
finding ID; the exposure it leaves is the accepted residual it links.

What every in-memory resource of the broker is owned by and ended by (goroutines, timers,
channels, NATS subscriptions and connections, and the server, account, session and
connection containers) is the [resource-lifetime census](resource-lifetime.md), done
2026-10-23 with its acceptance soak. Its defects are named D1 to D8 in that file rather than
given finding IDs; D1 is a stated residual and the rest are fixed.

The point-read absence census that once enumerated which decisions rest on a store read
that could not be completed (F-109) was retired on 2026-10-15; F-109 is upstream's, and the
broker does not compensate for it. See `docs/known-upstream-issues.md`.

## Active findings

- Preserve deferred disconnect state during foreign disposition.
  Status: Accepted residual (2026-10-12). Release treatment: Not an alpha blocker; stated
  residual in `docs/CONFORMANCE.md`.
  Details: [F-028](session-and-will.md#f-028).
- Bound the inbound QoS 2 hold purge and the discard marker it leaves.
  Closed 2026-08-18: every hold removal carries a proven cutoff, and a `discarded` marker
  records the cutoff its writer was entitled to purge through, so a later exchange can
  prove
  the marker is not about it.
  Details: [F-062](ha-arbitration.md#f-062).
- Add write lineage to shared QoS 2 assignment claims.
  Status: Closed 2026-08-18. Release treatment: Alpha blocker.
  Closed as an audit: no lineage was needed. It produced three findings, one per falsified
  premise — F-063 (retention evidence unbound from the source ack), F-064 (a live
  reference
  left to owner resume) and F-065 (a reap on a stale copy reading) — plus F-066, and one
  manifestation added to F-036. Unconditioned `CONSUMER.DELETE` on the interest-retained
  `$MQTT_out` is the enabling mechanism on some of those routes, not the premise of any.
  Details: [F-033](ha-arbitration.md#f-033).
- Prove a copy was retained before acknowledging its source.
  Status: Fixed 2026-08-19, guarantee re-scoped. Release treatment: Alpha blocker.
  Every route now proves the copy with a presence check and authorizes the release from
  `copyPresent ∨ AffineDone ∨ Discarded ∨ Lost ∨ SourceAcked`. The guarantee is
  conditioned
  on a fresh but non-atomic observation: `CONSUMER.DELETE` is unconditioned, so the
  residual
  is a consumer deleted between the final proof and the ack.
  Details: [F-063](ha-arbitration.md#f-063).
- Reconcile shared QoS 2 assignments independently of owner resume.
  Status: Open and partly remedied. Release treatment: Parked experimental shared-QoS-2
  work; post-alpha / maybe someday.
  Details: [F-064](ha-arbitration.md#f-064).
- Re-read copy absence after the reaper's own SourceAcked transition.
  Status: Fixed 2026-08-21, guarantee re-scoped. Release treatment: Alpha blocker.
  Details: [F-065](ha-arbitration.md#f-065).
- Make all nine broker-owned streams one recreation unit, so no stale cross-stream
  identity survives a supported recovery.
  Status: Closed 2026-09-01. Generalising the coupling was the intended remedy and is
  ABANDONED; the owner's 2026-08-31 ruling changed the recovery contract instead, and
  the enforcement has landed — an account-wide count preflight plus a recovery epoch
  stamped into all nine and compared by every validator. Independent single-stream
  recreation is withdrawn and refused.
  Release treatment: Alpha blocker, resolved.
  Details: [F-066](ha-arbitration.md#f-066).
- Prove a shared group durable exists before restoring its membership.
  Status: Closed 2026-08-23, guarantee scoped. Release treatment: Alpha blocker.
  The resume fall-through subscribed to a recorded deliver subject without ever proving the
  group durable still existed, so a durable deleted out of band left the client with Session
  Present and a subscription that never delivered. Now classified in two batched passes and
  refused when it cannot be proved. Establishes configuration evidence only, never ownership
  evidence — that half is F-077, which has since closed it for the CONNECT path and made an
  authoritatively absent durable repairable rather than merely refused.
  Details: [F-076](shared-qos2.md#f-076).
- Arbitrate ownership after any shared group durable create.
  Status: Closed — the CONNECT half landed first; the live SUBSCRIBE half is F-078, which
  has now landed and closes this.
  Release treatment: Alpha blocker.
  Split out of F-076, and a defect on `main` rather than one it introduced: no route that
  restored shared group interest revalidated that this broker still owned the membership it
  was keyed on. On the CONNECT path one classified record write now proves it before the
  CONNACK, and an authoritatively absent durable is repaired rather than refused. The orphan
  sweep does NOT discharge any of this, because it skips durables a resident session still
  references.
  Details: [F-077](shared-qos2.md#f-077).
- Arbitrate a live SUBSCRIBE's shared interest before it serves.
  Status: Closed 2026-08-25. Release treatment: Alpha blocker.
  The live half of F-077, split out by a non-convergence escalation. A live SUBSCRIBE
  installed and revived shared group interest with no ownership write, so a superseded
  broker could acknowledge group messages under a membership the record no longer carried.
  Closed by the same classified proof placed as the packet's last record write, plus a
  packet-scoped transmission gate the CONNECT path has no equivalent of.
  Details: [F-078](shared-qos2.md#f-078).
- A denied publish is accepted, stored and delivered.
  Status: Open. Release treatment: Alpha blocker.
  Auth passthrough enforces subscribe but not publish: core-NATS denial is asynchronous,
  so `PublishMsg` observes nothing, `pubAllowedFullCheck` always permits, and the
  `permIssue` arm is dead code. Every effect that does not ride the per-client
  connection proceeds -- the `$MQTT_msgs` store, the retained store, the QoS 2 hold, and
  internally replayed Wills (which keep their live fan-out too). Stated as a property,
  not a route list, because three review rounds each found another route.
  Details: [F-079](runtime-correctness.md#f-079).
- The authenticated identity's NATS account is never bound.
  Status: Open. Release treatment: Alpha blocker.
  Split out of F-079. Clients are bound to the synthetic service account before
  authentication, and a successful dial replaces only `c.nrc`, never `c.acc`. The
  documented v1 same-account constraint is unenforced, so credentials from another
  account authenticate while durable state lands in the service account.
  Details: [F-080](runtime-correctness.md#f-080).
- A subscribe deny overlapped by a wildcard is not enforced on delivery.
  Status: Open. Release treatment: Alpha blocker.
  Sibling of F-079 on the subscribe side. Enforcement stops at SUBSCRIBE: a filter
  denied outright gives SUBACK 0x80, but a wildcard overlapping a narrower deny is
  not filtered per message on the QoS 1/2 or retained paths, which ride the service
  connection. The inherited per-message guards are inert -- `mperms` has no writer
  anywhere and `checkDenySub` is a stub returning false.
  Details: [F-081](runtime-correctness.md#f-081).
- Release a failed shared filter's own state independently of the last-holder answer.
  Status: Closed. Release treatment: Alpha blocker.
  Split out of the last-holder fail-closed change by its plan review. The failed-filter
  release skipped its WHOLE body on a "held" answer, so a refusal withheld this session's
  own cleanup as well as the group delete -- and it re-derived that work from `sess.cons`,
  which the failing attempt has not written to yet, so on the common shape it discharged nothing
  either way and the orphan ack-budget charge permanently consumed the resident session's
  MaxAckPending budget. Closed by a three-way per-durable disposition (own-held, foreign or
  refused, unheld) with the release keyed on the group consumer's deterministic names.
  Details: [F-082](shared-qos2.md#f-082).
- Release UNSUBSCRIBE's own shared state independently of the last-holder answer.
  Status: Closed. Release treatment: Alpha blocker.
  Filed by F-082's consult as a separate caller with its own commit path. Three statements:
  the local durable-scoped purge is not gated the way the delete is, so leaving one filter
  strips state an overlapping surviving filter still needs; the staging half mixes a
  cons-keyed barrier with a durable-keyed void snapshot; and the no-cons branch discharges
  no local state at all.
  Details: [F-083](shared-qos2.md#f-083).
- Prove a successor before claiming a takeover.
  Status: **Done (2026-08-27).** Release treatment: Alpha blocker.
  The broker reported `0x8E` "session taken over by a new connection" — which tells a client
  to stop reconnecting — whenever a record CAS was rejected and nothing narrowed it to a
  sibling, including when the record was unreadable, absent, undecodable, released, at or
  below our own base, or a verbatim replay of our own bytes. **The successor predicate was
  avoided rather than found**: no persisted shape proves a successor, so the claim was
  withdrawn from every route that cannot hold one, and `0x8E` is now licensed only by an
  in-process same-client-ID replacement this broker holds both ends of. Everything
  record-derived says `0x89` ownership-unsettled. The Will half turned out to be **five
  routes, four of them real losses**, and now follows the same licence: only a demonstrated
  takeover may suppress a Will.
  Details: [F-084](ha-arbitration.md#f-084-prove-a-successor-before-claiming-a-takeover).
- Sweep the comment corpus for three false-claim families, indexed by claim.
  Status: Open. Release treatment: Post-alpha cleanup.
  Filed by the pre-CONNACK-signal item's tenth holistic round after nine rounds fixed instance
  after instance without exhausting the families: comments claiming `abortAcceptedResume`
  restores the session lifetime (it does not; ordering protects it), comments reading a
  rejected CAS or an absent record as proof another owner holds the session (F-084's
  distinction), and comments saying a pre-CONNACK refusal writes no packet when it now carries
  a v5 failure CONNACK. Some name mutations that cannot be written. Seeds are in the roadmap
  bullet; closing it needs a claim-indexed read, not a grep.
  Details: `docs/ROADMAP.md` §8.
- Refuse a pre-release delayed-drain record shape instead of repairing it.
  Status: Open. Release treatment: Post-alpha cleanup.
  Filed by F-034's census. The compatibility boundary refuses an unsupported persisted
  shape; this one is accommodated and repaired instead. Its dossier must first separate
  a genuinely unsupported old record from a current-schema crash state that
  legitimately carries a zero.
  Details: [F-067](runtime-correctness.md#f-067).
- Revalidate an enqueue against the session it was allocated under.
  Status: Open, chapter P's (re-scoped 2026-10-06). Release treatment: chapter P.
  Filed by F-034's audit. The supported path no longer has the filed route: it
  re-enters `beginSendLocked` and writes only through `writeAuthorizedFrame` (supported
  closure conditional on F-105's integrated branch review). The experimental affine
  route still writes raw after `beginSendLocked`, with no re-check at the write.
  Details: [F-068](runtime-correctness.md#f-068).
- Derive the outgoing DUP flag from MQTT retransmission, not the JetStream delivery count.
  Status: **CLOSED 2026-09-09.** DUP is now `mqttPending.transmitted` at a single derivation
  site, seeded from the durable unsent marker; `beginSendLocked` captures it for the send.
  Release treatment: was an alpha blocker. Its closure does **not** license a release —
  F-088 and F-097 remain unconditional alpha blockers.
  **The diagnosis below is historical**, describing the defect as it stood before closure.
  `trackPublish` set DUP from `dcount > 1` and that reached the wire via `mqttPubFlagDup`.
  MQTT-3.3.1-3 ties DUP to whether **the outgoing PUBLISH** is a retransmission; a copy
  refused before `trackPublish` produced no PUBLISH, so the next offer is a FIRST
  transmission and must carry DUP=0. **An in-memory fix is insufficient**: the register is
  confirmed and the message enqueued in two operations, and a crash between them makes
  recovery treat an unsent PUBLISH as started, so the fix owes DURABLE semantics and two
  deterministic tests. F-071's capture shows seven of eight messages arriving
  with DUP=1 on their first client-visible PUBLISH. Found by review of that capture, which
  I had recorded as legitimate behaviour.
  Details: [F-072](release-investigations.md#f-072).
- Preserve same-topic ordering for affine QoS 2 deliveries.
  Status: **Open, demonstrated to be OURS, and parked outside the alpha.**
  Release treatment: Experimental shared-QoS-2 work in ROADMAP chapter P.
  Found while verifying F-034 and reproduced from a SINGLE publishing Client — an earlier
  cross-publisher capture could not support the claim and was withdrawn. The packet
  identifier is allocated immediately before the synchronous register gate and enqueue, on
  a subscription `nats.go` dispatches with one goroutine, so allocation order equals enqueue
  order — and the capture shows a later-published message holding `pi=1`. A reader cannot
  invent an identifier allocation, so the harness is eliminated.
  **NOT a bare spec violation:** §4.6's ordering requirements are conditional on an Ordered
  Topic and on NON-shared subscriptions. It violates an INTENDED implementation property in
  `docs/PORTING.md`, which states per-(Topic,QoS) ordering as intended on EVERY path via one
  ordered carrier per topic-filter+QoS; the affine path adds a second carrier and so violates
  the intention rather than being excluded from it.
  **Three of four explanations were refuted**, each by evidence already in the tree, and the
  cause remains UNTESTED.
  Details: [F-071](release-investigations.md#f-071).
- Establish the affine contender fixture's rival-create premise rather than racing it.
  Status: Open. Release treatment: Parked experimental shared-QoS-2 test cleanup.
  Found while verifying F-034, and the attribution is settled against a baseline rather
  than argued: the fixture stages a rival consumer under a durable name it does not own,
  which JetStream refuses as an immutable-field update once that durable exists. 6/60 on
  the change against **16/60** on a clean baseline — the first isolated batch read 1/10
  against 0/10, which inverted at scale.
  Details: [F-070](release-investigations.md#f-070).
- Fence the orphan-reconcile fixture against the background sweep it does not drive.
  Status: Closed 2026-10-15 (the test owns its sightings: its own quarantine and grace).
  Release treatment: Post-alpha cleanup.
  Found while verifying F-034, with the mechanism PROVEN by construction rather than
  inferred from timing: `TestMQTTInflightOrphanReconcile` asserts that a first sighting
  must not delete, while the account manager's own goroutine can take that first sighting
  before the test's explicit pass. With the grace at zero, two sightings of any spacing
  suffice. The assertion is right; its premise is not established.
  Details: [F-069](release-investigations.md#f-069).
- Add write lineage to outbound in-flight registers.
  Status: Closed 2026-08-22 as an audit plus a census. No lineage was needed — but unlike
  F-032's and F-033's audits this one found a REACHABLE defect, and its remedy is identity
  conditioning plus bounded destruction. Produced F-067 and F-068, and routed its
  live-owner
  residue to F-036 as that finding's fourth manifestation — which F-036's 2026-09-03
  closure did NOT absorb, because it is affine state and the retirement licence names the
  SESSION generation; it is owned by ROADMAP chapter P.
  Details: [F-034](ha-arbitration.md#f-034).
- Bound every server-initiated courtesy DISCONNECT write.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-035](runtime-correctness.md#f-035).
- Revalidate ownership for otherwise quiet resident sessions.
  Status: Closed 2026-09-03, in two units — the incarnation completion lease, then the
  retirement classifier with the licensed bound retirement and the sweep's proof gate.
  Release treatment: was an alpha blocker. Two residuals stated in CONFORMANCE.md (the
  no-licence half of the deleted-record shape, and an unbounded retention leak); the
  affine manifestation is accumulated under ROADMAP chapter P rather than under this
  finding.
  Details: [F-036](ha-arbitration.md#f-036).
- Make a rejected finite disconnect stamp crash-safe.
  Status: Accepted residual (2026-10-12). Release treatment: Not an alpha blocker; stated
  residual in `docs/CONFORMANCE.md`.
  Details: [F-037](session-and-will.md#f-037).
- Prevent takeover races from publishing or dropping the wrong Will.
  Status: Arms (b) and (c) closed by F-084; arm (a) accepted residual (2026-10-12).
  Release treatment: Not an alpha blocker; stated
  residual in `docs/CONFORMANCE.md`.
  Details: [F-038](session-and-will.md#f-038).
- Restore shared-subscription membership during SUBSCRIBE rollback.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-039](runtime-correctness.md#f-039).
- Reproduce and fix reconnect versus DISCONNECT expiry-zero.
  Status: Investigating. Release treatment: Block only on a confirmed defect or
  unexplained repeatable failure.
  Details: [F-040](session-and-will.md#f-040).
- Persist the MQTT 5 DISCONNECT Session Expiry Interval override.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-041](session-and-will.md#f-041).
- Identify the unexplained full-suite failure.
  Status: Closed 2026-10-12 as unidentifiable, not refuted. Release treatment: Closed.
  Nothing actionable remains.
  Details: [F-042](release-investigations.md#f-042).
- Determine whether the phase-2 shutdown scan-stop observation is real.
  Status: Moved to the verification ledger 2026-10-12. Release treatment: Accepted as
  nonblocking on present evidence.
  Details: [F-044](release-investigations.md#f-044); the
  [verification ledger](release-investigations.md#verification-ledger).
- Release stranded shared pending state when membership is removed.
  Status: Closed, jointly. Release treatment: Alpha blocker.
  One defect at two removal paths, closed by two different items and neither changed both
  sites: UNSUBSCRIBE's no-`cons` branch by F-083, the 3.1.1 forced leave by the forced-leave
  item in ROADMAP §0.2.
  Details: [F-045](runtime-correctness.md#f-045).
- Complete the deferred in-flight-register stabilization follow-ups.
  Status: Open. Release treatment: Split before scheduling; most work is a post-alpha
  candidate.
  Details: [F-046](inflight-followups.md#f-046).
- Keep affine assignments when a SUBSCRIBE packet fails but the session survives.
  Status: Open. Release treatment: Parked experimental shared-QoS-2 work.
  Details: [F-047](shared-qos2.md#f-047).
- Arbitrate affine cleanup before discarding assignments.
  Status: Open. Release treatment: Parked experimental shared-QoS-2 work.
  Details: [F-048](shared-qos2.md#f-048).
- Provide an operator recovery path for unreadable affine claims.
  Status: Open. Release treatment: Parked experimental shared-QoS-2 work; no migration or
  recovery mechanism is owed unless the feature is promoted.
  Details: [F-049](shared-qos2.md#f-049).
- Refuse or retry a CONNECT with incomplete shared-subscription restoration.
  Status: Closed 2026-08-23 by refusing. Release treatment: Alpha blocker satisfied.
  Details: [F-050](shared-qos2.md#f-050).
- Arbitrate retained-index application by stream sequence.
  Status: Closed 2026-10-12. Release treatment: Alpha blocker satisfied; cache-TTL-bounded
  residuals stated in `docs/CONFORMANCE.md`.
  Details: [F-059](runtime-correctness.md#f-059).
- Report a failed retained store to the publisher.
  Status: Closed 2026-10-12. Release treatment: Alpha blocker satisfied; residuals
  stated in `docs/CONFORMANCE.md`.
  Details: [F-060](runtime-correctness.md#f-060).
- Send a CONNACK before closing an unauthorized CONNECT.
  Status: Closed 2026-10-26. Release treatment: Pre-alpha, ROADMAP §0.5, satisfied;
  limits stated in `docs/CONFORMANCE.md`.
  Details: [F-061](runtime-correctness.md#f-061).
- Refuse a CONNECT once the connect deadline has elapsed, not only once its timer has
  fired.
  Status: Closed 2026-10-26. Release treatment: Pre-alpha, ROADMAP §0.5, satisfied.
  Details: [F-074](runtime-correctness.md#f-074).
- A server-side default caps consumers per stream, and asking for unlimited arms it
  rather than opting out.
  Status: Decided 2026-10-12: a deployment prerequisite (option (a)); the limitation
  stays a stated residual. Release treatment: Not an alpha blocker.
  The v2.15.0 line defaults to 1000 consumers per stream whenever the stream's and the
  account tier's `MaxConsumers` are both `<= 0` — the shape four broker-owned stream
  validators require. Not readable from a client connection, so startup validation
  cannot catch it; documented as a deployment prerequisite in the interim.
  Details: [F-095](nats-platform.md#f-095).
- The affine owner's normal create can inherit a stream-imposed inactivity timer without
  noticing.
  Status: Open, awaiting a design decision. Release treatment: Parked experimental
  shared-QoS-2 work; post-alpha / maybe someday.
  Pre-existing, surfaced by the v2.15.0-RC.1 dependency audit. The refusal exists only in
  `recoverQ2AffineStorageConflict`, which runs after a create has already failed; the
  normal owner path validates identity names and never reads the threshold back.
  Details: [F-096](shared-qos2.md#f-096).
- Terminal-completion evidence and reconciliation across allocation replacement.
  Status: Open, design undecided. Release treatment: Alpha blocker, ordered BEFORE F-088.
  A completed exchange whose register delete was lost, indeterminate, or aimed at a moved
  sequence leaves durable residue that a later resume's recovery reserves and nothing
  releases, costing outbound QoS 1/2 capacity for the incarnation's life. Two
  identity failures are established: nonce-only lookup loses completion across an N1->N2
  replacement, and `(PI, Dur, Seq)` wrongly joins distinct exchanges because a lost
  fire-and-forget source acknowledgement lets a completed source be re-offered.
  Details: [F-097](ha-arbitration.md#f-097).

## Resolved findings

- Add write lineage to inbound QoS 2 holds and release claims.
  Closed 2026-08-17: the audit found every write already carries its per-subject
  expectation, so no lineage was needed; it produced F-062 (closed 2026-08-18) instead.
  Details: [F-032](ha-arbitration.md#f-032).
- Remove the accidental review sentence from the README attribution section.
  Details: [F-001](resolved.md#f-001).
- Make MQTT TLS startup fail closed.
  Details: [F-002](resolved.md#f-002).
- Reconcile the README TLS/mTLS feature claim with the missing certificate-rollover
  behavior.
  Details: [F-003](resolved.md#f-003).
- Resolve the MQTT core TODO concerning JetStream domain assignment.
  Details: [F-004](resolved.md#f-004).
- Resolve and document the policy for subscriptions to internal MQTT subjects.
  Details: [F-005](resolved.md#f-005).
- Replace brittle JetStream error-string matching in the QoS 2 path with a robust error
  classification.
  Details: [F-006](resolved.md#f-006).
- Close the destructive PUBREL move window.
  Details: [F-007](resolved.md#f-007).
- Keep a started QoS 2 delivery with its chosen shared-subscription client.
  Details: [F-008](resolved.md#f-008).
- Arbitrate session teardown across brokers, and stop deriving deterministic resource
  names from….
  Details: [F-009](resolved.md#f-009).
- Decide and test the expected QoS 2 behavior for an unacknowledged PUBLISH after
  UNSUBSCRIBE.
  Details: [F-010](resolved.md#f-010).
- Serialize session discard against inbound QoS 2 PUBREL release.
  Details: [F-011](resolved.md#f-011).
- Reap orphaned inbound QoS 2 hold records, the two structures session-affine shared QoS
  2 adds….
  Details: [F-012](resolved.md#f-012).
- Give outbound QoS 1/2 in-flight deliveries a durable identity, so a packet identifier
  survives a….
  Details: [F-013](resolved.md#f-013).
- Audit the remaining $MQTT_* streams for protocol prerequisites, deriving each list
  from what….
  Details: [F-014](resolved.md#f-014).
- Verify (or recreate) plain QoS 1/2 durables on session resume instead of trusting the
  persisted….
  Details: [F-015](resolved.md#f-015).
- Make purgeSharedPending reach a RECORD-RESTORED session's shared state.
  Details: [F-016](resolved.md#f-016).
- Call purgeSharedPending from the deferred-disconnect close path — as ONE classified
  operation….
  Details: [F-017](resolved.md#f-017).
- Make the late-uncertainty refusal unconditional in clear(), give the three
  newly-exposed callers….
  Details: [F-018](resolved.md#f-018).
- Settle a definite record-CAS rejection by RE-ATTEMPTING the write, for the UNBOUND….
  Details: [F-019](resolved.md#f-019).
- Converge a BOUND session's record-CAS rejection: close the split-brain [MQTT-3.1.4-3]
  requires,….
  Details: [F-020](resolved.md#f-020).
- The session-record fence is now BARGE-PROOF against non-blocking acquisitions, which
  is what….
  Details: [F-021](resolved.md#f-021).
- "Displace this connection" now has one owner per leg, and a connection gets AT MOST
  ONE….
  Details: [F-022](resolved.md#f-022).
- The uncertainty resolver cleared an ambiguity on a PLAIN LOAD at the base sequence, so
  a….
  Details: [F-023](resolved.md#f-023).
- MEASURED AND CLOSED: an ambiguous shared-QoS 2 AFFINE provisioning save did leave a
  durable….
  Details: [F-024](resolved.md#f-024).
- A per-subject CAS is a linearization point only WITHIN a leader term, so a surviving
  unapplied….
  Details: [F-025](resolved.md#f-025).
- processSessionPersist evicted the rightful client on a remote PubAck it could not
  classify —….
  Details: [F-026](resolved.md#f-026).
- Repair a recognised session-record fork.
  Details: [F-027](ha-arbitration.md#f-027).
- Arbitrate simultaneous takeovers from one session-record base.
  Details: [F-029](ha-arbitration.md#f-029).
- Add write lineage to the Will outbox.
  Details: [F-030](ha-arbitration.md#f-030). Closed by disproving the cross-election
  failure premise; no lineage was added.
- Replace the Will-cancellation watermark with an immutable acceptance fact.
  Details: [F-031](ha-arbitration.md#f-031).
- Replace legacy reserved-subscription migration with current-schema refusal.
  Details: [F-057](resolved.md#f-057).
- Remove inherited retained-message format and delete-protocol compatibility.
  Details: [F-056](resolved.md#f-056).
- Reject generationless delayed-Will outbox records.
  Details: [F-058](resolved.md#f-058).
- Bound a connection that never sends CONNECT.
  Details: [F-051](resolved.md#f-051).
- Remove declarations unused by production and the full suite.
  Details: [F-053](resolved.md#f-053).
- Validate MQTT options at the production startup boundary; wire `-max-conns`.
  Details: [F-052](resolved.md#f-052).
- Fix the removeSessFromFlappers data race.
  Details: [F-043](resolved.md#f-043).
- Separate test and future-feature scaffolding from production code.
  Details: [F-054](resolved.md#f-054).
- Restore reachability diagnostics: no file excluded from any analyzer, four lint passes.
  Details: [F-055](resolved.md#f-055).
