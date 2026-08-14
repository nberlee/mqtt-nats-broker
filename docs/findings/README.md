# Finding dossiers

This directory is the canonical home for technical findings and their planning evidence.
The roadmap carries ordering, release treatment, and completion state; these dossiers
retain failure mechanisms, invariants, rejected approaches, tests, and residual risks.

Stable IDs do not change when a title, priority, or status changes. Plans, tests,
commits, and architecture notes may refer to a finding by its `F-###` identifier.

Cross-cutting JetStream feature decisions that are not independent correctness
findings live in the [NATS platform-adoption audit](nats-platform.md). Its actionable
work is merged into the existing finding or roadmap item whose invariant it serves.

## Active findings

- Preserve deferred disconnect state during foreign disposition.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-028](session-and-will.md#f-028).
- Add write lineage to the Will outbox.
  Status: Open — upstream dependency under proof. Release treatment: Alpha blocker.
  Details: [F-030](ha-arbitration.md#f-030). nats-server 2.14.4 term-fenced proposals
  target the stale-leader route; deterministic differential evidence is required before
  the terminal regression can close.
- Add write lineage to inbound QoS 2 holds and release claims.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-032](ha-arbitration.md#f-032).
- Add write lineage to shared QoS 2 assignment claims.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-033](ha-arbitration.md#f-033).
- Add write lineage to outbound in-flight registers.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-034](ha-arbitration.md#f-034).
- Bound every server-initiated courtesy DISCONNECT write.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-035](runtime-correctness.md#f-035).
- Revalidate ownership for otherwise quiet resident sessions.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-036](ha-arbitration.md#f-036).
- Make a rejected finite disconnect stamp crash-safe.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-037](session-and-will.md#f-037).
- Prevent takeover races from publishing or dropping the wrong Will.
  Status: Open. Release treatment: Alpha blocker.
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
  Status: Investigating. Release treatment: Block only on a confirmed defect or
  unexplained repeatable failure.
  Details: [F-042](release-investigations.md#f-042).
- Fix the removeSessFromFlappers data race.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-043](runtime-correctness.md#f-043).
- Determine whether the phase-2 shutdown scan-stop observation is real.
  Status: Investigating. Release treatment: Block only on a confirmed defect or
  unexplained repeatable failure.
  Details: [F-044](release-investigations.md#f-044).
- Release stranded shared pending state when membership is removed.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-045](runtime-correctness.md#f-045).
- Complete the deferred in-flight-register stabilization follow-ups.
  Status: Open. Release treatment: Split before scheduling; most work is a post-alpha
  candidate.
  Details: [F-046](inflight-followups.md#f-046).
- Keep affine assignments when a SUBSCRIBE packet fails but the session survives.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-047](shared-qos2.md#f-047).
- Arbitrate affine cleanup before discarding assignments.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-048](shared-qos2.md#f-048).
- Provide an operator recovery path for unreadable affine claims.
  Status: Open. Release treatment: Post-alpha candidate if normal broker operation
  cannot create the malformed state.
  Details: [F-049](shared-qos2.md#f-049).
- Refuse or retry a CONNECT with incomplete shared-subscription restoration.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-050](shared-qos2.md#f-050).
- Arbitrate retained-index application by stream sequence.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-059](runtime-correctness.md#f-059).
- Report a failed retained store to the publisher.
  Status: Open. Release treatment: Alpha blocker.
  Details: [F-060](runtime-correctness.md#f-060).
- Send a CONNACK before closing an unauthorized CONNECT.
  Status: Open. Release treatment: Post-alpha candidate; promote if a conformance
  suite or an operator report shows the missing CONNACK in practice.
  Details: [F-061](runtime-correctness.md#f-061).
- Connect MQTT option validation to the production startup boundary.
  Status: Open. Release treatment: Maintainability item; promote if startup tests
  expose an accepted invalid configuration.
  Details: [F-052](dead-code.md#f-052).
- Separate test and future-feature scaffolding from production code.
  Status: Open. Release treatment: Maintainability cleanup; WebSocket remains in
  its transport milestone.
  Details: [F-054](dead-code.md#f-054).
- Restore reachability diagnostics for `mqtt.go`.
  Status: Open. Release treatment: Maintainability gate after F-052 through F-054.
  Details: [F-055](dead-code.md#f-055).

## Resolved findings

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
