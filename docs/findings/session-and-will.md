# Session-close, expiry, and Will findings

These findings cover deferred close state, MQTT 5 session-expiry semantics, and Will
correctness across rejection, reconnect, and takeover.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10.

## F-028

**Title:** Preserve deferred disconnect state during foreign disposition

- Status: Accepted residual (2026-10-12), not fixed
- Classification: Confirmed correctness or liveness defect
- Release treatment: Not an alpha blocker; stated residual in `docs/CONFORMANCE.md`
- Original roadmap order: 28

### Current ruling (2026-10-12 convergence triage)

Since F-084 every foreign route discharges the parked Will against its original deadline,
so the Will half of this finding is closed. Two parts remain and are accepted as a
residual under the user's licence for rare, costly edge cases (Codex consult, 2026-10-12):
the foreign discharge performs no session save and no expiry scheduling, so the original
session-expiry obligation is not enforced by that path; and retained `uncertainLanded`
continuations stay reachable while the old object is referenced. Both need an ambiguous
write followed by a foreign verdict. The residual is in `docs/CONFORMANCE.md` beside the
ambiguity path's F-084 resolution.

### Original finding record

- [ ] The ambiguity path's foreign disposition consumes the parked deferred disconnect
  (`disposeForeignStateLocked` calls `takeDeferredDisconnect`), which can drop the only
  copy of a Will and an original expiry deadline; and it clears `pendingWrite` and the
  uncertainty flag without clearing the retained `uncertainLanded` continuations, which
  stay reachable through a live `c.mqtt.sess` pointer after an off-readLoop disposal. Both
  are PRE-EXISTING on the foreign path — the forked verdict merely reaches them more often
  — and both were deliberately left alone by the recognition work rather than folded in.

## F-037

**Title:** Make a rejected finite disconnect stamp crash-safe

- Status: Accepted residual (2026-10-12), not crash-safe
- Classification: Confirmed correctness or liveness defect
- Release treatment: Not an alpha blocker; stated residual in `docs/CONFORMANCE.md`
- Original roadmap order: 37

### Current ruling (2026-10-12 convergence triage)

Accepted as a residual (Codex consult, 2026-10-12): the remaining gap needs a finite close
whose save is rejected and then a crash before any settlement write lands. An affected
persistent session can survive its original expiry deadline; recovery may reconstruct a
later deadline, but no bound from the original disconnect is guaranteed. The residual is
in `docs/CONFORMANCE.md` beside "A rejected close still publishes its Will".

### Original finding record

- [ ] The ordinary close's FINITE rejected disconnect save leaves `disconnectedAt`
  mutated in memory but unpersisted, with the timer armed and nothing rolled back
  (`mqtt.go`). Across a crash the deadline is lost and the record still reads as
  connected. Pre-existing and already a stated residual for the ambiguous case; what is
  new is that the DEFINITE rejection now has a settlement path (a landed probe re-saves
  current memory, including the stamp), so the remaining gap is only the crash window
  before any probe lands. Decide whether the finite branch should stamp before its
  transaction the way the never-expire close now does, or whether the probe is enough.

## F-038

**Title:** Prevent takeover races from publishing or dropping the wrong Will

- Status: Arms (b) and (c) closed by F-084; arm (a) accepted residual (2026-10-12)
- Classification: Confirmed correctness or liveness defect
- Release treatment: Not an alpha blocker; stated residual in `docs/CONFORMANCE.md`
- Original roadmap order: 38

### Current ruling (2026-10-12 convergence triage)

Arm (a) is accepted as a residual (Codex consult, 2026-10-12): a remote takeover racing an
ordinary finite close can cause one unwanted immediate Will from that close. It is an
extra publication, never a lost Will, and retracting it needs a durable Will intent. The
residual is "A rejected close still publishes its Will" in `docs/CONFORMANCE.md`.

### Original finding record

- [ ] A close can publish a Will that a remote takeover should have suppressed, on TWO
  pre-existing routes, and the rejection settlement cannot retract either publication. (a)
  The ORDINARY CLOSE: when the finite disconnect save is CAS-rejected by a remote
  takeover, the path logs, schedules the expiry, captures the origin sequence and calls
  `mqttHandleWill`, which publishes a non-delayed Will synchronously — before anything has
  classified the rejection. The probe may prove the session foreign a pass later, but the
  Will is already out. (b) The PERIODIC PASS's bound-foreign disposal reaches the close
  bookkeeping's `stillOurs == false` arm without a takeover discard, so the displaced
  connection's close classifies its Will as owed where a local takeover would have
  discarded it — and the bound settlement's sentinel close adds a way to REACH (b): its
  fail-closed return races the close bookkeeping against the pass's bound-uncertain
  resolver, which can dispose foreign first. (c) A teardown PARKED by an ambiguous write
  is dropped wholesale by a foreign uncertainty resolution, including a Will owed NOW by a
  v5 DISCONNECT reason 0x04 — which contradicts the takeover rule's too-late-to-cancel arm
  (then `mqttDiscardWillOnTakeover`, now `mqttSettleWillOnTakeover`): that Will's
  connection closed first and its delay has already run out, so nothing may cancel it.
  Pre-existing for every uncertain 0x04 close. All three are UNCHANGED by the rejection
  settlement — the diff touches neither `mqttHandleWill`'s call nor the finite save's
  error branch — and neither is closable by the obvious fixes. Discarding on a rejection
  alone is destructive: 10164 is CONTENTION, not takeover evidence, so it would drop a
  Will a session that is still ours owes. Probing inside the close before the Will costs a
  JetStream round trip on every rejected close and still has no answer on a contended
  verdict. And PARKING the close and its Will there is the design round 1 of the `clear()`
  recovery item killed with reasons: a parked Will is memory-only and non-transferable, so
  a remote takeover evicts the action under `as.mu` alone and nothing publishes it —
  regressing a Will today's broker delivers. A durable, cluster-visible Will intent is
  what would settle it, which is Will-outbox scope. Needs that analysis before any fix;
  stated as a residual in `docs/CONFORMANCE.md` meanwhile. (a) found by holistic round 2
  of the rejection settlement, (b) by its plan round 4, (c) by plan round 2 of the bound
  settlement.

**Post-F-084 note.** The record above is the finding AS ORIGINALLY FILED and two of its
premises are now historical. Arm (b) is closed: a bound-foreign disposal no longer reaches
the takeover discard at all, because F-084 withdrew the takeover claim from every route
that cannot hold a successor, and such a disposal now CONSUMES the displaced connection's
Will rather than classifying it against a takeover it never demonstrated. Arm (c) is
closed too: a foreign uncertainty resolution no longer drops a parked teardown wholesale
but discharges its Will against the original deadline. The paragraph's design argument
against parking — "a remote takeover evicts the action under `as.mu` alone and nothing
publishes it" — was true when written and is not now; the remote-persist eviction
discharges the parked action. Arm (a), the ordinary close publishing before anything has
classified the rejection, is UNCHANGED and is what keeps F-038 open.

## F-040

**Title:** Reproduce and fix reconnect versus DISCONNECT expiry-zero

- Status: Fixed (2026-10-14)
- Classification: Release-verification investigation
- Release treatment: Block only on a confirmed defect or unexplained repeatable failure
- Original roadmap order: 40

### Outcome (2026-10-14)

Reproduced deterministically at `54999b0` (3/3) by holding the old connection's close at
the teardown seam after the DISCONNECT recorded its override and before the close contends
for the gate, then sending CONNECT with Clean Start 0: Session Present 1. The mechanism is
the one recorded below, and it was not load-only: the takeover's resume decision never read
the bound connection's override. Fixed by having the takeover persist that override on the
close's behalf before deciding and before any detach. The mechanism is in
`docs/ARCHITECTURE.md` ("The DISCONNECT lifetime override is a connection's OBLIGATION"),
the residuals in `docs/CONFORMANCE.md` ("A reconnect racing its own DISCONNECT").

### Original finding record

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
  runs at `a546be9` under controlled identical load, so neither "introduced
  by the register-claims change" (whose code provably does not execute in
  this shape) nor "pre-existing" is EVIDENCED yet — the mechanism analysis
  above says pre-existing, the counts say only "rare and load-sensitive".
  Needs a deterministic reproduction (hold the old connection's close between
  the rebind check and the teardown, then CONNECT) before any fix.
  Unchanged by the `clear()`-recovery item above, which narrowed a SIBLING of this
  defect (below) but does nothing for this one: this window returns at the
  `sess.c != c` check, before any `clear()` is attempted, so no refusal and no
  recovery is involved.

## F-041

**Title:** Persist the MQTT 5 DISCONNECT Session Expiry Interval override

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 41

### Original finding record

- [ ] Persist the v5 DISCONNECT Session-Expiry-Interval override, or make the resume
  decision stop trusting the record's `Clean` flag alone. A v5 DISCONNECT carrying
  Session Expiry Interval 0 ends a session that was persistent, but it updates
  `sess.expiryInterval` and `c.mqtt.sessionExpiry` IN MEMORY ONLY: nothing is
  persisted and `sess.clean` is not updated either. So the record still says
  `ExpiryInterval > 0`, `Clean = false`, `DisconnectedAt = 0`, and
  `createOrRestoreSession` copies those values back on restore — a non-clean
  reconnect that meets the surviving record fails `cleanSess || es.clean || overdue`
  and resumes a session the DISCONNECT ended, reporting Session Present true against
  Spec5 [3.1.2.11]. A SIBLING of the reconnect-race bullet above, not the same
  defect: that mechanism is entirely in memory (the resident object is resumed and
  the record plays no part), while this one is a durable record that any broker can
  meet, including one that never saw the DISCONNECT. It bites whenever the owed
  discard's in-memory intent is lost, which the `clear()`-recovery item above narrowed
  from unconditional to three cases: a crash before the discharge, an observed remote
  record write that evicts the object, and — needing neither of those — the recovery helper
  DECLINING for an already-parked action that is not the discard-only shape, after
  which the close evicts and logs. The third case's reachability is unknown and is not
  assumed away; `docs/CONFORMANCE.md` states all three.
  The obvious fix is not a one-liner, and any plan must start here: the resume
  decision keys on `es.clean`, not on the interval, and the restore path sets
  `sess.clean = ps.Clean` separately from `ps.ExpiryInterval`, so persisting the
  interval alone produces exactly the inconsistent record this gap is about. And a
  DISCONNECT-time save can itself land ambiguously, which is the state the recovery
  above exists to handle — so the fix must not assume its own write succeeded.
  Found by the plan review of the `clear()`-recovery item.
