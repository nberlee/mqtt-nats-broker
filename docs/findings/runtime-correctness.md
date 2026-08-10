# Runtime correctness and liveness findings

These are bounded implementation defects in connection teardown, subscription rollback,
synchronization, and pending-state release.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10.

## F-035

**Title:** Bound every server-initiated courtesy DISCONNECT write

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 35

### Original finding record

- [ ] Every server-initiated COURTESY DISCONNECT write is unbounded, and one of them is
  performed with the ownership gate held. `mqttEnqueueDisconnect` → `queueOutbound` →
  `writeSocket` with a ZERO deadline (`shim_engine.go`), so a client that has stopped
  reading blocks the writer indefinitely: the parse-error footer's 0x89 defers the
  teardown that follows it, the bound settlement's foreign 0x8E pins its own readLoop, and
  the periodic pass's `disposeForeignSessionLocked` performs that write while holding the
  session gate, which blocks a reconnecting CONNECT for as long as the stalled client
  lasts. The DISCONNECT claim above removes a write only when it suppresses one (a second
  notification, or a pre-CONNACK takeover); the first claimant still writes unbounded, so
  every site here stands. A deadline is not enough on its own: `writeSocket` installs it
  only AFTER acquiring `wmu`, so an earlier unbounded writer holding that mutex leaves the
  "bounded" write unbounded — a gap `shutdownDisconnect` shares. Three fix shapes were
  tried and each was killed by a review round of the bound settlement, so do not repeat
  them: (1) a
  SYNCHRONOUS write on the readLoop bounds nothing and pins the loop; (2) an ASYNCHRONOUS
  goroutine that writes then closes never reaches the close while the write blocks, and
  with the read deadline cleared the connection stays open forever; (3) `writeDirect` with
  a deadline does not bound the `wmu` acquisition. What is needed is a real write
  watchdog (arm the connection deadline before contending for `wmu`, or close from a timer
  independent of the writer), designed once and applied to every courtesy write.

## F-039

**Title:** Restore shared-subscription membership during SUBSCRIBE rollback

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 39

### Original finding record

- [ ] Make `readd` restore `sharedSubs` alongside `cons`, or release from both.
  `rollbackSubscribePacket`'s `readd` (`mqtt.go`) restores the packet's added
  `cons` entries and reopens their delivery, but not the `sharedSubs` key, so a
  session can end in a state where a live shared consumer is outside the
  membership. The pending-state-derived purge above is immune, but `clear()`'s
  shared-durable RELEASE still derives from `sharedSubs` alone (`mqtt.go`), so
  such a session never releases the re-added shared consumer's group durable —
  which `sharedDurablesHeldByOthers` then keeps alive for a member that no
  longer exists. Either restore both maps in the rollback, or make the release
  walk both. Found by the plan review of the purge bullet above.

## F-043

**Title:** Fix the removeSessFromFlappers data race

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 43

### Original finding record

- [ ] Fix the `removeSessFromFlappers` data race, with a reproduction first.
  `mqttDeferDisconnectEx` calls `asm.removeSessFromFlappers` WITHOUT holding
  `asm.mu` (`sess_uncertain.go`), while that helper documents "Lock held on entry"
  and deletes from `as.flappers` directly (`mqtt.go`) — and CONNECT reads the same
  map under `asm.mu`. A real pre-existing race at both of that function's call
  sites. Found by the plan review of the `clear()`-recovery item and verified
  against the code; that item's design does not go through
  `mqttDeferDisconnectEx`, so it neither widens nor fixes it. Needs, in this order:
  a `-race` reproduction covering BOTH call sites; a lock-order and deadlock
  analysis for `sess.mu` against `asm.mu`, since the call sites hold or take
  `sess.mu` and the naive fix takes `asm.mu` inside it; and then a focused fix with
  a regression test.

## F-045

**Title:** Release stranded shared pending state when membership is removed

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 45
- Narrowed since: the preserved record below names three stranding paths;
  `purgeReservedSubs` was deleted by [F-057](resolved.md#f-057), so two remain (the
  3.1.1 forced leave and UNSUBSCRIBE's no-`cons` branch). The finding stands
  unchanged on those.

### Original finding record

- [ ] Free stranded shared pending state at the membership removal, not at the
  close. The 3.1.1 forced leave, UNSUBSCRIBE's no-`cons` branch and
  `purgeReservedSubs` all stage register verdicts and deliberately leave a
  TRANSMITTED delivery's `cpending`/`pendingPublish` entries behind, because
  their own purge walks only `cons`-derived durables and a record-restored
  session has none. The fix above frees them at the close (previously: never),
  but the removal-to-close hold remains: for the rest of the connection those
  entries hold packet identifiers and Receive Maximum slots for a subscription
  that is already gone — under a Receive Maximum of 1, a session that can no
  longer deliver at all. The removals should purge what they strand, by the same
  pending-state derivation. Spec5 [4.8.2], [4.9].
