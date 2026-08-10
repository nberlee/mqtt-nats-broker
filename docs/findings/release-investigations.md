# Release-verification investigations

These observations need deterministic identification or reproduction before they become
implementation tasks. Preserve complete logs for every run.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10.

## F-042

**Title:** Identify the unexplained full-suite failure

- Status: Investigating
- Classification: Release-verification investigation
- Release treatment: Block only on a confirmed defect or unexplained repeatable failure
- Original roadmap order: 42

### Original finding record

- [ ] Investigate: ONE unidentified `mqttport_fulltests` failure in 21 forced runs on the
  `clear()`-recovery tree, and the identity was DESTROYED rather than lost to chance.
  The run that failed (346.1s) was piped through `tail -4`, so its `--- FAIL: <TestName>`
  line was discarded and cannot be recovered; the same command class that reported "no
  tests to run" as a passing mutation earlier in that work. Rule that follows from it:
  never truncate a suite run's output before knowing the outcome — capture the whole log
  and grep it.
  NOT reproduced in, all on the same tree and all `-count=1`: 20 further full-suite runs
  (ten plain, ten `-v`, each executing 786 top-level tests and 2065 including subtests, so
  none vacuous), 12 runs of the `Expiry|Expire` subset, 20 runs of the change's own 14
  tests, two `-race` unit suites and two `-race` cluster-failover runs. Rate therefore
  1/21 (~5%), which is why no cheap paired soak can settle it: distinguishing 5-9% from 0%
  needs roughly 40 runs per arm, and `test/soak.sh` adds `-race`, putting a whole-suite
  paired soak near 15 hours. The identity has to come first, and then
  `SOAK_RUN='<TestName>' SOAK_BASE=HEAD make soak` is minutes and decisive.
  The failing run overlapped no other suite and no review, so the one known remaining
  cause in the flakiness notes — starvation above roughly four concurrent suites — does
  not explain it. First hypothesis to test, and it belongs to that change: `expireSession`
  now acquires the record fence with the BLOCKING `beginRecordTx` around its record load
  and superseded decision, where it previously took no fence at all. The plan review
  cleared that as bounded contention rather than deadlock, which is a statement about
  SAFETY and not about latency — an expiry that used to proceed immediately can now wait
  for a readLoop writer. The 12 clean subset runs argue against it without clearing it,
  since the subset does not reproduce the whole suite's timing or its session population.

## F-044

**Title:** Determine whether the phase-2 shutdown scan-stop observation is real

- Status: Investigating
- Classification: Release-verification investigation
- Release treatment: Block only on a confirmed defect or unexplained repeatable failure
- Original roadmap order: 44

### Original finding record

- [ ] Investigate: the Phase-2 shutdown scan-stop can miss its between-records
  window under load. Observed once in a full (sequential) `mqttport_fulltests`
  run: `TestMQTTWillOutboxWatermarkPhase2ShutdownStopsScan` counted 2 marker
  loads where the loop-top `willClaimStopCh` check should have stopped the scan
  after the first record ("scan performed 2 marker loads"). 30/30 standalone
  passes on the same tree, nine prior full-suite passes, and the Will-cancel
  machinery is untouched by the register-claims change (last touched b70e9d9 /
  3a25be2), so this is either a scheduling-sensitive test assumption or a real
  ordering gap between `closeWillDispatch` and the scan loop's stop check.
  Needs the same treatment as the expiry-race bullet above: a deterministic
  reproduction before any fix.
