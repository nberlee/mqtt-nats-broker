# Release-verification investigations

These observations need deterministic identification or reproduction before they become
implementation tasks. Preserve complete logs for every run.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10.

## F-042

**Title:** Identify the unexplained full-suite failure

- Status: Closed 2026-10-12 as unidentifiable, not refuted
- Classification: Release-verification investigation
- Release treatment: Closed; nothing actionable remains
- Original roadmap order: 42

### Current ruling (2026-10-12 convergence triage)

The failing test's identity was destroyed by a truncated log, and 20 later full-suite runs
plus the narrower runs below did not reproduce it. With no name and no repeatable failure
there is nothing an open box can act on, so the investigation is closed as unidentifiable.
That is not a finding that the failure was benign. The complete-log rule it produced
stands.

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

- Status: Moved to the [verification ledger](#verification-ledger) 2026-10-12
- Classification: Release-verification investigation
- Release treatment: Accepted as nonblocking on present evidence
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

## F-069

**Title:** Fence the orphan-reconcile fixture against the background sweep it does not
drive

- Status: **Closed 2026-10-15** by the `TestMQTTInflight*` family attribution (ROADMAP
  §0.4): the test now owns its sightings (see "Resolution" below). Filed 2026-08-22, found
  while verifying F-034, with the mechanism PROVEN rather than suspected.
- Classification: Test-harness race
- Release treatment: Post-alpha cleanup. It is a fixture premise, not broker behaviour:
  every assertion the test makes about the sweep is correct, and what is unsound is its
  assumption about how many times the sweep has run.

### The finding

`TestMQTTInflightOrphanReconcile` seeds an orphaned register, sets `testOrphanGrace` to
zero, and then drives the reconcile by hand, asserting that **the first sighting must
not delete**. That assertion rests on the test's own pass being the first sighting, and
nothing makes it so: `sweepOrphanedQoS2State` also runs from the account manager's own
goroutine — immediately at manager creation and then on a ticker — so a background pass
can land between the seed and the explicit pass.

Eligibility is a stable observation rather than an age: `observedOrphanFor` returns
false when it inserts a key and true on a later sighting with the same fingerprint once
the grace has elapsed. With the grace at zero, **two sightings of any spacing are
sufficient**. So one background pass in that window makes the test's "first" pass the
second sighting, the register is deleted, and the assertion fails.

### Proven, not inferred

Calling `sweepOrphanedQoS2State` once between the seed and the explicit pass makes the
failure deterministic. That is the whole mechanism, established by construction — no
statistical argument, and no need to reproduce the timing.

The window is reachable in a real run for two independent reasons, and the second is the
one that makes it load-sensitive: the manager's immediate startup pass can be scheduled
after the seed under load, and `mqttSessionExpirySweepIntervalNS` is a PACKAGE-LEVEL
value that other parallel tests shorten — `TestOrphanSweepRunsFromTheAccountSweepLoop`
sets 100 ms — so the ticker can fire inside the window as well. Observed once in a `make
test` run during F-034's verification; **0/8 isolated**, and **0/4** on a clean baseline
worktree at `ba97fee`. Neither figure is evidence about the cause, because the mechanism
above is not statistical — they only say the window is narrow.

### What a fix must not do

Not raise the grace and not weaken the assertion. The assertion is the point: a single
sighting must never delete. What is missing is a fence establishing that the test's pass
IS the first — `mqttOrphanSweepRuns` already exists for exactly this purpose and is
documented as "test-only: the count a test watches to know a pass has finished touching
state" — or suppressing the background driver for the test's manager. Either makes the
premise true instead of likely.

### Resolution (2026-10-15)

Neither of the two fences above: both leave the test sharing the manager's quarantine,
and a gate on the background driver brought its own install gap and lifetime problems
(plan review, rounds 1-3). The intrusion was possible only because the test shared two
things with the background sweep — the quarantine the sightings accumulate in, and the
grace, which the test lowered to zero for the whole manager. `reconcileOrphanInflight` now
takes the grace and the quarantine as parameters (the sweep passes the manager's own), and
the test drives it with its OWN fresh table at grace zero without touching
`testOrphanGrace`. The manager's passes then run at the default five-minute grace against
their own table: they can neither hand the test a sighting nor delete a register seeded
seconds earlier. The first-sighting claim is unchanged, and is now observed at its cause:
the test records every delete the reconcile REQUESTS (the per-manager
`testInflightDelete` seam) and asserts the first pass requested none, because the
reconcile's delete is fire-and-forget and a pass that wrongly deleted on first sight would
still leave the register readable for a moment. Mutations: an `observedOrphanFor` that
qualifies on first sight, and the test reverted to the shared table at grace zero with one
forced sweep between seed and pass (this finding's own construction), each fail it.

## F-070

**Title:** Establish the affine contender fixture's rival-create premise rather than
racing it

- Status: Open, with the mechanism PROVEN and the attribution SETTLED against a baseline.
  Filed 2026-08-22, found while verifying F-034.
- Classification: Test-harness race
- Release treatment: Parked experimental shared-QoS-2 test cleanup in ROADMAP chapter P.
  Like F-069 this is a fixture premise rather than broker behaviour; the assertions remain
  correct, but a failure does not block the supported alpha.

### The finding

`TestQ2AffRacingAbsenceConverge/the_relookup_finds_it_absent_again` installs a create hook
that, on the first attempt, stages a **rival** consumer under the same durable name with
`MemoryStorage` inverted:

```go
rival := *cfg
rival.MemoryStorage = !cfg.MemoryStorage
if _, err := real(&rival); err != nil { t.Errorf("staging the rival's create: %v", err) }
```

JetStream treats `CONSUMER.CREATE` against an existing durable as an **update**, and
refuses a change to an immutable field with `10012 storage type can not be updated`. So
the staging succeeds only while that durable does not yet exist. The fixture assumes it
wins that race and does not establish it; under load it loses. The follow-on `10014
consumer not found` from `deleteWinnerDurable` is pure cascade — with the rival create
refused there is nothing to delete.

The same collision shape appears legitimately in the production restore path, which logs
`deliver policy can not be updated (10012)` and `storage type can not be updated (10012)`
and refuses the restoration. Those warnings are the designed behaviour, not this defect.

### Attribution, and why the first reading was wrong

| Arm | Failures / runs | Rate |
|---|---|---|
| the F-034 change | 6 / 60 | 10% |
| clean baseline worktree at `ba97fee` | **16 / 60** | **27%** |

The first isolated batch read **1/10 on the change and 0/10 on the baseline**, which looks
like a regression and is not one — the baseline is nearly three times worse at 60 runs per
arm. This is the second time in one lane that a small-sample comparison inverted under a
larger one, and the rule it argues for is the same both times: a ten-percent flake needs
tens of runs PER ARM before a difference means anything.

No claim is made that the change improves it. A timing difference moves a flake rate
without touching the defect, and the defect is in the fixture.

### What a fix must not do

Not delete the rival staging — the rival is the whole point, since the test exists to
prove that an identical retry after the obstacle is removed is the retry that succeeds.
What is missing is establishing the premise: create the rival only once the durable is
known absent, or stage it under a name this test alone can own.

## F-071

**Title:** Preserve same-topic ordering for affine QoS 2 deliveries

- Status: **Open, demonstrated to be OURS, and parked outside the alpha since
  2026-09-02.** Filed 2026-08-22 while
  verifying F-034; reclassified the same day when review refuted the redelivery
  explanation; then REPRODUCED with instrumentation, which showed the broker's own
  packet-identifier allocation transmitting the sixth-published message first. The fixture
  is exonerated. The cause inside the affine path is not yet identified.
- Classification: Ordering defect against an INTENDED implementation property — NOT a spec
  violation; see the basis discussion
- Release treatment: **Parked experimental shared-QoS-2 defect; ROADMAP chapter P.** The
  specification permits reordering on a shared path, but the implementation still violates
  its stated intended ordering property. Parking is a feature-scope decision, not a
  refutation or acceptance of the observed behavior.

### The observation

`TestQ2AffineOrderingPreserved` publishes eight same-topic QoS 2 messages to one member
of a shared affine subscription and reads them back on one ordered connection, recording
each payload's **first** client-visible PUBLISH:

```go
if !seen[string(payload)] { seen[string(payload)] = true; order = append(order, ...) }
```

Observed once in `make full-suite`: `[m01 m00 m02 m03 m04 m05 m06 m07]` — every message
present, exactly one inversion at the front.

### Why the redelivery explanation is REFUTED, and it was mine

The first filing said an AckWait redelivery of `m00` after `m01` produced this. **That
cannot be true, and the fixture's own code says so.** The dedupe records a payload's
FIRST arrival and ignores every later duplicate, so if `m00` had arrived before `m01` no
subsequent redelivery of `m00` could move it. `[m01 m00 …]` therefore means `m00`'s
first **client-visible** arrival came after `m01`'s: either `m00` was overtaken before
reaching the client, or its first packet never reached the reader.

The explanation was worse than unproven — it was contradicted by the design it
described. `testQ2AffSetup` sets `o.MQTT.AckWait = 400ms` deliberately, and the
fixture's comments say it "interleaves duplicate PUBLISH redeliveries" on purpose.
Redelivery in a 20-second loop is **expected** here; the dedupe exists precisely to
tolerate it. So redelivery is the normal case, not the anomaly, and the open question is
why `m01` passed `m00` at all.

### What IS established: attribution, and only attribution

F-034 is bounded out of this path, and that argument stands on its own terms. Every
non-comment changed line in the production diff was mapped to its enclosing function:
`mqtt.go`'s only changes are two `nil`-in-production test seams; `orphan_qos2_sweep.go`
and `sess_incarnation.go` changed comments only; `shim_natsalias.go` added a type alias;
and in `inflight_register.go` exactly one changed function sits on this test's path —
`storeInflightRegister`, six lines, the mismatched-occupant refusal, unreachable absent
a body naming a different identifier than its subject. The test performs no UNSUBSCRIBE,
no teardown and no resume.

**Bounding a change out of a path settles who did not cause a failure. It does not explain
the failure**, and conflating the two is what produced the wrong classification.

Isolated repeats: **0/36 on this tree and 0/36 on a clean baseline** — no rate signal in
either direction.

### REPRODUCED FROM ONE PUBLISHING CLIENT — this is the evidence that counts

**Read this before the cross-publisher capture below.** An earlier version of this dossier
claimed a §4.6 violation from a capture in which the eight messages came from **eight
different MQTT Clients** — `b.publish` opens a fresh connection per message
(`clientID: fmt.Sprintf("%s-pub-%d", b.group, pi)`, `defer cp.Close()`). MQTT 5 §4.6
orders
messages per Topic/QoS **from a given publishing Client**; across publishers there is no
requirement. Review caught that, and the claim did not follow from that evidence. It also
cited MQTT-4.6.0-1, which governs **client retransmission**, not server ordering.

So the experiment was rerun in the only shape §4.6 constrains: **all eight published by
ONE
Client on ONE connection**, each QoS 2 handshake completed before the next was sent. It
reproduces — **2 inversions in 120 attempts under concurrent load**:

```text
order = [m07 m00 m01 m02 m03 m04 m05 m06]
m07 pi=1 dup=false | m00 pi=2 dup=true | m01 pi=3 dup=true | ... | m06 pi=8 dup=true

order = [m02 m00 m01 m03 m04 m05 m06 m07]
m02 pi=1 dup=false | m00 pi=2 dup=true | m01 pi=3 dup=true | m03 pi=4 dup=true | ...
```

**What this violates, stated carefully after getting it wrong twice.** It is NOT a bare
MQTT
§4.6 violation: MQTT-4.6.0-5 is conditional on an *Ordered Topic*, MQTT-4.6.0-6 requires
ordered treatment only when forwarding on **Non-shared** Subscriptions, and §4.8.2 does
not
extend that to Shared Subscriptions. The specification permits a server to reorder on a
shared path.

**But this broker intends to preserve it on every path, and three of its tests depend on
that.** The claim is in `docs/PORTING.md`, and its location matters: it sits under
**"## Testing strategy"**, in a bullet explaining why three upstream cross-topic/cross-QoS
ordering tests were REWRITTEN rather than kept. As it now reads:

> *"That per-(Topic,QoS) guarantee is **INTENDED on every path** — each topic-filter+QoS
> stream is meant to ride a single ordered carrier (one core-NATS subscription for QoS0, one
> JetStream push consumer for QoS1/2) on a single dispatch goroutine — and it holds on the
> non-affine paths these three rewritten tests exercise. **The affine shared-subscription
> QoS 2 path currently VIOLATES it** ... That is stated as a defect against the intention,
> NOT as a carve-out from it."*

**Two corrections to that sentence, both made by this finding and both worth knowing.** It
previously read *"is preserved here"* with no exception, and it cited **[MQTT-4.6.0-9],
which does not exist** — §4.6's normative statements end at MQTT-4.6.0-6. The citation is
now section-level. And an intermediate edit of mine re-scoped it to "the non-affine paths",
which would have turned this violation into a documented carve-out and removed the ground
this finding stands on; review caught that, and the present wording is the repair.

The affine QoS 2 path breaks the very mechanism that sentence names: it delivers **copies**
through a `$MQTT_Q2AFF_*`
consumer on `$MQTT_out`, a SECOND carrier alongside the topic-filter consumer described. One
ordered carrier per topic-filter+QoS does not hold there.

**How strong that is, stated precisely, because three earlier attempts at this overreached.**
It is NOT a normative conformance contract: `docs/CONFORMANCE.md` makes no global ordering
promise, and its only ordering statement is scoped to the Receive Maximum prompt-release
path. It is an **INTENDED implementation property**, stated for every path and load-bearing
for three rewritten tests — so the affine path is a defect against the intention, **not a
carve-out from it.** That distinction matters: an earlier edit of `PORTING.md` scoped the
promise to "the non-affine paths", which would have removed the very ground this finding
stands on. Review caught that, and the wording now says the guarantee is intended everywhere
and currently violated here.

Before the 2026-09-02 scope decision, F-071's alpha-blocker status rested on two things,
neither of them a spec citation: that stated property, and the judgement that silently
reordering one client's same-topic QoS 2 messages was not acceptable behaviour for this
broker whatever the specification permits. The decision has now made the honest
reclassification: this remains an open product-path defect, but it is parked post-alpha in
ROADMAP chapter P. That does **not** weaken the assertion or evidence that found it.

**The signature is specific and constrains any explanation.** In both captures exactly ONE
message — an arbitrary one, `m07` then `m02` — goes out first on `pi=1` with
**`dup=false`**,
and every other follows **in source order** with `dup=true`. The majority ordering is
preserved; what happens is that one copy is delivered on its FIRST JetStream offer while
all
its siblings had already been offered at least once. Whatever refuses those earlier offers
does not refuse this one.

**The existing fixture does not test the guarantee.** `TestQ2AffineOrderingPreserved`
uses a
fresh publishing Client per message, so it asserts an ordering the spec does not require
and
would catch a single-publisher violation only by coincidence. A correct fixture publishes
from
one Client. That is part of what this finding owes, and it is why the assertion must be
kept
and STRENGTHENED rather than weakened.

Neither probe is left in the tree: at 2 in 120 they are evidence, not tests.

### The cross-publisher capture, kept as secondary evidence

An instrumented probe — the same scenario, logging every packet as received with its type,
payload, packet identifier and DUP flag before any dedupe — reproduced it **once in 240
attempts under concurrent `full-suite` load**, and **0 in 40 attempts unloaded**. The
reproduction is load-SENSITIVE; 40 unloaded attempts finding nothing does not establish
that
load is necessary. The captured stream:

```text
order = [m05 m00 m01 m02 m03 m04 m06 m07]

PUB m05 pi=1 dup=false
PUBREL
PUB m00 pi=2 dup=true
PUBREL
PUB m01 pi=3 dup=true
... m02 pi=4, m03 pi=5, m04 pi=6, m06 pi=7, m07 pi=8, all dup=true
```

**The identifier is allocated immediately before the synchronous register gate and
enqueue** — `trackPublish` mints it via `bumpPI()` inside the delivery callback and caches
it under `cpending[jsDur][sseq]`, and a redelivery reuses it. Because this fixture's
affine
subscription is dispatched on a single goroutine, allocation order equals enqueue order,
which is what makes the identifier order meaningful here:

> **m05, m00, m01, m02, m03, m04, m06, m07**

`m05` — published sixth — took `pi=1`. **The inversion is broker-side.** A reader cannot
invent an identifier allocation, and `bind()` reads nothing from the wire, so the harness
is
eliminated.

### A mechanism I proposed and review REFUTED — recorded, not deleted

This is the fourth explanation I offered for this one failure, and the third that was
wrong.
It is kept because the refutation is a fact about the code that anyone re-deriving a
mechanism here needs.

**What I proposed.** `trackPublish` declines a delivery whose register store is still in
flight (`if ack != nil && ack.regPending { return 0, ... }`, R1: never put the payload on
the
wire before its identity is durable), and AckWait re-offers it. So — I argued —
first-transmission order follows whichever copy's register store confirms first, and a
message whose store lands promptly overtakes siblings still waiting.

**Why it cannot be true.** `nats.go` dispatches a subscription's messages **on a single
goroutine**, and this tree does not merely assume that, it *depends* on it —
`shim_engine.go` mutates the per-subscription delivery client's `pa.hdr` in the handler
and
says so: *"nats.go dispatches a subscription's messages on a single goroutine, so mutating
dc.pa here is race-free."* One affine callback blocked in its register store therefore
**cannot be overtaken by a sibling callback on the same subscription.** Eight siblings on
one
affine consumer cannot race each other for transmission order at all.

**And the DUP evidence is non-diagnostic for ORDERING**, which I had over-read. `dup`
comes
from the JetStream delivery count, so a copy refused *before* `trackPublish` — by the
claim
or materialization gate — carries `dup=true` on its first MQTT transmission. The flags
establish that an earlier JetStream offer happened; they do not establish where.

An earlier version of this paragraph called that DUP **legitimate**. It is not (F-072,
now CLOSED, moved the derivation onto the unsent marker):
MQTT-3.3.1-3
ties the outgoing DUP flag to whether the outgoing PUBLISH is a retransmission, and an
internal re-offer that produced no PUBLISH is not one. **That is a separate alpha-blocking
defect, filed as F-072**, and review found it in this very capture — the ordering evidence
and the DUP evidence are two defects, not one clue.

**What survives, narrowed.** The identifier is allocated **immediately before the
synchronous
register gate and enqueue**, inside the delivery callback — not at claim time and not at
copy
creation. On this fixture's serialized affine subscription that makes allocation order
equal
to enqueue order, which is what keeps the broker-side attribution intact. "Allocated at
first
transmission" was too loose a phrase for it.

Since callbacks are serialized, the reordering must already exist **in the order JetStream
offers the copies** — which points back at copy store or re-offer order, i.e. the
copy-materialization hypothesis, and leaves it UNTESTED rather than replaced.

### What the investigation still has to produce

The two candidate mechanisms were:

1. **A message was overtaken before first transmission** — a genuine same-topic ordering
   violation on the affine path, and a defect of ours. **CONFIRMED** by the identifier
   order above.
2. **A first packet was discarded by the reader** — a fixture defect. **ELIMINATED**: the
   broker's own identifier allocation shows the reordering, and `bind()` reads nothing
   from the wire.

So what is owed is the cause inside the affine path, not the side. Capture the source and
copy delivery attempts behind each identifier and the claim-materialization timeline for
the message that jumped, and test the copy-order hypothesis directly.

**Do not weaken the ordering assertion.** It caught a real reordering; the fixture was
never the problem. The first filing assumed a mechanism and was wrong twice over — first
blaming a redelivery the dedupe makes invisible, then treating the fixture as the likely
culprit while the evidence pointed the other way.

### Consequence for verification bookkeeping

**A passing `make full-suite` run means F-071 did not trigger in that run. It does not
mean
the tree has no known conformance defect.** That sentence is the whole of what a green
suite
can be claimed to show while this finding is open, and it must appear beside any such
claim.

A `full-suite` retry after **this** failure may NOT be counted as a retry after a
mechanism-proven flake — that argument covers F-069 and F-070, whose mechanisms are
demonstrated, and does not cover F-071, whose mechanism is unknown.

## F-072

**Title:** Derive the outgoing DUP flag from MQTT retransmission, not the JetStream
delivery count

- Status: **CLOSED (2026-09-09).** The sibling **unsent marker**
  (`internal/mqtt/inflight_marker.go`) is built, and the holistic finding against it — a
  surviving marker did not prove the PUBLISH was never transmitted, because the transmission
  was recorded only by a fire-and-forget marker delete issued after the socket write — is
  closed by the user-authorized redesign of 2026-09-08: the marker is consumed by a
  CONFIRMED delete as the record of **send intent**, after admission and BEFORE the write.
  Marker present = proven-unsent (DUP=0); marker absent = possibly-attempted (DUP=1,
  identifier held, expiry and withdrawal refused). The intent→write crash window is
  answered DUP=1 as the accepted conservative reading (plan N-1.2/A-1 revised by the user;
  acceptance tests T1/T2 unchanged). Validation (`make test` twice under `-race`, vet, lint,
  full-suite with both tags, cluster-failover) and four holistic rounds passed; rounds 2-3
  each found and fixed two retention/classification defects (affine expiry gate, retry
  ambiguity, generic-delete-failure classification, recovery of a possibly-attempted
  register whose source expired), round 4 swept every release path and approved. What is
  built and green:
  the register and its marker are created in one atomic batch, the transmission is recorded
  by deleting the marker, and the outgoing DUP flag is `mqttPending.transmitted`, derived
  from the marker's absence and nothing else. Both acceptance tests below hold, deterministic
  and not load-sensitive (`TestMQTTAffineRefusalBeforeTrackingSendsFirstPublishWithoutDup`,
  `TestMQTTInflightUnsentMarkerRedeliversWithoutDup`). Every refused design listed below was
  kept out. **This does NOT license a release: F-088 remains an unconditional alpha blocker.**
  Filed 2026-08-22, found by review of F-071's packet capture — the same trace carries two
  independent defects.
- Classification: MQTT conformance — PUBLISH flags
- Release treatment: **was an alpha blocker; CLOSED 2026-09-09.** It was a wire-visible
  protocol error on a first transmission, not a liveness or performance cost. Its closure
  does not license a release: F-088 and F-097 remain unconditional alpha blockers.
  **Everything below this line is the historical diagnosis and the settled design, kept as
  the record of what the fix had to satisfy, not as current obligations.**
- **Design: SETTLED 2026-08-29; it bound the attempt that LANDED it.** An eleven-round Codex
  plan loop (`~/.claude/lanes/f-072/`, plan revision 11 and its review ledger) converged on
  the **sibling unsent marker**: the register is created unchanged, a marker on
  `$MQTT.inflight.<idHash>.<gen>.<PI>.unsent` is created in the SAME atomic batch
  (`$MQTT_inflight` accordingly now requires `AllowAtomicPublish`), and the transmission is
  recorded by
  DELETING the marker at its immortal exact sequence. Neither member of the pair is ever
  rewritten BY THIS MECHANISM — claim arbitration, retirement and cancellation still
  rewrite registers, which is F-087's subject — so no sequence F-072 depends on ever
  moves. The loop
  proved the alternative — an in-place `Sent` field — unsound over eight rounds: a rewrite
  moves the record's stream sequence, and every exact-sequence operation racing it is wrong;
  five successive cover mechanisms (a completion-side delete, a terminal handoff, sequence
  lineage, regression guards, an authoritative body cache) each failed review, one of them
  leaving an unreapable `Lost`-source register holding a Receive Maximum slot. The lane's
  plan lists every refused design so none returns. **Stated before the closure**, what
  remained open was narrow: the same-delivery contested-store lifecycle and two one-sided
  recovery states (register present with the marker authoritatively absent; register absent
  with an orphan marker). **That is historical.** F-072 CLOSED on 2026-09-09; the
  same-delivery contested-store lifecycle is now **F-088**'s, planned with its completion
  half **F-097** as prerequisite, and neither is this finding's remaining work. The
  requirement below — now worded "the durable register state" — binds
  the durable **semantics** (excluding an in-memory-only flag and a `Cancelled`
  tombstone), not one record's JSON shape; a register-plus-marker pair in one indivisible
  batch satisfies it. Re-scoping the two acceptance tests was ruled outside the
  implementing lane's authority, was put to the user on 2026-08-29, and was declined: both
  tests remain binding.
- **Refused designs, none reintroduced** (the implementing lane's plan carries the full
  justifications): an in-memory-only transmission bit; reaping a "not proven written"
  register at recovery like a `Cancelled` tombstone; registering after the send; holding
  `sess.mu` across the socket write; dropping the send reservation because one dispatcher's
  callbacks are serialized; marking the transmission **in place** on the register (any
  rewrite, `Sent` field or sixth `State`, and its five cover mechanisms — a rewrite moves
  the record's sequence and every exact-sequence operation racing it is wrong); a durable
  terminal verdict; landing only the in-memory half and re-homing the durable half; a
  per-session "everything below this watermark was written" watermark; and revision 11's
  `storeContested` flag with an AckWait-driven retry (superseded by F-087's quarantine).
  **Refusal H (the in-place mark) stands, but its original justification is stale**: it was
  first refused for the sequence-movement reason above; it now also stands because the
  marker mechanism carries the fact with no register rewrite at all, so an in-place mark
  would be a second, redundant home for it.

### The finding

`trackPublish` derives DUP from the **JetStream delivery count** on the ack reply subject:

```go
sseq, _, dcount, _, _ := ackReplyInfo(jsAckSubject)
if dcount > 1 {
    dup = true
}
```

and that value reaches the wire unchanged — `WritePublishHeader` sets `mqttPubFlagDup`
from
it.

**MQTT-3.3.1-3 ties the outgoing DUP flag to whether the outgoing PUBLISH is a
retransmission of that PUBLISH packet.** A JetStream re-offer is not: when a copy is
offered
and refused *before* `trackPublish` — by the claim or materialization gate, or by the
register gate that declines while a store is in flight — **no MQTT PUBLISH was ever
sent**.
The next offer is still the FIRST transmission to the client, and it must carry DUP=0.

### The evidence

F-071's single-publisher captures show it directly. Eight messages, one publishing Client,
and seven of the eight arrive with `dup=true` **on their first client-visible PUBLISH**:

```text
m07 pi=1 dup=false | m00 pi=2 dup=true | m01 pi=3 dup=true | ... | m06 pi=8 dup=true
```

Only the message delivered on its first JetStream offer carries DUP=0. The rest had been
offered internally and refused, and the broker labelled their first transmissions as
duplicates.

This was visible in the F-071 trace from the beginning and I recorded it as
*"legitimate"* —
a conclusion about the protocol drawn from what the implementation happens to do. Review
identified it as an independent conformance defect in the same capture.

### What a client sees, and why it matters

The flag is wrong on the wire. What that costs, stated without overreach — an earlier draft
here claimed a conforming QoS 1 receiver could discard a message it never received, and
**that is not supported**: §3.3.1.1 says a receiver cannot assume DUP=1 means it has seen
an earlier copy, and QoS 1 processing is governed by packet-identifier state, so such a discard
would itself be non-conforming.

The defensible impact: an incorrect wire flag, conformance-suite failure, misleading
diagnostics for anyone reading a packet capture, and interoperability risk with clients that
are themselves non-conforming in this respect. A QoS 2 receiver is protected by the
packet-identifier exchange regardless.

### What the fix has to do

Derive DUP from whether *this allocation* has already put a PUBLISH on the wire, rather than
from `dcount`.

**An in-memory flag is NOT sufficient, and review found the window that proves it.** The
register is confirmed and the message enqueued as two separate operations:

```go
if isNew && !sess.storeInflightRegister(cc, pi, qos, sub.mqtt.jsDur, sseq, regNonce, ...) {
    return
}
if !pc.mqttEnqueuePublishMsgTo(cc, sub, pi, qos, dup, ...) {
```

A crash **after** `storeInflightRegister` confirms (clearing `regPending`) and **before**
`mqttEnqueuePublishMsgTo` queues anything leaves a non-`Cancelled` register for a PUBLISH
that was never attempted on the wire. Recovery rebuilds `pendingPublish` / `cpending` from
that register, treats the delivery as started, and the re-offer's first client-visible PUBLISH
carries DUP=1 — the same violation, reached by a different route. **No `mqttPending` field
survives a restart**, so a fix built only on in-memory state passes the obvious test and
leaves this open.

So the fix owes durable semantics: the durable register state must be able to say "identity
durable, nothing transmitted yet", distinct from "transmitted at least once". (Reworded per
the 2026-08-29 ruling from "the register itself", which falsely mandated an in-place field;
the settled design carries the fact on the register's sibling marker.) Note that a
`Cancelled` tombstone is NOT that fact — it means never-transmitted AND withdrawn.

### Acceptance criteria — TWO deterministic tests, and both are required

Neither may be a load-sensitive reproduction; a fixture that merely observes real traffic
reaches neither reliably.

1. **The pre-transmission refusal.** Force a claim or materialization refusal *before*
   `trackPublish`, then require the first MQTT PUBLISH for that message to carry **DUP=0**.
2. **The crash-and-recover window, and this one is what makes the fix durable.** A seam that
   stops *after* `storeInflightRegister` has confirmed and *before*
   `mqttEnqueuePublishMsgTo` runs; then restart, recover, and let the source be re-offered;
   then require the first client-visible PUBLISH to carry **DUP=0**.

**Test 1 alone is not sufficient acceptance**, and review had to point that out twice: a fix
built on transient state satisfies it while leaving the crash path emitting DUP=1. The second
test is the one that forces the durable representation, so a fix that passes only the first
has not closed this finding.

Both tests must be mirrored in the roadmap bullet, so neither can be dropped by whoever picks
this up.

## Verification ledger

Created 2026-10-12 by the roadmap convergence triage. It holds verification observations
that are not, on present evidence, a loss, corruption or security mechanism: test
infrastructure defects and unattributed intermittent failures. Observations whose
dangerous mechanism is still unexcluded stay in `docs/ROADMAP.md` §0.4 instead.

An entry satisfies the alpha gate's "every release-verification investigation is closed or
has produced a confirmed finding with an explicit release disposition" only through its
**disposition**: either *accepted as nonblocking* (an explicit closure on present
evidence), or *open* with its corrective work named. Rates and baseline reproduction alone
are not a disposition, and no entry waives the gate's separate requirement that the suites
pass and CI is green: an *open* entry's corrective work is owed before that gate can be
met. Never loosen an assertion to discharge an entry. The roadmap bullets each entry came
from keep their full history.

- **F-044, `TestMQTTWillOutboxWatermarkPhase2ShutdownStopsScan`.** Intermittent: 2 marker
  loads where 1 was expected, once in a sequential full-suite run; 30/30 standalone and
  nine prior full-suite passes. Mechanism: unresolved, either a scheduling-sensitive test
  assumption or an ordering gap between `closeWillDispatch` and the scan loop's stop check.
  Reproduce: the tagged full suite. **Disposition: accepted as nonblocking** on present
  evidence; reopen with a deterministic reproduction if it recurs.
- **F-118, `TestMQTTRedeliveryAckWait`.** Intermittent, "Maps should be empty, got 1, 1",
  with a log of a durable PUBLISH action that could not be drained. Base `c374099` 7 of 10
  interleaved `-race` runs. Mechanism: untraced; an unresolved drain/reservation liveness
  observation, not a proven fixture flake (F-116's neighbourhood is a candidate only).
  Reproduce: `-run TestMQTTRedeliveryAckWait -count=10`. **Disposition: open**; owed: trace
  the mechanism, and file a §0.3 finding only if it shows loss.
- **F-123, `TestF097DispatcherIsFairUnderContinuousHints`.** Intermittent, "took 4
  iterations under continuous hints, want <= 3"; 1 of 10 at `48a9003`. Mechanism: suspected
  measurement (the test samples `base` before registering both obligations) or dispatcher.
  Reproduce: tagged build, `-count=10`. **Disposition: open**; owed: a traced mechanism,
  then a dispatcher fix or a measurement correction justified by that trace.
- **`TestWillClaimantAbortsThePassOnATransientFactOutage` and
  `TestWillPendingArmAbortsThePassOnATransientFactOutage`.** Intermittent under load, the
  read count is 2 where 1 is asserted. Paired soak at `eadb22d`: Claimant 2/20 candidate
  and 0/20 baseline, Pending 2/20 baseline and 0/20 candidate; the candidate-only half is
  suspect, not dismissed. Mechanism: inferred, not settled, that concurrent passes inflate
  the count. Reproduce: `make soak` with the selector in the roadmap bullet.
  **Disposition: open**; owed: count reads for this pass only, or run each half alone under
  injected concurrency.
- **`TestMQTTExclusionReclaimWireSafetyUnderADisplacedWriterCreate`,
  `a_displaced_fresh_allocation_create`.** Deterministic, 3 of 3 at `c1b37b0`: no allocation
  observed within 5 s at the fixture-reach check. **Disposition: CLOSED 2026-10-16, a stale
  fixture; no loss.** The observation seam (`testInflightStore`) watched a standalone
  register CAS that admission no longer takes since F-105; it now observes the admission's
  authority batch for a member on the contested subject. Reaching its route showed the
  arm's other premise stale too: the refused allocation does not withhold the delivery, the
  admission moves on and delivers under the next identifier. The arm now requires that the
  contested write was attempted, that the delivery arrives once and NOT under the contested
  identifier, and nothing after it, which is the licence's claim (never two exchanges under
  one identifier). 3 of 3.
- **The custody dispatcher's exit panics the tagged binary.** Deterministic at
  `c1b37b0`: `exitCustodyDispatcher` calls `scheduleWillTimer` on a rig server whose timer
  map is nil ("assignment to entry in nil map"), taking every later test with it; triggered
  by `TestRetiredDrainMustNotTransmitAPastPubRecSource` and
  `TestRetiredDrainBarrierMustBeAtomicWithTheMapDrop`. **Disposition: CLOSED 2026-10-16, a
  test-rig defect; production servers are built by `NewServer`, which makes the map.** The
  session-capable rigs hand-built `&Server{...}` without the Will dispatch state and closed
  only `quitCh` at cleanup, so a custody dispatcher they started outlived its test. They now
  use `testRigServer` (`rig_server_test.go`): the production constructor, and a cleanup that
  stops Will dispatch (close the gate, stop the timers, close `quitCh`, then join timers,
  claim loops and admitted workers; unlike `Shutdown`, `quitCh` closes before the join,
  because a rig has no clients to drain and some tests close or reopen the gate
  themselves). Both triggers pass 3 of 3, and the tagged whole-package run completes
  without a panic.
- **`TestMQTTInflightConsumerPurgeSecuresCancellationsAuthority`.** Deterministic, 3 of 3
  at clean `e280b83`, both subtests ("the fence must be disposed of by its provenance, got
  0"; "expected 0 register(s) for this incarnation, got 1"). Found 2026-10-12 by the
  subs-barrier item's verification, identical on its tree, and not investigated there.
  **Disposition: CLOSED 2026-10-15, a stale test premise; no loss.** Bisected: green at
  `c1b37b0`, red from `5847952` (F-119), which made a pending fence disposable only
  behind a confirmed authority advance and only once no admission ladder of its nonce is
  live. The
  test's hook runs inside that ladder's own commit, so both its in-ladder disposal and the
  ladder's old "meet our own fence and dispose of it" are now refused, and the fence stands
  with the identifier excluded when the admission returns. Nothing landed beside it. The
  test's rig session was not resident, so the exclusion-reclaim pass that disposes of the
  fence afterwards disarmed itself at once; made resident, as a served session is, one pass
  disposes of it behind the advance and leaves the incarnation empty. The test now asserts
  F-119's contract in both arms: the fence stands and is then reclaimed, and an in-ladder
  disposal is refused. Mutations: dropping F-119's live-ladder check fails both arms (the
  batch lands as an exchange), and a gate that never disposes fails both (the fence is never
  reclaimed).
- **A harness data race on `mqttRetainedCacheTTL`.** Written by
  `TestMQTTRetainedMsgCleanup` while an account-manager goroutine leaked by
  `TestEvidenceForRefusesAStagedVerdict` reads it; 2 of 2 at HEAD when paired.
  **Disposition: open**; owed: stop the leak or stop mutating the global. Blocks a green
  `-race` run.
- **`TestMQTTDrainBarrierFailsClosed`, "packet identifier 1 is still reserved by a send
  hold".** Filed under F-116, which was measured as its mechanism at `a6ba9ee`. At
  `85024d9`, interleaved, 16 runs per arm under `-race`: baseline 11 of 16, F-116 tree 12
  of 16. On both trees the failing hold has no token, no PUBCOMP debt, no action and no
  retained term, only an identifier exclusion (`slotOwed`, two binds on the slot subject)
  installed when a PUBREL action drain retired without completing the exchange
  (`retireActionLocked_`), not reclaimed within the test's 10 s. F-116's fix removes the
  debt term and leaves this one. Mechanism: unclassified; whether the exclusion reclaim
  pass should have lifted it after the exchange's terminal cleanup is what to establish.
  **Disposition: open**; owed: classify why the exclusion outlives the completed exchange,
  and file a §0.3 finding only if it shows loss.
- **Per-exchange slowdown on one long-lived connection.** Measured by F-116's verification
  at `85024d9` with the F-105 long-lived schedules: per-iteration time grows roughly
  linearly with the exchanges already carried (S1-S5/S7 under `-race`: 1.9 s to 4.1 s over
  25 iterations; without `-race` all eight schedules 0.7 s to 1.7 s), while the in-memory
  census stays flat. Not attributed. **Disposition: open**; owed: find which store-side
  read grows with history, and decide whether it is a resource finding.
- **`TestF105RetainedAdmissionIsALedgerCharge` and
  `TestMQTTv5RetainedTransmittedSendSurvivesAResume`: a withheld live delivery arrives
  late after a retained exchange frees the only slot.** Met by F-116's verification
  (2026-10-14). Both run at Receive Maximum 1 with a retained exchange holding the slot,
  complete it (PUBACK; PUBREC, PUBREL, PUBCOMP), then read the live delivery that waited
  behind it, and intermittently time out on that read ("Error reading data: ... i/o
  timeout"). Interleaved prebuilt `-race` binaries, 11 runs per arm in one four-test
  combination: the first failed 3 of 11 on both `85024d9` and the F-116 tree, the second
  4 of 11 and 3 of 11; each passes alone. Not caused by F-116. Mechanism: untraced;
  whether the withheld delivery waits for an AckWait re-offer rather than a prompt redrive
  once the slot frees is what to establish first. **Disposition: open**; owed: trace which
  path re-offers the withheld delivery, and file a §0.3 finding only if it shows loss.
- **`TestMQTTMaxAckPending` temp-directory cleanup.** Once under full-suite load, `TempDir
  RemoveAll cleanup: ... directory not empty`: Go's cleanup racing JetStream's file removal,
  not a broker assertion; 0/12 in isolation on both arms. **Disposition: accepted as
  nonblocking**; reopen if it recurs.
- **`TestMQTTShutdownV5SendsDisconnect`.** Once in the tagged suite at F-119's
  verification, then 5 of 5 on each tree. **Disposition: accepted as nonblocking**; reopen
  if it recurs.
- **`TestMQTTQoS2InflightMsgsDeliveredAfterUnsubscribe` at an AckWait of 250 ms (F-115).**
  Its strict packet order after PUBREC 2 raced the fixture's own retry timer: with F-115
  fixed, PI 2's payload was re-sent 0 of 20 traced runs, but in 9 of 20 the legitimate
  retransmission of the never-acknowledged PI 3 preceded PUBREL 2, exactly the runs in which
  a PI 2 re-offer opened a send section whose write F-115 now prevents and whose drain
  PUBREL admission waits for (PUBREL 2 at ~100-150 ms after PUBREC 2, against ~35-97 ms
  otherwise). A fixture timing budget, not a protocol defect: the test's AckWait is now
  2 s with every assertion unchanged, 20 of 20 standalone. **Disposition: accepted as
  nonblocking**; an answered section still holds PUBREL admission until it drains
  (latency only), which F-115 deliberately does not change.
- **`TestExchangeQoSWireMatchesPreservedRecord`.** Deterministic, 3 of 3 at clean
  `1a15a49` ("the occupant was never injected, so the replacement arm was not reached"), in
  the tagged suite. **Disposition: CLOSED 2026-10-16, a stale fixture; no loss.** It wrote
  its QoS 2 occupant through the standalone register CAS seam, which admission no longer
  takes since F-105. The occupant is now ADMITTED through the production authority batch
  (`opAdmitPublish`) from inside the caller's own admission, so the caller's batch collides
  with a proven-unsent same-delivery occupant and the ladder takes the pair-replacement
  arm. The wire QoS and the register QoS agree on the occupant's 2 for a delivery the
  caller computed at 1, 3 of 3.
- **`TestRetainedResetCreateResponseLostReconciles`.** Intermittent ("want exactly one
  observer consumer, got 0"), in the default `-race` suite: 2 of 2 at clean `1a15a49`, 2
  of 10 at `7551cb6`. **Disposition: CLOSED 2026-10-16, a test read; no loss.** On every
  failing run the observer existed (a lookup by name found it), indexing worked, and the
  listing reported it within 500 ms: a clustered CONSUMER.LIST read right after the create
  had not reported the new consumer yet (it is reported as missing until it does). The test
  now requires the observer by name, fails at once on any listing with more than one, and
  reads the listing until it reports exactly one. 15 of 15 under `-race`.
- **`TestMQTTQoS0DowngradeAmbiguousLandedSaveLeavesTheDurableToTheSweep`** (renamed
  `...LeavesTheDurableToTheNextTenure`). Intermittent, "the durable must survive a CONNECT
  that reconciled from the record": 2 of 5 at clean `1a15a49`, 6 of 20 at `7551cb6`.
  **Disposition: CLOSED 2026-10-16, a stale test; no loss.** It pinned the pre-F-105
  residual ("the orphan sweep is the only convergence") and required the durable to survive
  both reconnects. The documented convergence (`docs/CONFORMANCE.md`, "On an AMBIGUOUS
  landed save the durable loses its prompt cleanup path") is the next accepted tenure's
  settlement: measured, the first reconnect's settlement decides the durable
  `tenure-superseded` and the owner retires and deletes it, racing the old assertion. The
  test now asserts that convergence (decision, RETIRED name, durable gone, no teardown) and
  keeps the record-restore check. 20 of 20.
- **`TestMQTTFlappingSession`.** Deterministic, 3 of 3 at clean `903bee1` under `-race`
  ("Did not find the client ID in the flappers map"), identical on F-040's tree. Found
  2026-10-14 by F-040's verification. Mechanism: unclassified; it asserts the flappers
  jail entry a takeover adds, so whether the jail is skipped in production is what to
  establish first. **Disposition: open**; owed: classify the mechanism.
- **`TestMQTTQoS2RetriesPublish`.** Deterministic, 3 of 3 at clean `903bee1` under `-race`
  ("Expected nothing, got [98 2 0 1]", a PUBREL where the test expects silence), identical
  on F-040's tree. Found 2026-10-14 by F-040's verification. Mechanism: unclassified.
  **Disposition: open**; owed: classify whether the PUBREL is a protocol duplicate or a
  fixture expectation that predates the F-115/F-116 PUBREL handling.
- **`TestMQTTInflightStrandedReservationOverADeletedConsumerIsReleased`.** 3 of 3 at clean
  `903bee1` and 2 of 3 on F-040's tree, interleaved under `-race` ("fixture: F-100's reap
  must have released the entry"). Found 2026-10-14 by F-040's verification. Mechanism: the
  fixture does not reach its route; what stops the reap is unclassified. **Disposition:
  open**; owed: make the fixture reach F-100's reap. Re-measured 2026-10-15 at `b334415`
  by the `TestMQTTInflight*` family attribution: 11 of 11 family runs, and it is now the
  only member that keeps the family run red; still carried here, not by that bullet.
- **`TestMQTTInflightLandedButUnconfirmedAckLeavesNothing`, historical intermittent.** One
  of the four members the 2026-09-01 family bullet named; the bullet never recorded which
  assertion failed, and it reproduced 0 of 19 family runs at `b334415` (11 at
  `-parallel 8`, 8 at `GOMAXPROCS=2`). Mechanism: **unattributed**. A candidate was found
  and removed: its hook modelled "the acknowledgement landed, its confirmation was lost"
  with a fire-and-forget ack, so "landed" was not established when
  `resolveRetiredSource` read the ack floor (a 500 ms-late landing still cleared, so the
  candidate is a slower clear, not a leak). The hook now makes the real confirmed
  submission first, and the retirement is asserted before the PUBCOMP, where terminal
  cleanup cannot clear it independently — which also made the test's documented mutation
  real (it could survive before). **Disposition: accepted as nonblocking**; reopen if it
  recurs.
- **`TestMQTTInflightRetiredSourceSurvivesRestart`.** Intermittent under CPU starvation
  ("the register must be marked retired before the restart, got `<nil>`": the register is
  already gone, not merely unmarked). Interleaved at `GOMAXPROCS=2 -parallel 8`, the
  `^TestMQTTInflight` family on a prebuilt tagged `-race` binary: 4 of 18 at clean
  `b334415` and 4 of 18 on the family-attribution tree (2026-10-15, two batches of 8 and
  10 per arm); 0 of 19 in the unstarved runs. Pre-existing; the counts establish
  provenance, not improvement or cause, and it is not attributed to the QoS 1 re-offer
  residual (this is QoS 2).
  Mechanism: unclassified; what deletes the register of a retired exchange before the
  restart is what to establish first. **Disposition: open**; owed: classify, and file a
  §0.3 finding only if a completed payload is re-sent.
- **`TestMQTTInflightRecoveryPubRelKeepsOwnerWhenSourceComplete`. UNMATCHED.** Once in
  CPU-starved `^TestMQTTInflight` family runs on the family-attribution tree (2026-10-15):
  the owner slot read back `pubrel_intended` where `pubrel_ready` is asserted. 1 of 28
  starved family runs on the change against 0 of 18 at clean `b334415` (10 of each
  interleaved); 0 of 200 per arm in isolation (`GOMAXPROCS=2`, 50 concurrent instances,
  four alternating batches each). Neither pre-existence nor causation is established. The
  change adds no call on its path (the prompt handoff is called only from send sections,
  and recovery opens none), which does not exclude a suite-level scheduling effect.
  Mechanism: unclassified; whether route B's re-phase completes after
  `recoverInflightRegisters` returns is what to establish first. **Disposition: open**;
  owed: a reproduction on either arm, then classify.
- **`TestMQTTInflightQoS2IdentifierSurvivesRestartBeforePubRec`. UNMATCHED.** Once in 10
  interleaved CPU-starved family runs on the family-attribution tree, 0 of 10 at clean
  `b334415` (2026-10-15): "Expected ping response got 60" at the warm-up loop's flush
  after PUBCOMP — a PUBREL where the flush expected PINGRESP, the shape of a PUBREL
  retransmitted at its 1 s AckWait before the PUBCOMP was processed. The change does not
  touch the PUBREL/PUBCOMP path (only PUBACK and failed-PUBREC completions), which again
  does not exclude a scheduling effect. **Disposition: open**; owed: a reproduction on
  either arm, then classify whether the PUBREL is a legitimate retransmission the fixture
  does not expect.
- **`TestMQTTDrainClusterDefiniteIntentFailureResumesElsewhere`.** Intermittent, once in a
  sharded tagged `-race` run on F-040's tree ("Expected PUBLISH (30), got 60"), and 3 of 3
  green standalone on that tree and on clean `903bee1`. Found 2026-10-14 by F-040's
  verification. Mechanism: unattributed; a PUBREL arrives where the re-offered PUBLISH is
  expected, under suite load only. **Disposition: open**; owed: a reproduction under load,
  then classify.
- **`TestMQTTAckWaitConfigChange`.** Deterministic, 3 of 3 at clean `28e5516` under
  `-race` ("Expected topic "bar", got "foo""), identical on F-117's tree, interleaved.
  Found 2026-10-14 by F-117's verification. Mechanism: unclassified; a message on the
  earlier topic arrives where the test expects the later one after an AckWait change, so
  whether that is a redelivery the test does not expect or a stale delivery is what to
  establish first. **Disposition: open**; owed: classify the mechanism.
- **`TestMQTTDrainRestoredClaimRedeliveredAfterRestart`.** Intermittent, 4 of 6 at clean
  `28e5516` and 3 of 6 on F-117's tree, interleaved under `-race` ("Expected payload
  "payload", got "burn""). Found 2026-10-14 by F-117's verification. Mechanism:
  unclassified; a different message arrives where a restored drain's claimed sequence is
  expected, so whether the claimed message is lost or only reordered is what to establish
  first. **Disposition: open**; owed: classify the mechanism, and file a §0.3 finding only
  if it shows loss.
- **`TestMQTTv5SharedSubscription`.** Intermittent, once in a sharded tagged `-race` run on
  F-117's tree ("message 10 not delivered to any group member"), and 6 of 6 green
  standalone on that tree and 6 of 6 on clean `28e5516`, interleaved. Found 2026-10-14 by
  F-117's verification; F-117 changes nothing on the shared route (a shared subscription
  carries no No Local filter key, so its No Local decision is unchanged). Mechanism:
  unattributed, under suite load only. **Disposition: open**; owed: a reproduction under
  load, then classify.
- **`TestMQTTRestoreQoS0OverLiveConsumerCompletesQoS1Exchange`.** Intermittent at `5847952`:
  1 of 6 on each of two trees, interleaved (the takeover-settlement item's verification).
  NOT REPRODUCED at `b5ec59b`: 0 of 18 isolated runs (the `c1b37b0` classification item,
  2026-10-16). Mechanism: unattributed; nothing measured shows which assertion failed or
  why. **Disposition: accepted as nonblocking** on present evidence, which is a failure to
  reproduce, not a resolution; reopen with the failing assertion and a reproduction if it
  recurs.
