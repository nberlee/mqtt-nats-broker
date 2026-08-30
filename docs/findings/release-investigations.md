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

## F-069

**Title:** Fence the orphan-reconcile fixture against the background sweep it does not
drive

- Status: Open, with the mechanism PROVEN rather than suspected. Filed 2026-08-22, found
  while verifying F-034.
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

## F-070

**Title:** Establish the affine contender fixture's rival-create premise rather than
racing it

- Status: Open, with the mechanism PROVEN and the attribution SETTLED against a baseline.
  Filed 2026-08-22, found while verifying F-034.
- Classification: Test-harness race
- Release treatment: Post-alpha cleanup. Like F-069 this is a fixture premise rather than
  broker behaviour, and the assertions the test makes are correct.

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

- Status: **Open. Alpha blocker, and demonstrated to be OURS.** Filed 2026-08-22 while
  verifying F-034; reclassified the same day when review refuted the redelivery
  explanation; then REPRODUCED with instrumentation, which showed the broker's own
  packet-identifier allocation transmitting the sixth-published message first. The fixture
  is exonerated. The cause inside the affine path is not yet identified.
- Classification: Ordering defect against an INTENDED implementation property — NOT a spec
  violation; see the basis discussion
- Release treatment: **Alpha blocker — by product judgement plus a stated implementation
  property, NOT by MQTT conformance.** The specification permits reordering on a shared
  path; this broker says it preserves per-(Topic,QoS) order and three rewritten tests rest
  on that. Disagreeing with the judgement means reclassifying to a post-alpha product-path
  anomaly — not weakening the assertion that found it.

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

So F-071's alpha-blocker status rests on two things, neither of them a spec citation: that
stated property, and the judgement that silently reordering one client's same-topic QoS 2
messages is not acceptable behaviour for this broker whatever the specification permits. If
the reviewer or the user disagrees with that judgement, the honest reclassification is
"product-path anomaly, post-alpha" — and **not** a weakening of the assertion that found it.

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

An earlier version of this paragraph called that DUP **legitimate**. It is not:
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

- Status: **Open. Alpha blocker. UNBLOCKED 2026-08-30** —
  [F-087](ha-arbitration.md#f-087-rebuild-every-in-flight-register-rewrite-from-the-authoritative-reread)
  landed, so the rewrite rules this design rests on are settled: no writer erases a
  monotonic fact any more, and the same-delivery contested store now has a converging
  disposition. Filed 2026-08-22, found by review of F-071's packet capture — the same
  trace carries two independent defects.
- Classification: MQTT conformance — PUBLISH flags
- Release treatment: Alpha blocker. It is a wire-visible protocol error on a first
  transmission, not a liveness or performance cost.
- **Design: SETTLED, 2026-08-29, and binding on the next attempt.** An eleven-round Codex
  plan loop (`~/.claude/lanes/f-072/`, plan revision 11 and its review ledger) converged on
  the **sibling unsent marker**: the register is created unchanged, a marker on
  `$MQTT.inflight.<idHash>.<gen>.<PI>.unsent` is created in the SAME atomic batch
  (`$MQTT_inflight` gains `AllowAtomicPublish`), and the transmission is recorded by
  DELETING the marker at its immortal exact sequence. Neither member of the pair is ever
  rewritten BY THIS MECHANISM — claim arbitration, retirement and cancellation still
  rewrite registers, which is F-087's subject — so no sequence F-072 depends on ever
  moves. The loop
  proved the alternative — an in-place `Sent` field — unsound over eight rounds: a rewrite
  moves the record's stream sequence, and every exact-sequence operation racing it is wrong;
  five successive cover mechanisms (a completion-side delete, a terminal handoff, sequence
  lineage, regression guards, an authoritative body cache) each failed review, one of them
  leaving an unreapable `Lost`-source register holding a Receive Maximum slot. The lane's
  plan lists every refused design so none returns. What remains open is narrow: the
  same-delivery contested-store lifecycle (now F-087's converging-takeover scope) and two
  one-sided recovery states (register present with the marker authoritatively absent;
  register absent with an orphan marker). The requirement below — now worded "the durable
register state" — binds
  the durable **semantics** (excluding an in-memory-only flag and a `Cancelled`
  tombstone), not one record's JSON shape; a register-plus-marker pair in one indivisible
  batch satisfies it. Re-scoping the two acceptance tests was ruled outside the
  implementing lane's authority, was put to the user on 2026-08-29, and was declined: both
  tests remain binding.

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
