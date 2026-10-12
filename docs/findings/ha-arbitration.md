# HA arbitration and write-lineage findings

These findings share one architectural dependency: JetStream per-subject CAS is a
linearization point only within a leader term. Treat them as one design epic before
scheduling stream-specific work.

**That shared premise was RETIRED on 2026-08-16 rather than assumed, and it did not
hold up.** F-030 enumerated every route a write conditioned on a superseded base could
take to a proposal across a leader transition and exercised each against the supported
floor (`test/depproof/`). Both live routes are closed by the combined route/mutation
evidence, the pinned source mechanisms, and the NATS maintainer's explicit confirmation
that leader-local in-flight ownership and applied-before-serving enforce the header
across elections. The mechanisms are stream-independent, so the per-stream findings
inherit the server contract and the canary — but each owns a consequence of its own, and
none closes on this evidence alone. All three per-stream audits have now run, F-034's
last, on 2026-08-22, and it is the only one whose consequence turned out to be reachable.
F-032's ran on 2026-08-17: it inherited the contract without re-proving it, needed no
lineage, and found a local defect that JetStream's guarantees never spoke to — **F-062**,
filed below and since closed. F-033's ran on 2026-08-18 to the same shape: no lineage, and
three local defects the server contract never spoke to — **F-063**, **F-064**, **F-065**
and **F-066**.

Multi-frontend HA is supported alpha scope (decided 2026-08-10, `docs/ROADMAP.md`
section 0.2), so every finding here is an unconditional alpha blocker.

The one arbitration design these findings share was made on 2026-08-11 and is sliced
in dependency order: Slice A (the observation and convergence backbone) landed first
and NARROWED F-036 without closing it, F-029 is Slice B and closed on 2026-08-11
leaving F-029a and F-029b behind, F-027 is Slice C, closed on 2026-08-12, and both
remainders are now closed too — F-029a on 2026-08-12 and F-029b on 2026-08-13. Each
remaining slice gets its own implementation-depth plan against that design; the roadmap
bullets carry the constraints each must answer.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10.

## F-027

**Title:** Repair a recognised session-record fork

- Status: **Closed 2026-08-12** (Slice C), for the recoverable subset; every bound is
  recorded in `docs/CONFORMANCE.md`, and shared-subscription membership stays excluded
  until F-050 closes
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 27

### Original finding record

- [ ] A proven fork is recognised but never REPAIRED: the surviving sibling stays and the
  rightful owner's granted state is lost. A naive repair was designed and rejected — it
  would resurrect an incarnation a teardown had already deleted (`mqtt.go`, "A CLAIMED
  teardown must be FINISHED, never resumed and never overwritten"), it needs a
  last-durable byte snapshot that does not exist (`save()` discards
  `mqttSessSaveDetail`), its worker races a same-broker reconnect with no ownership guard,
  and the repair CAS is itself exposed to the window it repairs. Any future attempt has to
  answer all four.

### What closed it

A fork's durable loss is exactly the record DELTA. The anchor condition
`classifyRecordLineage` tests means the survivor and the erased record are siblings of ONE
NAMED parent, so that parent is a single-hop common ancestor and a three-way merge against
it is well defined — which is what a union cannot be: with base `{a}`, ours `{a,b}` and a
survivor `{}` carrying an acknowledged UNSUBSCRIBE of `a`, a union resurrects `a`, because
"absent from the survivor" is ambiguous between a lost addition and a survivor-side
removal.

Two grant snapshots, both built from the persisted RECORD rather than from memory, rotate
at every adoption that can later mint. A forked verdict at any of seven classification
points mints an in-memory capsule of the delta, inside the transaction that classified.
The client's own reconnect re-asserts it through the ordinary accept-save CAS, behind the
record fence and strictly after the CONNECT's uncertainty resolution.

That answers the four rejections rather than waiving them: generation-anchored, so a
retired incarnation can never be resurrected; a semantic delta rather than a byte replay,
so no snapshot `save()` discards is needed; no worker and no repair write, so nothing
races a same-broker reconnect; and the write IS the accept save, so it inherits F-029's
conditional arbitration, rejection classification and re-drive instead of opening a window
of its own.

The repair claim is: an acknowledged SUBSCRIBE or UNSUBSCRIBE a proven fork erased is
re-asserted **where the survivor made no grant decision about the same filter**. It is NOT
that every recognised loss is recovered — a conflict is left to the survivor and counted,
a restart forfeits the capsule, and the cardinality bound forfeits the oldest client's.
See "Repairing a recognised fork" in `docs/ARCHITECTURE.md`.

## F-029

**Title:** Arbitrate simultaneous takeovers from one session-record base

- Status: **Closed 2026-08-11** (Slice B), with two shapes explicitly left open and
  filed as their own `docs/ROADMAP.md` §0.2 blockers — see F-029a and F-029b below.
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 29

### What closed it

Every session-record write is conditional, base 0 included, so a first-ever create is
CASed on "the subject must currently be empty" instead of landing on top of whatever is
there. A superseded teardown claim is a typed outcome rather than success, so the path
that produced the unconditional create no longer runs; four callers are terminal on it
and two re-drive. `finishAdoptedTeardown`'s superseded release is surfaced rather than
swallowed — the one production path that wrote on a base it had authoritatively been
told was stale. The lineage chain crosses the teardown re-mint (`reMintedFrom`), and the
incarnation generation no longer short-circuits the fork test, because a clean-start
sibling always carries a different generation and that short-circuit hid exactly the
fork the rule exists to catch. See "Two frontends taking over one session from the same
base" in `docs/ARCHITECTURE.md`.

The convergence claim is: every interleaving converges on one owner, and each client is
told something true. It is NOT that neither side destroys what the arbitration awarded
the other — that is F-029a.

## F-029a

**Title:** The claim-won deletion window

- Status: **Closed** on 2026-08-12 by the generation-retirement watermark
  (`internal/mqtt/sess_teardown.go`, `$MQTT_teardown`; `docs/ARCHITECTURE.md`, "The
  generation-retirement watermark").
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Filed: 2026-08-11, out of F-029's implementation

A teardown that WINS its claim deletes the retiring generation's consumers and durables
before its release tells it whether it still owns the claim, so a cross-term resume off
the same base can survive with a record naming state that is already gone. Pre-existing:
a property of the teardown protocol, not something F-029 introduced. Reordering release
before delete was designed and refuted — `releaseTeardown` proves absence rather than
authorship, the unsafe interval merely moves, shared group durables are not
generation-scoped, and after the release the durable target list is gone while PUBREL
durables have no orphan reaper. The roadmap bullet carries the full refutation.

**How it was closed.** The refutation above says what the fix had to be: a durable
teardown intent that does not live on the replaceable record subject. Every teardown that
wins its claim now records a permanent per-client retirement watermark on a dedicated
`$MQTT_teardown` stream — naming the generation and the `$MQTT_sess` sequence its claim
marker landed at — before its first deletion, always confirmed, and fails the teardown
closed with nothing deleted if it cannot. Three readers make the window recognisable and
repair it: the winner's own release-superseded arm condemns a same-generation survivor
with a fresh teardown marker (mark only, so the survivor's broker can still observe the
move); the CONNECT restore refuses to resume a retired generation and routes the record
through the existing overdue-discard shape instead; and the orphan sweep condemns one
with no CONNECT at all, deferring the finish to a later pass. Replacement is ordered by
the marker sequence alone, nothing ever reaps a watermark, and the stream is epoch-coupled
to `$MQTT_sess` so a partial recreation of the unit is refused at startup. The repair is
truthful TERMINATION, never restoration: the deleted state stays deleted, and what is
guaranteed is that no client is served as though it existed. The stacked apply-time
escapes that remain — the fence names only the latest retirement, and the slot itself can
be regressed by a cross-term apply — are residuals in `docs/CONFORMANCE.md`, strictly
narrower than the parent CAS hole they come from.

## F-029b

**Title:** Cross-term create × create recognition

- Status: **Closed** on 2026-08-13 by inheriting the retired record's identity, then
  **REOPENED as a stated residual on 2026-08-16** by the write-lineage audit, which
  deleted that inheritance and every other one (`docs/ROADMAP.md` §0.2;
  `docs/CONFORMANCE.md`, residual R-create). The closure section below is kept because
  the mechanism it describes is what was deleted and why.
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Filed: 2026-08-11, out of F-029's implementation

Two brokers creating from an empty subject across a leader term both apply; the later
survives and the erased side concedes 0x8E, correct about ownership and wrong about
cause. Both records are parentless, and the rule requires a non-empty acknowledged
parent precisely so two unrelated first-creates do not read as siblings. A `bound`-gated
parentless arm was designed and refuted: its premise is false, and a FALSE fork verdict
suppresses delayed-Will cancellation, so this is not a vocabulary-only gap. The roadmap
bullet carries the full refutation.

**How it was closed.** The finding asked for a durable identity for each empty-subject
interval that both racing creates inherit, and that fact was already recorded,
permanently and per client, before every deletion: the generation-retirement watermark.
It gained one required field, `rwid`, naming the RECORD its deletion removes — the record
the teardown claimed, never the marker that claimed it; **that field has since been removed
outright, and the reasoning is below** — and a fourth reader, the
record-absent create, which inherits it as the parent of its first write immediately
after the proof that the subject is empty. So a create becomes a child of the record it
replaces: two creates in one interval are siblings of each other, a create in a later
interval is a descendant, and ordering survives deletions with no new comparison
anywhere. `classifyRecordLineage` is untouched; the whole change to the seven consults
is where a parent token comes from.

**That fourth reader no longer exists.** The write-lineage audit deleted it, along with
the other three sources; `rwid` stayed on the watermark with no reader and has since been
removed outright (see below), and a create is
parentless again. The reason is in the section below and in `docs/ROADMAP.md` §0.2: every
route by which the inheritance produced a forked verdict was a documented FALSE fork.

Three designs were refuted on the way and are recorded so they are not re-proposed: a
separate per-client deletion-fact stream, whose objection was answered at the root by
the rollback rework below and which otherwise adds a second pre-deletion marker stating
an overlapping fact; inheriting the watermark's `nonce`, which misses the MIXED pair
where one broker tears the record down locally while the other only sees an empty
subject; and minting a retirement watermark at a first create, which would fence the
session being created and unfence a generation a real teardown retired.

The invariant the design rests on is that every record deletion this broker performs
advances the interval identity — so the CONNECT rollback's bare sequence-targeted delete
had to go. It now runs the real teardown, whose CLAIM is a CAS at the candidate's own
sequence: a same-generation successor rejects it, nothing is deleted and nothing is
fenced, while a won claim means the record it retires really is current. Recording a
fence without holding the base would have permanently condemned a legitimate successor.
That rework also fixed a live defect of its own — the bare delete branched on the
interval the CONNECT REQUESTED rather than the session's effective persisted lifetime,
so a rejected resume that asked for 0 destroyed a persistent session's record. It also
carries the one piece of new exposure this closure has: the claim it takes can be
regressed by a cross-term apply, which the bare delete could not reach, and that is
stated as a residual rather than claimed away.

**THIS CLOSURE HAS SINCE BEEN REVERSED, cause and all.** The record-absent create with no
usable watermark was filed as its own `docs/ROADMAP.md` §0.2 bullet and closed by the
session-interval register — a per-client single-slot register on `$MQTT_origin` whose
sequences only move forward, rotated by every teardown from inside `recordGenRetirement`.
The write-lineage audit deleted that register, its stream and every other source a create
drew a parent from, and `$MQTT_origin` is now refused at startup.

The reason is what the residual tree had been recording all along. Four routes were
already stated as FALSE forks (R1, R2, R3, R4) and the register added a fifth (R0c, a
restore or live recreation replaying an identity); each needed a create carrying an
inherited identity that had outlived the interval it named, and each fired the same
verdict a true cross-term create pair would. The register also WIDENED R1 and R4 to the
no-watermark population, which is the same effect stated from the other side: a create
that used to concede now derived an identity and could reach both routes. Audited
obligation by obligation, there was no route on which the inheritance produced a true
verdict and not a false one, so the mechanism was deleted rather than narrowed. The
cross-term create pair concedes again — pre-F-029b behaviour, restated as residual
R-create in `docs/CONFORMANCE.md`.

What survives F-029b is the machinery around it: the CONNECT rollback's real teardown,
its effective-lifetime branch, the parked discard, and the generation-blind classifier.
What survived on the record was `rwid`, offered as the only durable statement of *"this
specific record was retired"* — with **no reader at all**, since the create's valid arm no
longer consults it and the register once offered as its replacement does not exist. **The
field has since been REMOVED**, and both candidate readers are why.

F-036 failed first. Its 2026-08-27 attempt established that the watermark holds ONE SLOT per
client and that `recordGenRetirement` treats any standing higher `MarkerSeq` as sufficient,
so requiring `ackWID == rwid` recreates the superseded-watermark false negative the fix
exists to close: after two teardowns for one client ID the original silent bound session
could never be proven displaced. The licence that attempt found is `MarkerSeq`-only and
consults neither `Gen` nor `rwid`.

"Give a vanished record its own disposal vocabulary" was the last candidate and decided
against it too, which closed that box as refuted. The statement above is the defect: `rwid`
is not a statement that *this specific record* was retired, because a write identity
names a record's BYTES and not one OCCURRENCE of them. `storeFramedSessionMsg` replays
acknowledged bytes verbatim, the readback adopts the higher sequence with the original
`WID`, and
restore adopts `WID`/`PWID` unvalidated — so one identity can stand at two sequences,
and a teardown that claimed the later one satisfies `rwid == ackWID` against a session
holding the earlier. `MarkerSeq` bounds the removed occurrence below the marker but does
not say which, so no conjunction of surviving fields is occurrence-exact. An exact reader
needs the claimed PREDECESSOR SEQUENCE on both the marker and the watermark, refused as
disproportionate to a diagnostic string. What the field also enforced — a record carrying
no `WID` fails its teardown closed — is now an explicit precondition in
`recordGenRetirement`.

The bounds that held for the false-fork routes still hold for what remains: no false
verdict can resurrect state, because the anchor's generation conjunct drops any capsule
minted for a create, and a survivor carrying a retired generation is condemned by the
retirement fence.

### Original finding record

- [ ] Takeover versus takeover at one base is unordered. With the persist observation now
  classifying, both sides WITHHOLD the claim on a recognisable (resume-shaped) fork — but
  nothing orders or arbitrates the two evictions themselves, a clean-start side's
  re-minted generation still concedes 0x8E, and a lost observation decides nothing.
  Converging the two brokers on one owner needs the arbitration this item's parent bullet
  could not provide.

## F-030

**Title:** Add write lineage to the Will outbox

- Status: **CLOSED 2026-08-16.** No server defect was reproduced and no lineage was
  added. The cross-election failure premise is disproved by three independent layers:
  the exhaustive route inventory and mutation evidence in `test/depproof/`, the pinned
  server mechanisms, and the NATS maintainer's explicit confirmation of the leader-only
  contract across elections.
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 30

### Original finding record

- [ ] `$MQTT_willq` carries no write lineage, so a create-only enqueue erased by a
  same-base sibling across a leader transition is unrecognisable. The Will it named is
  then either lost outright — nothing re-enqueues it, and the enqueuing incarnation has
  moved on — or, if the surviving sibling holds an earlier state, re-fired after `done`
  was supposed to have retired it. The record shape and the classifier from
  `sess_fork_lineage.go` are reusable; this stream's CONSEQUENCE analysis is not.

### What the acceptance-fact change had already closed

The **cancellation half** was closed WITHIN a `$MQTT_willq` leader term by F-031: every
claimant takes its claim before it decides, persists the decision and the cutoff it was
taken under with the CAS that licenses it, renews that claim while scanning, and
re-checks its budget immediately before licensing. A recovering claimant EXECUTES the
persisted decision instead of re-deciding.

What stayed open was the **terminal half**: a committed `done` taken back to
`publishing` by a write conditioned on the base `done` replaced, after which a claimant
enters on the persisted decision and republishes. That is not the sanctioned
at-least-once duplicate — this repository draws that line by EVIDENCE rather than by a
crash, and here completion evidence the system already held would have been discarded.

### What has been established, and what has not

By testing the premise instead of building on it. The premise was that a write's
per-subject expectation is checked pre-proposal and the in-process map is cleared by a
leader transition, so two writes taken from one base could both apply. Every
`$MQTT_willq` write this broker makes is already conditional, so the finding reduces
entirely to a property of the server, and the server was measured — on the supported
floor and on a generation predating its proposal fence.

`test/depproof/` holds the route inventory, the fixtures and one mutation per
mechanism, driven by `test/depproof/run.sh --matrix` (also `make depproof`) inside
isolated nats-server checkouts. Two routes can carry such a write to a proposal across
a transition, and each is refused by a named mechanism:

- **R-old — CLOSED.** An operation whose captured proposing term outlives that term,
  refused by Raft's comparison of the captured term against the current one
  ([`nats-io/nats-server#8370`](https://github.com/nats-io/nats-server/pull/8370), in
  the pinned floor since 2.14.4 and re-verified on each bump by `make depproof`). Two
  mutations discriminate it: removing the comparison, and
  substituting the node's current term for the one the operation captured.
- **R-new — CLOSED.** A newly elected leader admitting a
  CURRENT-term write, which the term comparison cannot refuse. Refused by two
  mechanisms, each with its own mutation. A READINESS envelope with two redundant
  halves: a node behind on applies does not take
  leadership, and a new leader is not announced as serving until the log it was elected
  with has been processed. Each half is isolated by its own mutation, and the redundancy
  is measured — with the campaign refusal alone removed a node leads from a stale applied
  view and the stream still refuses to serve; with the announcement alone removed nothing
  changes at all; only with both removed does a leader serve from the superseded base and
  propose a write carrying it, while the unmutated server produces neither. Then, once a
  leader IS serving, the per-subject expectation it compares against is reconstructed
  from its own APPLIED state, which contains `done`; bypassing exactly that comparison
  is the second mutation.

Seven further shapes — a stale-log election, in-term contention, an unclustered stream,
the batch path, `ForwardProposal`, this broker's own sequence-targeted deletes, and an
entry that reaches the log anyway — are refuted in writing in
`test/depproof/README.md`. Mirror/source ingestion is classified rather than refuted:
initial validation excludes it from a conforming stream, while post-start source drift
remains outside F-030. The closure claim is the INVENTORY, not the fixtures.

### The control run falsified the plan's own prediction

The plan required a v2.14.3 control showing "both operations apply and the stale
reclaim is the surviving record". **That prediction is false, and is recorded here
rather than quietly restated.** What v2.14.3 shows is that the older-term write IS
ADMITTED at the proposal boundary — which is the mechanism #8370 changed, and is what
makes the fixture evidence rather than an artefact — but the entry is then discarded by
two version-common backstops: the apply-time log-order comparison, and beneath it the
store, which refuses any sequence that is not the next one. So the proven version
difference is proposal ADMISSION, and the terminal record stands on both generations.
Neither backstop is mutation-proved and neither is part of the claim: making an
admitted entry survive would take a second mutation to the store itself, which is a
server that does not exist.

That is why every fixture asserts at the boundary its mutation targets, with the
record-state assertions kept as documented defence in depth: a fixture asserting only
the final record could not discriminate any of these mechanisms, since the record
survives with all but one of them removed.

### Why closure does not rest on the harness alone

R-new's mechanisms refused the write in every run measured, and no run has ever
produced the hazard on the supported floor.

Four adversarial review rounds each found a distinct false-positive path in R-new's
oracle — a mutation that staged the wrong state and reported success; a scorer that
counted a kill without its boundary assertion firing; a scorer that required a marker but
did not forbid the others; and a fixture whose final observation was unmarked, so a late
proposal scored as the expected kill. Each was real and each was fixed. That history
still prevents treating the harness as the sole authority. It no longer prevents closure
because upstream has now stated the contract independently: a NATS maintainer confirmed
that the leader owns conditional admission, keeps conflicting clustered proposals in
flight, and accepts no new messages after election until pending entries are applied,
thereby guaranteeing enforcement of the expected-subject header across elections
([`nats-server#8469`](https://github.com/nats-io/nats-server/issues/8469#issuecomment-5304075553)).
The pinned source shows those same readiness and admission boundaries, and the harness
exercises and mutates them. The three layers converge; no one scorer carries the claim.

The harness remains in CI as a dependency canary. Its negative checks and exact markers
matter because a future dependency can regress the contract even though the historical
closure does not depend on the harness proving itself in isolation.

### Rejected upstream apply-time recheck

The abandoned alternative was to let every follower re-evaluate the expected-subject
condition against its physical store during replicated apply. That is unsound: physical
store contents can legitimately differ across replicas under interest retention and
replica-local expiry timing, so peers could disagree about whether to store the same
committed entry and diverge their sequence accounting. A current-configuration gate does
not repair the historical-state or configuration-ordering problem. This is the same
desynchronisation avenue the maintainer identified in rejecting follower decisions.
Leader-only admission is therefore part of the resolution, not a weakness left behind.

### What this evidence does NOT claim

- **Not that the two backstops beneath the proved mechanisms are proved.** An entry
  admitted anyway is discarded by apply-time log order, and beneath that by the store's
  refusal of any sequence that is not the next one. Both are version-common and neither
  is mutation-proved — making an admitted entry survive would take a second mutation to
  the store itself — so they are stated as observed and are not part of the claim.
- **Not that the other streams are closed.** The server mechanisms are
  stream-independent, but each per-stream finding owns a consequence of its own and stays
  open until it is audited on its own terms. **All three have since been audited on those
  terms.** F-032's produced F-062, which is closed too; F-033's produced F-063, F-064,
  F-065 and F-066; F-034's closed on 2026-08-22, producing F-067 and F-068 and routing its
  live-owner residue to F-036.
- **Not that a Will is never published twice.** The sanctioned at-least-once duplicate
  is untouched, and its boundary is unchanged: external publication with no durable
  evidence of completion. What is retired is the class where evidence EXISTED and was
  taken back.
- **Not that an unconditional write is harmless.** It is not, and
  `TestWillUnconditionalWriteRefiresAPublishedWill` states exactly what it would cost —
  which is why it is retained, renamed as a negative dependency test rather than cited
  as evidence about the server.
- **Not that post-start stream configuration is continuously validated.** The closure
  assumes the validated `$MQTT_willq` shape remains conforming. A source added after
  manager initialization can introduce an unconditional foreign write; it remains under
  the configuration-drift residual and is not a route this conditional-write inventory
  claims to refuse.
- **Not that follower-local apply decisions would strengthen the result.** They would
  introduce a second, non-deterministic admission decision and can desynchronise peers;
  the guarantee is deliberately leader-authoritative.

### The bridge-level sentinel

`TestMQTTWillOutboxReplicatedTerminalDoneStands` (ungated, in the cluster job) drives
the whole shape through the production helpers over a real R3 cluster: a licensed
record, a real stream-leader stepdown through the public endpoint, the finalize to
`done`, and then the reclaim from the superseded base — refused, with the terminal
record standing and nothing published. Its paired mutation is EXECUTABLE, not a comment:
`test/depproof/run.sh --bridge` copies the working tree, replaces that reclaim with the
unconditional store the negative tests use, and requires the failure to be the one about
the external act — *"a Will whose record committed `done` was published again"* — so a
build failure or an unrelated assertion is not accepted as the kill.

It is an integration sentinel, not sole proof that the internal window was reached;
`test/depproof/` supplies the package-internal dependency canary, with the pinned source
and maintainer statement completing the closure.

### Keeping the evidence alive across a dependency bump

`make depproof` is wired into a CI job that runs on dependency bumps and on demand
(`.github/workflows/ci.yml`). The broker's own suites cannot replace it: they pass
whether or not the server still refuses the write. Neither can the generic upstream
term-fence unit test, which exercises Raft directly and says nothing about whether
JetStream propagates the term an operation captured.

**F-030's inventory covers the SINGLE-MESSAGE writer only.** It exercises `$MQTT_willq`'s
conditional write, which is one `Propose`. Every accepted CONNECT writes through
`storeAcceptBatch`, an `AllowAtomicPublish` two-message batch, and the inventory's R-batch
row is a written refutation rather than a fixture because `$MQTT_willq` never uses
batches. At the pinned nats-server no Raft-level field preserves "these entries came from
one `ProposeMulti`" — entries are pushed individually, popped as whatever prefix exists,
regrouped by size thresholds, and each group rechecks leadership on its own — so the two
writers are not equivalent and this finding's evidence does not reach the batch one. That
gap is its own `docs/ROADMAP.md` §0.2 item, and it is why the session record's
authored-fork machinery was retained by the write-lineage audit.

F-039 is explicitly outside this finding. It is a local rollback invariant: the inner
`readd` closure on `rollbackSubscribePacket`'s save-failure path restored the packet's
`cons` entries without restoring their `sharedSubs` membership. The main rollback
snapshot restores both maps correctly. No JetStream proposal fence can reconstruct the
missing entry in the `readd` arm.

**Reachability as of this writing, since superseded:** F-039 is now CLOSED — that arm
restores the shared membership beside `cons`. The paragraph above is kept as the analysis
that held when F-078 was filed, because it is what F-078's own reasoning rested on; the
chain it describes is no longer live, and `processSharedJSConsumer`'s comment records why
the branch is nonetheless retained.

## F-031

**Title:** Add write lineage to Will-cancellation watermarks

- Status: **CLOSED** — the watermark it was about no longer exists
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 31

### Original finding record

- [x] `$MQTT_willcancel` carries no write lineage. Its per-client watermark is monotonic
  by design, so a surviving sibling that holds a LOWER `SessionSeq` silently un-suppresses
  a Will an accepted CONNECT had already cancelled — the one failure this stream exists to
  prevent. Worse than the others because the marker is permanent: nothing deletes it, so
  the lowered value stands until the next accepted CONNECT advances it again.

### How it was closed

Not by adding lineage to the watermark — by **deleting the watermark**. `$MQTT_willcancel`
is gone, and with it the single-slot record this finding is about.

An accepted CONNECT now writes one permanent, dated acceptance FACT on a subject nothing
else ever names (`$MQTT.accept.<domainTk><cidHash>.<epoch>.<acceptID>`), committed in ONE
JetStream atomic batch with the session record on `$MQTT_sess`. Two properties retire the
finding outright rather than mitigating it:

- **A fact is never overwritten.** Its subject carries its own `acceptID`, so the set of
  acceptances is monotone and there is no slot for a sibling to regress. The failure this
  finding describes — a surviving sibling holding a LOWER value, silently un-suppressing a
  Will — has no representation left: to lose an acceptance you would have to delete a
  record nothing deletes.
- **The evidence and the acceptance commit together.** The batch removes the interval in
  which a session has been accepted and the evidence of that acceptance does not yet
  exist. That interval, not the missing lineage, was what made every ordering of a
  separate watermark write unsound.

The lineage question does not survive the deletion: a same-base sibling on a create-only
unique subject would carry the identical body, and a fact says only "this acceptance
happened", which two identical facts also say.

`TestWillClaimantSeesAnEarlierAcceptanceAfterALaterOne` pins the property directly — an
EARLIER entitled acceptance still suppresses after a later non-entitled one, which a slot
could not do.

**What this closure does NOT claim.** That no acceptance is ever missed. A fact is
unloseable once COMMITTED; one still uncommitted when a claimant takes its decision is
unreadable, and the persisted decision means it is never read later. That is a separate
residual in `docs/CONFORMANCE.md` with its own interleaving, irreducible while eligibility
is dated by `At` and a due Will must still be decided — not a survival of this finding,
whose subject is a durable value being REGRESSED.

## F-032

**Title:** Add write lineage to inbound QoS 2 holds and release claims

- Status: **Closed 2026-08-17 — no lineage was needed, and the audit produced F-062.**
  The two results are separate and neither implies the other.
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 32

### What the audit established

**Clauses (a) and (b) answer NO: no write here is unconditional, and none takes a base it
did not read.** All four bridge-authored writes set `JSExpectedLastSubjSeq`. Three pass the
literal `0` (create-only, so there is no base to be stale about): the hold store
(`mqttStoreQoS2MsgOnce`, `mqtt.go`), the releaser's claim and the discard's claim (both
`storeQoS2RelClaimCAS`, `qos2_release_claim.go`). The fourth, the terminal commit
(`finalizeQoS2MoveInner`), CASes at the exact sequence its own caller loaded, under a
generation comparison. Deletes target an exact sequence; the one exception is the hold
purge, which names a subject — bounded to a proven cutoff rather than sequence-targeted,
which is F-062.

The inventory is **complete over bridge-authored data mutations with the test seams nil**,
derived from two mechanical sweeps — one over `mqttQoS2IncomingMsgsStreamName` and the
subject prefix, one over the subject builders `mqttQoS2HoldSubject` /
`mqttQoS2ClaimSubjectFor`. It is not "complete by construction": those sweeps enumerate
in-tree references and cannot enumerate a direct NATS publication, which stays scoped out
by `docs/CONFORMANCE.md`'s three-part same-base boundary plus the reserved-namespace deny
and the required ACL.

**Clause (c): `test/depproof/`'s route inventory re-derived for this stream.** R-old,
R-new, R-elect, R-inflight and R-apply are **inherited by citation** — the server
mechanisms F-030 proved are stream-independent, and re-proving them here would be the
error `docs/ARCHITECTURE.md`, "Trusting the backend" now warns against. R-unclustered is
refused by the shared replica check in `validateBrokerOwnedStreamShape`, the same one
`$MQTT_willq` relies on. R-batch: no writer here emits `Nats-Batch-*`, and
`AllowAtomicPublish` is an audited, ACL-conditional waiver. R-forward and R-mirror/source
are refuted in writing, with the post-start configuration-drift residual unchanged.

**R-purge is the row `$MQTT_willq` never needed.** `purgeQoS2Hold` is the only mutation on
any audited `$MQTT_*` stream that names a *subject* rather than a sequence, so no CAS
argument covers it and the server has no condition to enforce — the bridge never supplied
one. Its two caller classes are arbitrated differently, and neither arbitration made it
exchange-bound. That is **F-062** (the teardown caller — closed by giving the purge a
proven cutoff, since no arbitration on a subject-named removal could substitute for one)
and **F-036** (the orphan sweep caller, plus the post-enumeration gap).

**What the audit did NOT find:** any need for write lineage. Three of the four writes are
create-only, so there is no base to descend from, and the fourth is guarded by an immutable
exchange generation that answers "is this record mine?" more strongly than a parent pointer
would. §0.2's own write-lineage audit deleted the create side of `$MQTT_sess`'s lineage for
the same reason.

**Also corrected:** the statements that had made F-062 unrecognisable — comments and
conformance text asserting that exactly one claim create can ever succeed, that a
`discarded` marker can only ever concern its own exchange, that a late purge is "harmless
by naming", that every hold subject is reached by the arbitration, and the headline "no
silent loss on any PUBREL path". Those sentences are why the defect survived review, so
correcting them is part of filing it.

### Original finding record

- [ ] `$MQTT_qos2in` carries no write lineage, on both its hold subjects and their `.rel`
  siblings. A hold claim erased mid-exchange lets a second broker claim the same inbound
  QoS 2 message, which is the duplicate delivery [MQTT-4.3.3] forbids; a `.rel` claim
  erased between PUBREL and fan-out loses the release, stalling the exchange until the
  orphan sweep reaps it. The two subject families have different consequences and need
  naming separately even though one bullet covers the stream.

## F-033

**Title:** Add write lineage to shared QoS 2 assignment claims

- Status: **Closed 2026-08-18 — no lineage was needed, and the audit produced F-063, F-064
  and F-065.** The two results are separate and neither implies the other.
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 33

### What the audit established

**Clauses (a) and (b) answer NO: no write on `$MQTT_qos2assign` is unconditional, and
none takes a base it did not read.** Every claim write is funnelled through
`storeQ2AffClaimCAS` (`qos2_shared_affine.go:696`, `:739`), which *always* emits
`JSExpectedLastSubjSeq`; there is no unconditioned store to find. Three primitives, and
each is conditioned:

| # | mutation | condition sent | base |
|---|---|---|---|
| W1 | claim election, `moveStartedSharedQoS2:1526` | `JSExpectedLastSubjSeq: 0` | create-only: no base exists |
| W2 | every transition, `advanceQ2AffClaim:1306` | `JSExpectedLastSubjSeq: <seq>` | the sequence loaded in the same attempt; a rejection re-reads |
| D1 | claim reap, `reapQ2AffClaim:1377` | `deleteMsg(..., seq, true)` | the exact sequence this call loaded |

The inventory is **complete over bridge-authored data mutations with the test seams
nil**, derived from three mechanical sweeps — over `mqttQoS2AssignStreamName` and
`mqttQoS2AssignStreamSubjectPrefix`, over the subject builder `mqttQ2AffClaimSubject`,
and over the destructive primitives `purgeStream(` / `deleteMsg(` / `deleteConsumer(`.
It is not "complete by construction": those sweeps enumerate in-tree references and
cannot enumerate a direct NATS publication, which stays scoped out by
`docs/CONFORMANCE.md`'s three-part same-base boundary plus the reserved-namespace deny
and the required ACL.

Eviction routes that would erase a claim with no writer at all are refused at startup:
`LimitsPolicy`, `DiscardOld`, `MaxMsgsPer: 1`, `MaxMsgs`/`MaxBytes`/`MaxAge` unlimited,
no sources, no `MaxMsgSize`, no `SubjectDeleteMarkerTTL` (`validateQoS2AssignStream`).

**Clause (c): `test/depproof/`'s route inventory, inherited by citation from F-030.** No
new depproof row — the server mechanisms are stream-independent and re-proving them is
the error `docs/ARCHITECTURE.md`, "Trusting the backend" warns against. R-old, R-new,
R-elect, R-inflight and R-apply are inherited. R-unclustered is refused by the shared
replica check in `validateBrokerOwnedStreamShape`. R-batch: no writer here emits
`Nats-Batch-*`. R-forward and R-mirror/source are refuted in writing, with the
post-start configuration-drift residual unchanged. **R-purge is not applicable, and that
is positive evidence rather than an absence**: `purgeStream` has exactly three
appearances in the tree (`mqtt.go:4540` the primitive, `qos2_release_claim.go:979` on
`$MQTT_qos2in`, `sess_teardown.go:821` on `$MQTT_inflight`), so nothing purges this
stream.

**What the audit did NOT find: any need for write lineage.** W1 is create-only, so there
is no base to descend from. W2 CASes at a base it read in the same attempt, which alone
answers "is this record still the one I reasoned about?" more strongly than a parent
pointer would — a peer that moved the subject wins and this write is rejected. Seven of
the ten transitions additionally assert the winner's identity (`wantWinner`/`wantGen`);
the other three deliberately do not, because the facts they record are owner-independent.
For the two `SourceAcked` writes the per-subject CAS is what makes them safe rather than an
identity check. `AffineDone` is owner-independent too but NOT unfenced: F-065 gave it the
claim's incarnation and the copy the delivery carried, because it states something about one
incarnation's own copy. D1 targets
an exact sequence, and a peer's later transition lands at a new sequence that
`MaxMsgsPer: 1` evicts, so the delete cannot reach it. §0.2's own write-lineage audit
deleted the create side of `$MQTT_sess`'s lineage for the same reason.

### Where the yield actually is: an unconditioned CONSUMER.DELETE, not a purge

As in F-032, the value lies in the one operation that carries no condition:
**`CONSUMER.DELETE` on `$MQTT_out`**, which is interest-retained, so deleting a consumer
destroys every copy it still held. Measured, not assumed: `jsConsumerDeleteRequest`
(nats-server v2.14.5, `server/jetstream_api.go:5116`) rejects any non-empty request body,
so there is no precondition field in the API at all. By the "whose gap is it" rule this
is **ours wherever we act on evidence weaker than the destruction**, and a dependency
question nowhere.

### The five route inventories, dispositioned

**1. Claim transitions (all inheriting W2).** *F-036 course E removed the sweep's
pre-delete discard together with the consumer delete it prepared for, so ONE sweep
transition remains, `discardAbsentWinnerClaim`. The stated TOTAL below is **stale and
deliberately not restated** — see the census-accuracy note at the end of this part.*
They CAS at a sequence loaded in
the same attempt and preserve proven facts through `mergeProven`, so a transition can
only ever ADD to what is known. Six carry an explicit `wantWinner`/`wantGen` identity
assertion and mutate nothing on a mismatch: `qos2_shared_affine.go:1727` (materialize),
`:1809` (the on-behalf discard arm), `:2156` (`discardQ2AffClaimsFor`), `:2237` (`Lost`),
`:2251` (recovery's `Discarded + SourceAcked`) and `orphan_qos2_sweep.go:1871`
(`discardAbsentWinnerClaim`). Three pass no
identity because the fact they record is owner-independent — `SourceAcked` is a statement
about the group delivery and `AffineDone` about a copy the gate has already attributed:
`qos2_shared_affine.go:1367` (the reaper's unofferable-source inference), `:1423`
(`confirmSourceAckAndRecord`) and `:2036` (`mqttFinishQ2AffineDelivery`). Of those three
only the two `SourceAcked` writes rest on the per-subject CAS alone; F-065 fenced
`AffineDone` on the claim's INCARNATION and on the COPY the delivery carried, so an
owner-independent fact about one incarnation's copy cannot be written onto another's
record. None is
unconditional and none takes a stale base. The ONE remaining sweep transition is the one
whose EVIDENCE is weaker than a CAS can express; that is F-064, not a lineage gap.

**2. Claim reaps (all inheriting D1).** *F-036 course E removed the sweep's post-delete
reap, whose whole premise was that the consumer delete had just made the copies absent. The
stated TOTAL below is **stale and deliberately not restated** — see the census-accuracy note
at the end of this part.* Every one re-reads the claim and
re-derives both proofs before deleting, and deletes only the exact sequence it loaded:
`qos2_shared_affine.go:1574`, `:1593`, `:1655`, `:1816`, `:1855`, `:2047`, `:2163`,
`:2215`, `:2258`, `orphan_qos2_sweep.go:1796`, `:1879`. The re-read is what
makes reaping from every proof-writer correct rather than redundant: the two proofs are
produced by unordered paths, so whichever commits second observes the first. All of them
inherit the same internal TOCTOU on the copy-absence reading, which is **F-065** — one
defect in the primitive, not one per site.

**Census-accuracy note, added 2026-09-01 by F-036 course E, and it is a DEFECT IN THIS
CENSUS rather than in the code.** This census's per-site totals and line numbers were
already wrong before course E, so course E does **not** restate them by subtracting from
them. Measured against the current tree: the reap class has **17 direct production call
sites** (five in `orphan_qos2_sweep.go`, eleven in `qos2_shared_affine.go`, and **one in
`mqtt.go` that this enumeration never listed at all**, in the PUBREL/PUBCOMP discharge
path), excluding `reapQ2AffClaim`'s own delegation to `reapQ2AffClaimChecked`. The cited
line numbers are stale too, verified with `git show` against HEAD `4afa533`:
`qos2_shared_affine.go:1727` is a blank line there. Course E's own contribution is the
MECHANICAL fact stated above — one reap site and one transition site removed — and nothing
here should be read as a verified inventory until the class is re-censused. That re-census
is filed in §0.5 of `docs/ROADMAP.md`; a lineage or TOCTOU audit must redo it from the tree
rather than trusting these numbers.

**3. Affine consumer deletes (THREE).** *Count updated by F-036 course E: the sweep's
delete — the one this entry called out as acting on evidence weaker than its consequence,
and F-036's third manifestation — was REMOVED, not hardened. Every remaining one runs
under a session-side fence, so the class no longer contains a member that deletes on
absence alone.* `qos2_shared_affine.go:1137` (provisioning rollback, `minted` only —
nothing durable ever referenced that generation), `:2474` (`finishQ2AffineUnwind`, under
the session's own fence) and `sess_teardown.go:750` (teardown, under a won
`claimTeardown`, and after `discardQ2AffClaimsFor` has marked every assignment of the
generation). What the removal costs is a permanent leak, stated in
`docs/CONFORMANCE.md`; two of the three remaining deletes have no retry anchor if they
fail, which is part of that residual. **This count IS verified against the current tree**,
unlike the two above: `deleteConsumer(mqttOutStreamName, ...)` yields exactly these three
affine sites plus two PUBREL ones (`sess_teardown.go` and
`deleteOrphanPubRelConsumers`).
The two **PUBREL** consumer deletes on the same stream — `orphan_qos2_sweep.go:2202` and
`sess_teardown.go:732` — are **outside this finding's bound**: no assignment claim
references a PUBREL durable or its copies (`CopySubject` and `AffDurable` are Q2AFF-shaped
and `mqttParsePubRelDurable` parses a disjoint namespace), so destroying them destroys
nothing a claim points at. Their consequence is already routed elsewhere by that file's
header; this audit does not adjudicate which finding owns it. Every other
`deleteConsumer` hit targets `mqttStreamName` or another stream and destroys neither
claims nor affine copies.

**4. Copy acknowledgements (six), each destroying exactly one copy.** Five reach
`mqttFinishQ2AffineDelivery`, which records `AffineDone`, confirms the copy's ack and
only then attempts D1 — the inverted window (recorded, not yet acked) is benign and
self-correcting, and the opposite order would make an absent copy indistinguishable from
a lost one. Those five are the delivery gate's `Discarded` disposition
(`qos2_shared_affine.go:1937`), its `AffineDone || Lost` disposition (`:1945`), the
expiry drop (`:1986`), the oversize drop (`:2010`) and the client's own PUBREC
(`mqtt.go:14095`). **The sixth is different in kind and gets its own disposition**: the
owner-mismatch arm sends a bare `sendAck` (`qos2_shared_affine.go:1931`), destroying the
copy with no claim transition, no `AffineDone` and no reap attempt. That is correct
rather than defective, and the reason is ownership, not accounting: the claim names a
different session, so this copy is an artifact — the real owner has, or will have, its
own, and the claim it is attributed to is untouched. A lost ack is equally safe: the copy
is re-offered, the gate re-runs, the claim still names the other owner, and the same arm
disposes of it again. The one thing it must never do is record `AffineDone`, which would
assert a delivery on a claim this session does not own.

**5. Source acknowledgements of the GROUP delivery (nine), scoped to
`moveStartedSharedQoS2`.** The scope boundary is part of the claim; the reply is the group
delivery's ack subject by the input's own definition. Confirmed through
`confirmSourceAckAndRecord`, which acks and only then records `SourceAcked`, at five:
`:1522` (terminal contender), `:1531` (foreign already materialized), `:1565` (own claim),
`:1717` (the on-behalf discard arm) and `:1745` (on-behalf materialized). Fire-and-forget
`sendAck` on the same reply at four: the expiry drop (`:1446`), the oversize drop
(`:1471`) and the two already-`SourceAcked` shortcuts (`:1521`, `:1562`) — each releasing
a delivery whose arbitration is already durably settled, so a lost ack only causes a
redelivery that takes the same arm. Two **pre-dispatch** acks of the same group delivery
sit before the mover and are refuted rather than dispositioned: subscribe-deny
suppression (`mqtt.go:14796-14803`) is unreachable because `checkDenySub` returns false
unconditionally (`shim_engine.go:591`), and reserved-subject suppression
(`mqtt.go:14790-14793`) cannot produce a consequence because
`group durable → effective subject → reserved flag` is invariant across the members of
one durable (`mqtt.go:2390-2394`, `:15958-15959`) — qualified by the pre-existing
negligible-collision assumption of the 16-character truncated hash, which this audit
inherits rather than re-argues. No Local is rejected outright on shared subscriptions
(`mqtt.go:14427-14428`) and the drain routes exclude shared consumers
(`unsub_drain.go:304-305`).

**6. Copy stores — one primitive, three control-flow routes, two precondition
families.** The primitive is `storeQ2AffCopy`
(`qos2_shared_affine.go:1710`), written only by
`materializeQ2AffCopy`. The own-claim family reaches it through `completeQ2AffOwnClaim`
(`:1628`) from two entry points, a fresh election (`:1540`) and a resumed own claim
(`:1560`), admitted on local state alone. The on-behalf family reaches it on
`:1838`, preceded by a consumer lookup that nothing binds to the store. Both counts are
stated so neither reading looks like an omission. These two families were **F-063**, and
what binds them now is the presence check inside `materializeQ2AffCopy` — neither the
admission gate nor the lookup, both of which precede the store and prove nothing about it.

### What the audit found instead: three defects, sorted by falsified premise

Consequences are classified by **the premise each falsifies**, never by where a fix could
be installed — the axis F-036's own dossier uses and the one F-062 was filed on. Eleven
interleavings were worked; they resolve into three premises of ours, one F-036
manifestation, and two dispositions that are not defects at all.

- **F-063** — nothing binds evidence that a copy was RETAINED to the irreversible source
  acknowledgement that follows it. **Fixed 2026-08-19**, with the guarantee re-scoped to a
  fresh but non-atomic observation; see the finding below.
- **F-064** — a live generation reference is treated as reason enough to leave every
  claim of that generation to owner resume.
- **F-065** — `reapQ2AffClaim` deletes on a copy-absence reading taken before its own
  `SourceAcked` transition.

**A fourth consequence sorted onto F-036, and that sort was CORRECTED during review.**
(Read this in the past tense: F-036 course E removed the delete, so the consequence is
DISSOLVED. The reasoning is kept because it is what refused every guarded version.) The
sweep's affine consumer delete was by name, and a session resuming inside its final window
ADOPTS the consumer rather than recreating it — provisioning is a create-or-UPDATE — so
step (5)'s `Created` fingerprint did not discriminate it. The audit first filed this as a
defect of ours, reasoning that the record was genuinely absent when step (4) read it and
so no false-absence inference was involved. **That reasoning does not hold.**
`sess.q2AffGen` is only ever populated from a persisted record (`mqtt.go:6045`, `:8702`,
`:8755`), and provisioning mints a FRESH generation when it is empty
(`qos2_shared_affine.go:1072`), so a session resuming with no record cannot adopt the
condemned generation's consumer at all. Adoption requires a session already resident with
that generation while its record is absent, and the sweeping broker counts only its own
residents — F-036's premise exactly. It is therefore recorded there as a third widening
manifestation. What is genuinely new, and belongs to whoever fixes F-036, is that the
`Created` fingerprint cannot be relied on as a defense.

**Not defects of ours.** A contender's discard arm firing on an *absent* winner record
(`materializeQ2AffOnBehalf`) rests on the same F-036 premise and is recorded there; a
local guard that would mitigate it does not convert it into a separate finding. The
*different-generation* arm is **settled** on the clean-start route and rests on no false
premise: `clear()` drains the affine fence and re-mints the incarnation, and teardown
marks every assignment of the retiring generation `Discarded` before deleting its consumer
(`sess_teardown.go:738-753`), so the old session really has ended and [MQTT-4.8.2-5]
permits the delivery to die with it. What F-036 owns there is the obsolete broker object
continuing to serve after that termination, not the discard.

**Also benign, and stated so rather than filed — and now MOOT.** The sweep's consumer
delete committing while its response was lost skipped that pass's reaps, but the next pass
independently revisited orphan `Discarded` claims and called the reaper. The end state
was a bounded delay in reaping an otherwise-reapable subset, not durable accumulation. F-036
course E removed that delete, so this window no longer exists; what replaced it is not
a delay but the accepted leak in `docs/CONFORMANCE.md`.

### Correcting the statements that made these unrecognisable

Per F-032, correcting the prose that deferred the question is part of filing the answer.
Four in-code markers across three sites said "F-033, unaudited":
`qos2_shared_affine.go` twice inside `materializeQ2AffOnBehalf`'s discard arm (two
markers, one site), `orphan_qos2_sweep.go` in `discardAbsentWinnerClaim`, and the same
file's step (4). Each now states the audit's answer and cites this section rather than
deferring to it. Three further sites that asserted something this audit falsified are
corrected at the same time: `beginQ2AffOp` (admission on local state alone),
`materializeQ2AffCopy` (a successful store is not evidence of retention) and the sweep's
reference skip. The class bounds at `orphan_qos2_sweep.go`'s header and
`docs/ARCHITECTURE.md`'s orphan-reconciliation section keep F-034 and F-036 and drop
F-033.

### Original finding record

- [ ] `$MQTT_qos2assign` carries no write lineage. An affine claim erased by a sibling
  leaves contenders waiting on a `claimed` record whose owner never materialises, which
  stalls the shared-group delivery of that message indefinitely — the same stall the
  generation-scoped naming was introduced to make impossible.

## F-034

**Title:** Add write lineage to outbound in-flight registers

- Status: **Closed 2026-08-22 as an audit plus a census.** The shared cross-term premise
  was already closed (see the note at the top of this file and F-030); what this finding
  owed was its OWN consequence audit against that route inventory, plus a fixture and a
  discriminating mutation for anything still reachable. **No lineage is owed** — and unlike
  F-032's and F-033's audits, this one found a REACHABLE defect. It is **V1**: the drain
  release deleting by a persisted absolute `RegSeq` after an `$MQTT_inflight` recreation,
  the route this file's own F-066 table documents and assigns to "that mechanism's owner".
  The remedy is an identity-conditioned exact-sequence delete plus **subject** addressing
  for the drain plus a retained enumeration watermark for the teardown purge — two
  mechanisms closing two windows, no new persisted field, and explicitly NOT a per-register
  CAS arbitration protocol, because the route is identity confusion rather than contention.
  Provenance classes A (the terminal paths) and D (the orphan sweep) are **refuted as
  already protected** rather than conditioned, because the conditioning could not be
  mutation-proved. The live-owner residue routes to **F-036** under the plan's necessity
  test, with a passing reproducer as the artifact. Replacing the teardown's filter purge
  also retired a stream prerequisite: `validateInflightStream`'s obsolete `DenyPurge`
  rejection is removed and the waiver recorded, since nothing on `$MQTT_inflight` purges
  by subject any more. Produced **F-067** and **F-068**, filed and not fixed.
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 34

### Original finding record

- [ ] `$MQTT_inflight` carries no write lineage. A register erased by a sibling frees a
  packet identifier that is still in flight (so a later delivery can collide with it) or,
  in the other direction, leaves a register whose deletion was proven, reserving an
  identifier and a Receive Maximum slot for the life of the session.

### The census: what it established, and the four defects the oracle found

The cross-stream identity enumeration F-033 deferred here is landed as
[the persisted-identity census](persisted-identity-census.md), enforced by eight tests
rather than asserted — and it does **not** discharge F-066's completeness deferral. The
interpretation-surface guard is bijective against a hand-maintained root set and keys
sites by unqualified function name, so it catches real drift without proving closure, and
the plan's no-fallback rule makes that a reason to claim neither completeness nor the
discharge. Three things about it are worth
keeping here, because each is a conclusion rather than a description.

**Closure is anchored to the interpretation surface, not to a list of transitions.**
Producer closure was reopened three times in review, each time by a transition class
deeper inside JetStream, and each addition was correct. "Enumerate every durable-state
transition" enumerates JetStream's implementation and has no dry-out criterion. Anchoring
instead on *"in scope iff it can change the outcome of an enumerated interpretation
site"* gives the completion test the transition model lacked, and it is finite here: 161
sites, derived by parsing this package with `go/ast`.

**The surface is the SEMANTIC layer, and a mutation is what proved it.** An early version
anchored the surface at the `mqttJSA` methods. A mutation adding a new caller of
`loadInflightRecord` — a package wrapper, not a transport method — **passed**. Widening to
the wrappers that establish an outcome partition of their own added 39 sites, a third
again of the 117 it had then; the manifest now stands at 161, having grown with the code.

**F-066's sample is an ORACLE, applied afterwards, and it found four real defects** in the
first independent enumeration: `mqttQ2AffClaim` missing entirely though two sample rows
name it; `mqttPersistedDrainSeq.RegSeq` dispositioned as not-an-identity because F-034
demotes it to a hint, when demoting what a mechanism *relies on* does not stop the field
being a cross-stream reference; no way to express that `CancelledRegs`' *elements* are
`$MQTT_inflight` identities; and `ConsumerConfig` deferred though three sample rows
resolve to `ConsumerConfig.Durable`.

**Two findings were filed and not fixed**, per the census's disposition-not-repair rule:
[F-067](runtime-correctness.md#f-067) (a pre-release drain record shape accommodated
rather than refused) and [F-068](runtime-correctness.md#f-068) (an enqueue that
revalidates nothing after a local discard).

**A pattern the census surfaced that a sample could not.** The JS domain token appears in
exactly three streams' subject spaces, and those three are exactly the three F-066
records as coupled; every stream that omits it is one F-066 records as uncoupled. Not
asserted as causal — recorded because a pattern across the whole population is what
distinguishes a census from a sample in kind rather than in length.

## F-036

**Title:** Revalidate ownership for otherwise quiet resident sessions

**Supported-scope ruling, 2026-09-02 — HISTORICAL, and it governed the closure that
followed.** As of that date F-036 remained an alpha blocker for its ordinary QoS 2 and
general session-ownership routes; those are what unit 2 closed on 2026-09-03. A closure
need not preserve the experimental session-affine shared-QoS-2 branch, and it did not.
Any affine-only regression caused by the sound general
fix is accumulated under ROADMAP chapter P rather than widening this finding or blocking
its implementation. The affine history below remains evidence about rejected mechanisms,
not an active acceptance requirement.

- Status: **CLOSED 2026-09-03**, in two units — unit 1 on 2026-09-02 (the incarnation
  completion lease, `901ee8f`) and unit 2 on 2026-09-03 (the two-dimensional PUBREL
  classifier and sites 1 and 3). The section "F-036's seventh attempt (2026-09-03)" is
  canonical for unit 2 and states what it closed and what it deliberately did not; the
  residuals are in `docs/CONFORMANCE.md`. The history below is retained unchanged, because
  the refuted designs are what kept six attempts from being repeated a seventh time.
  Previously: Open — NARROWED, not resolved, and **ATTEMPTED BY CONSTRUCTION AND STOPPED
  UNRESOLVED on 2026-08-27** after three plan-review rounds and two scope consults, with no
  code written. The attempt found and verified the site-1 licence — a valid retirement
  watermark whose `MarkerSeq > sess.seq`, plus record absence, plus a full in-fence
  revalidation — and **disproved `RWID` as this finding's predicate**, because the watermark
  is one slot per client so requiring `Gen`/`RWID` equality recreates the
  superseded-watermark false negative the licence exists to close. It also established that
  the watermark records only the SESSION generation, so no retirement proof can license the
  four AFFINE-generation destructive decisions, and that recording `Q2AffGen` is refused
  as a new persisted field. **Site 2 is only PARTLY established, and two review
  rounds each caught the roadmap bullet overstating it.** Settled: the proof's placement,
  the matching-generation refusal, and the unreadable-watermark fail-closed. OPEN: the
  no-watermark arm — DISSOLVED by F-066's landing 2026-09-01, since `$MQTT_teardown`
  recreated alone used to admit EMPTY and lose every open fence, and an account holding
  eight of the nine is now refused before anything is provisioned; and the
  different-generation arm, where proceeding
  on a bare generation mismatch is wrong but the unexplored direction is a two-dimensional
  classifier crossing the generation relation with `MarkerSeq` against a fully revalidated
  `sess.seq` — so it is NOT established that historical per-generation state is required.
  What stopped it: `mqttQ2AffGenerationSettled` returns true for an empty inventory and
  for an all-`Discarded`/`Lost` generation, so the affine consumer delete cannot be
  authorized by suppressing the `Discarded` transitions that precede it, and needs a
  positive authorization of its own or the removal of its destructive arm. The complete
  ten-decision census, the refuted designs and the mechanism-plus-test-infrastructure list
  that is owed are in this finding's `docs/ROADMAP.md` bullet; the working lane is
  `~/.claude/lanes/f-036/`. Slice A landed on 2026-08-11 (the session-record watch plus
  its committed-read backstop, `internal/mqtt/sess_record_watch.go`;
  `docs/ARCHITECTURE.md`, "The session-record watch"): ownership changes only by a record
  write, so the watch observes every one of them, a quiet session still costs no write per
  interval, and every MOVED record now converges — bound sessions included, through the
  eviction worker itself. What remains is the DELETED record on a BOUND session: the
  backstop arms the ownership marker, and the record-rejection settlement that answers it
  is packet-driven for a bound connection, so a Keep Alive 0 client that goes silent still
  holds a session whose record was durably deleted. (Deleted by a teardown, which is what
  the deletion shows; it does not name the broker that ran it, and nothing here needs it
  to — F-084.) That is this finding's own failure shape, so the finding
  stays open until a bound-session disposal exists that does not wait for client
  traffic. Residuals in `docs/CONFORMANCE.md`.

  **A second route into the same shape arrived with F-029a's fix (2026-08-12), and any
  fix here must revalidate against it.** A condemnation marker written onto a bound
  hollow record converges as MOVED, but whoever FINISHES that marker deletes the record —
  and if the survivor's broker dropped the marker observation, it is then facing an
  absent record under a bound, silent Keep Alive 0 client: this finding's own shape. Two
  variants, deliberately different: a CONNECT that adopts the marker finishes it
  IMMEDIATELY and cannot be deferred, and the orphan sweep defers its finish by a pass
  (which narrows the case it owns to a survivor whose serial committed-read backstop does
  not reach the record within the widened window — the unbounded-walk residual). Neither
  is claimed to be closed by the deferral; both are listed here so this finding's
  eventual fix tests them alongside the pre-existing route.

  **SECOND ATTEMPT, 2026-09-01, STOPPED UNRESOLVED** after five consults and four
  plan-review rounds, with **no implementation code written**. F-066's enforcement had
  landed (`1205d32`), dissolving OPEN 1, so the ordered pre-plan investigation ran. This
  section is canonical for what it established; `docs/ROADMAP.md` carries only the gating
  summary. Working lane `~/.claude/lanes/f-036-preplan/`.

### F-036's second attempt (2026-09-01): what it established

**A third manifestation, and it is the most severe thing in this finding: SINGLE-BROKER,
NON-HA-DEPENDENT SILENT LOSS of an acknowledged QoS 2 message.** No cluster, no crash, no
peer — an ordinary clean-start reconnect. `clear()` deliberately leaves the displaced client
BOUND (`sess.c` untouched, F-089) while re-minting the reused session object, and
`mqttStoreQoS2MsgOnce` derives its hold subject from whatever `sess.gen` currently is, under
no gate. Between the re-mint and the replacement's bind the displaced connection can store
payload A onto the **re-minted** incarnation's subject for identifier P; the replacement then
binds, publishes B on P, its create-only CAS meets A, and the `mqttQoS2Held` arm returns
"genuine DUP PUBLISH; PUBREC". **B is never stored, the client is told PUBREC, and the PUBREL
releases A.** Two sibling routes share the cause: `mqttProcessPubRel` has no `sess.c`
reference in its whole body while `mqttProcessPubRec` and `mqttProcessPubComp` both refuse
on `sess.c != c`; and a stale PUBREC continues past the rebind and stores an uncorrelated
PUBREL onto the fresh incarnation's `sess.pubRelSubject`. All three are **this finding's own
causal defect reached through this broker's own reused session object** rather than through
a peer, which is why they take no new finding ID and no second checkbox.

**A false claim in the tree, owed a correction.** `reopenQoS2ReleaseFence` justifies
reopening the release fence inside `clear()` as "Safe because the incarnation has been
re-minted by then, so a newly admitted release works on the fresh generation's subjects,
which this teardown cannot name." False in both directions: a PUBREL derives its hold subject
BEFORE admission, so an admitted release can name the RETIRED generation's subjects, and the
FRESH generation's subjects are exactly what a displaced connection can reach. Filed as
family 6 of §0.5's comment-corpus sweep.

**Q2 was SETTLED as course E, and it has LANDED.** `deleteOrphanQ2AffConsumers` is removed
OUTRIGHT rather than only its step-6 consumer
delete: with the delete forbidden its step-2 `Discarded` writes became an unlicensed
destructive decision rather than preparation, both record revalidations and the
`ConsumerInfo.Created` fingerprint guarded only that delete, `mqttQ2AffGenerationSettled`
lost
its only caller, and step 7's reaps lost their premise — `reconcileOrphanQ2AffClaims` already
retries an existing `Discarded` claim, so the general retry is not lost. Removed with it,
as
dead once the function was gone: `mqttQuarantineConsumerKey`, `mqttParseQ2AffDurable` and
`mqttSessRefs.affDurables`, whose only reader was the deleted candidate check. Every positive
authorization was refused on its own terms: a retirement proof is structurally unavailable,
because the watermark names the SESSION generation while the consumer is keyed by an affine
generation that OUTLIVES incarnations; consumer metadata has the same mismatch; removing only
the absent-record route still lets a stale owner create a fresh nonterminal claim after the
verification scan, destroying a live copy and leaving F-064's claim-without-copy state; and
`NumPending == 0` is an instantaneous observation, not a fence. **The accepted price is a
PERMANENT UNBOUNDED LEAK, and implementation MEASURED it larger than this paragraph first
recorded: it is a CORRELATED TRIPLE, not a consumer alone.** The affine consumer's
`InactiveThreshold` is deliberately zero
and the stream refuses any `ConsumerLimits.InactiveThreshold`, so a consumer the sweep
declines to delete has **no automatic orphan-collection backstop** left. Its
interest-retained COPIES stay with it, and so do the CLAIMS those copies belong to:
`reapable` requires the copy to be authoritatively absent as well as `SourceAcked` — "a copy
that still exists keeps its claim, whatever else is proven" — and step 7 worked only because
step 6 had just made the copies absent. The surviving `discardAbsentWinnerClaim` keeps
CREATING that third population, so it is not merely a pre-change shape.
That is still narrower than "never collected": `deleteTeardownTargets` still
deletes `t.q2AffDur`, and the rollback and unwind paths also delete affine consumers, so a
consumer whose session is later torn down or re-entered can still reach lifecycle
cleanup —
and its claims then become reapable once `SourceAcked` is also proven, since a consumer
delete removes only the copy-side blocker. **A resuming session's adoption is not a
deletion route** — teardown adoption is a different thing, see the manifestation below —
but it is not inert either: a completed delivery after it, or
`settleRetiredAffineRegister`, can retire the copy and the claim. What leaks permanently,
and without a cardinality bound, is the
triple with **no later authorized teardown, adoption or register settlement** — plus one
class implementation added: every authorized cleanup route that FAILS after dropping its
reference, with no retry anchor (`dispose`'s failure arm, `finishQ2AffineUnwind`'s failed
delete, provisioning rollback's failed minted delete). All of that is preferred to an
unclosed acknowledged-message destruction path. **"Independently terminal" is
`Discarded ∨ Lost ∨ AffineDone`, which ALREADY EXISTS as `mqttQ2AffClaim.terminal()`**, so
the definition this finding owed is discharged by pointing at it; an empty owned inventory
authorizes NOTHING and must never authorize by vacuous truth. **On landing** the
destructive-decision census dropped from ten to eight; F-033's route census lost **one claim
transition and one reap** — its own totals are stale and are NOT restated here, see that
census's accuracy note and §0.5's re-census bullet — while its **affine consumer delete count
of three IS verified** against the tree; and F-033's "adoption blind
spot" manifestation below is **dissolved rather than narrowed**, because it existed only
through a by-name delete of a consumer a resuming session adopts. The
`docs/CONFORMANCE.md` leak residual landed with it, as did the correction of every statement
naming the orphan sweep as a later affine-consumer cleanup route — found by a mechanical
closure rather than a list, after three review rounds each named one more site a hand list
had missed.

**Q1: OPEN 2's classifier is PROVED CONDITIONALLY.** The sequence half survived three
adversarial rounds with no counterexample: refuse iff the watermark is valid and
(`Gen == sessGen` ∨ `MarkerSeq > seq`); absent proceeds, now that F-066 refuses lone
recreation of `$MQTT_teardown`; corrupt, indeterminate and `seqUncertain` fail the release
closed. **So the necessity claim this finding had withdrawn is answered outright: historical
per-generation watermark state is NOT required.** `MarkerSeq == seq` is unreachable for a
bound session — every `sess.seq` adoption takes either a store response for this broker's
own bytes or a digest-matched readback, and a marked record is never restored into a bound
session. **What is NOT proved is the identity prerequisite**, named progressively across three
rounds: the wrong generation term, then that generation equality is the wrong KIND of term,
then that what is missing is the caller's authority to act for the currently bound
incarnation. **A correction that cost a round and must not be repeated: `rec.gen` is a
per-PUBLISH EXCHANGE identity, minted by `nuid.Next()` and carried in `Nmqtt-Q2Gen` — NOT
the session incarnation generation.** The two are unrelated; the classifier needs the
subject-derived session generation while the claim, fence 2 and the terminal commit keep the
exchange one, and substituting either for the other makes nearly every legitimate record read
as superseded.

**What the four plan revisions established, so a third attempt does not re-derive it.** A
per-connection incarnation stamp compared under the session lock is the right mechanism, and
**its synchronization discipline was VERIFIED**: production has exactly two non-nil
assignments to `mqttSession.c`, both already under that session's mutex, so adjoining the
stamp write supplies the happens-before; the re-drive detach gives detach-before-next-bind
program order; and no production path binds one client to two session objects, sets `sess.c`
non-nil outside those two sites, or restamps the displaced client. A stamp closes the
clear-to-bind gap by VALUE DISCRIMINATION where a fence cannot by mutual exclusion — in that
gap `sess.c` is the displaced client and `sess.gen` is fresh, and nothing holds the fresh
generation as its bound generation until the replacement bind writes it. **`clear()` must
never restamp the displaced client; that mismatch IS the mechanism.**

**Three designs REFUTED, so they are not tried again.** *A write-once stamp* — it misses the
**new-session** bind site, a client rebinds across CONNECT re-drive, and two writes under
two different session objects' mutexes are a data race. *A generalised operation fence replacing
the release and affine fences* — `clear()` REOPENS on return, before the replacement binds,
so admission passes in the gap and the race merely moves; the merge additionally drops the
affine predicate's readiness, `q2AffGen` and consumer conditions, and self-deadlocks because
`prepareQ2AffineUnwind` closes the very fence an admitted holder would be waiting on. *A
mechanically enumerable "incarnation-derived state" set defined by `setIncarnationLocked`
plus `clear()`'s reset block* — **not closed**: `clear()` advances `recordEpoch` INDIRECTLY
through a deferred `endRecordTxNewGeneration`, outside both functions, and "anything derived
at use time" is semantic taint that no source test can decide.

**One exclusion is VALID and may be relied on:** an exact-sequence reap cannot reach a
replacement record, including after a same-generation takeover, because the loaded record's
stream sequence is immutable and a later occupant receives another. **One hypothesis is
CLEARED:** there is no separate account-manager incarnation key and no hidden consumer-name
root — every durable naming root traced reduces to `gen`, `q2AffGen`, the PUBREL fields, or
state captured from pending/drain entries, so the closure failure is lifecycle and transitive
state, not a third naming scheme.

**What stopped it, and it is the same shape as 2026-08-27.** Point-linearized admission needs
every incarnation-sensitive effect placed, and plan-review rounds 1 to 4 each found another
that was missed — PUBREC's tail through `retireSourceLocked`, `mqttFinishQ2AffineDelivery`
and `markInflightRegisterRetired`'s re-read of `sess.gen`; PUBCOMP's two effects separated
by JetStream I/O, which one check cannot govern and `sess.mu` cannot span; PUBREL's release,
tombstone and reap arms; then both PUBREC error rollbacks, `secureRetiredSource`'s write after
its own I/O, `noteQ2AffCarrierRefused`'s write inside `dischargeQ2AffPubRelProof`, and PUBACK's
`finalizeDrainSource` tail. The attempt to answer that class with a property instead of a
fifth enumeration is the third refuted design above.

**Constraints a third attempt must carry, all confirmed by the final review.** The
**five-route boundary** stands — QoS 2 PUBLISH, PUBREL, PUBREC, PUBCOMP and PUBACK through
the shared handler — and the dispatcher-wide census an earlier revision demanded was
WITHDRAWN, because it existed only to prove a dispatcher-wide fence property that no longer
exists. Admission is **point-linearized, not operation-duration exclusion**; the claim that
a connection "remains bound throughout unlocked I/O" was withdrawn and must not return. The
stamp discipline is not merely the equality: it is the write at **both** bind sites in the
same critical section as the binding, **invalidation at the re-drive detach**, reads **only
after `sess.c == c` is proved** under that same mutex, and an **explicit
invalid-generation refusal** rather than bare comparison. **Success-shaped protocol
responses are effects the next property must cover** — they have neither an
incarnation-derived target nor a session-state write, so a predicate phrased only over
those two misses them, which is why the last revision's placement table could not be
generated from its own stated rule. And the mutation suite needs more than barrier
placement: a re-read mutation must change the state actually asserted rather than a
different subject; PUBCOMP's two helpers need a second barrier between them; the QoS 0/1
and PUBACK boundary tests need values an admission check would actually change; the
exact-sequence exclusion needs a paired code mutation; the enforcement test must cover
writers and not only readers; and the race proof needs a deterministic unordered-access
schedule rather than an asserted one.

**What is OWED before a third attempt**, and it is another property design plus another route
audit rather than a prose correction: a real lifecycle boundary covering `clear()`'s INDIRECT
effects, interprocedural enforcement for reads AND writes, helper-internal placements, and
a mutation suite whose barriers sit before the effects they discriminate. Of the
first attempt's mechanism-plus-test-infrastructure list, **item (a) is DISCHARGED** — course
E removes the destructive arm outright and `mqttQ2AffClaim.terminal()` supplies the
"independently terminal" definition, and every positive authorization for the step-6 delete
was sought and refused, so a third attempt must not re-run that question. **Only (b) through
(e) remain owed**, unchanged.

### F-036's third attempt (2026-09-01): the property pass, and why it stopped

**STOPPED UNRESOLVED for the third time, with no implementation code**, after one
property-and-route pass and one adversarial challenge, under a stopping rule
pre-registered before the pass began. Working lane
`~/.claude/lanes/f-036-property-audit/`, which holds the pass (`pass.md`), the SSA
instrument that generated its matrix (`main.go`, `go.mod`) and its four outputs, the
decisions and the ledger. The pass produced the deliverable the second attempt said was
OWED — a lifecycle property and an interprocedural effect matrix instead of a fifth
enumeration — and the challenge refuted enough of its measurement and its dispositions
that a plan may not be written on it. This section is canonical for what it established;
`docs/ROADMAP.md` carries only the gate.

**The instrument, because a fourth attempt should not rebuild it wrong.** An SSA
analysis (`x/tools` `go/ssa`) roots the five QoS 2 packet routes — as **six** SSA
functions, because PUBACK and PUBREC are rooted both at their wrappers and at the shared
handler they call, so `ROOTS 6` in the outputs is not five — computes reachability
at two boundaries — static callees plus `MakeClosure` targets plus same-named interface
methods, and an "indirection-sound" closure admitting every signature-matching package
function at a dynamic call — and classifies effects per instruction. **Seven** defects in
it were found by the challenge and every one is CONFIRMED against the tree, so a rebuilt
instrument must not repeat them:

1. **A builtin `delete` is not an `*ssa.MapUpdate`.** It falls through the `Call` case
   and is invisible, so `delete(sess.pendingPubRel, pi)` in the stale-PUBREC rollback
   (`mqtt.go:16767`) never appeared in the matrix. Map deletion is an effect operation
   the classifier omitted entirely.
2. **A response predicate keyed on the callee's NAME counts refusals as successes.**
   `mqttEnqueuePubResponseReason` carries the reason as an argument, so
   `mqttReasonNotAuthorized` (`mqtt.go:15485`), `mqttReasonQuotaExceeded` and
   `mqttReasonPayloadFormatInvalid` were all classified as success-shaped. The reason
   argument has to be read.
3. **Pointer taint "at any depth" escapes the session object.** Following `sess.asm` and
   `sess.c` reaches account-manager and client state that is not session-incarnation
   state at all — `mqttAccountSessionManager.refusedAcceptFacts`
   (`sess_accept_fact.go:378`) and `.forkedGrants` are in the matrix and do not belong
   there. That is the semantic taint the stopping rule forbids, arrived at from the
   opposite direction: the extension that fixed a real under-count (below)
   over-corrected into one.
4. **The signature-matched dynamic closure collapses precision**: 1219 of 1376 package
   functions, pulling in `mqttProcessConnect`, `mqttProcessUnsubs`,
   `createOrRestoreSessionWithin` and connection teardown, none of which a QoS 2 packet
   route calls. A superset that admits 89% of the package is not a reach boundary.
5. **A "functions with effects" count must be per class.** The pass reported 72 and 148;
   only 46 static and 105 sound-mode functions carry an in-memory-write row.
6. **Both propagation loops are FIXED-ITERATION, not fixed-point**, though the pass
   describes them as a fixed point: the interprocedural loop caps at 12 rounds and the
   local one at 4, with no convergence assertion anywhere. So every count is a LOWER bound
   on the instrument's own predicate at the same time as it is an over-approximation of the
   real one — two errors in opposite directions inside one number. A rebuilt instrument
   iterates to convergence and asserts it, never caps.
7. **Closure free-variable propagation is described in a comment and never implemented.**
   A `FuncLit` reached through a tainted enclosing frame is not tainted, so the capture side
   of the very indirection the "indirection-sound" boundary exists to cover is not covered
   at all — only the call side is.

**The headline is RETRACTED.** The pass claimed the measurement proved that
point-linearized per-effect placement has no closed set: 150 in-memory write sites
across 72 functions statically, 374 across 148 soundly. The arithmetic is consistent
with the analyzer, but at least **67 of the 150 static sites are not stale-route
placement targets at all** — 40 are `clear()`'s own lifecycle transition, 13 hydrate the
transient expiry object, 8 initialize an unreachable constructor object, 4 are
`setIncarnationLocked` itself and 2 hydrate the transient adopted-teardown object —
leaving at most 83 before excluding already-admitted critical sections. **A large
over-approximated set is not a proof of non-closure**, and no future attempt may cite
these numbers as one. What the measurement DOES support is narrower and still useful:
hand-placing effect checks over this call graph is unsafe, which is consistent with
rounds 1-4 each finding one more.

**One under-count the pass found in itself, worth keeping.** Its first classifier
reported ZERO effects for `secureRetiredSource`, which the second attempt names as
carrying "a write after its own I/O". The write is one hop off the session —
`cur.durable`/`cur.regSeq` through `sess.srcAckOwed[key]`
(`inflight_register.go:2015,2017`) — and a predicate stated over direct `*mqttSession`
fields cannot see it. **A session-state predicate that does not follow at least one hop
through the session's own maps under-counts; one that follows every hop becomes taint.**
Both errors were made here, in that order.

**What the pass ESTABLISHED and a fourth attempt must not re-derive: the property P.**

> A session object's incarnation generation changes only while no client is bound to it.
> `setIncarnationLocked` may be called only with `sess.mu` held and `sess.c == nil`, or on
> an object not yet reachable by another goroutine.

P is **sound**, and the challenge confirmed it as such. It is stated over the object's
lifecycle TRANSITIONS rather than over `setIncarnationLocked` and `clear()`'s reset
block, which is what the second attempt's refuted design 3 got wrong — a
transition-level statement covers `clear()`'s INDIRECT effects, the deferred
`endRecordTxNewGeneration` that advances `recordEpoch` among them, without naming them.
`setIncarnationLocked` has exactly **two** production callers: `mqttSessionCreate`
(`mqtt.go:10089`), on an object not yet reachable, and `clear()` (`mqtt.go:10823`). Of
`clear()`'s six callers only the **CONNECT clean-start arm (`mqtt.go:12852`) violates
P**, and it says so itself — "clear() has already reset this object to sequence 0
**without detaching its client**" (`mqtt.go:12855-12856`). The other five are unbound or
transient: `mqtt.go:4037` detaches at `4013`; the expiry timer at `6662` refuses on
`rebound` (`6587`); `expireSessionFromRecord` (`6834`) and `restoreSession`'s
overdue/retired arm (`9920`) both build transient objects; and
`sess_uncertain.go:1650`'s caller returns on `sess.c != nil` (`sess_uncertain.go:935`).
That last one **satisfies P and is NOT a check-then-act race**, even though its sample and
its `clear()` are not under one mutex: `resolveUncertainNow` takes the account-manager
ownership gate and defers its release across both (`sess_uncertain.go:864,867`), and
CONNECT tests that same `sessLocked` gate before it binds (`mqtt.go:12591`), so no
reconnect can move `sess.c` from nil to bound in between. The code says as much itself —
the gate is taken "precisely so that the resolution and this discharge cannot be split by
a reconnect".

**P retires the per-connection incarnation stamp FOR THE ENTRY GAP, and only for it.**
The stamp's premise was that inside the clear-to-bind window `sess.c` IS the displaced
client, so pointer identity cannot discriminate and value discrimination must. Under P
`sess.c` is nil there, so `sess.c == c` discriminates again. That does not retire a
DURATION mechanism, and the pass's claim that it discharged the whole matrix was
overstated in the pass itself and cut before the challenge ran.

**Enforcement, with its reach bounded — and two limits the challenge added.** A runtime
assertion inside `setIncarnationLocked` is total over the mechanism because the field is
written nowhere else, and a package-wide source guard bounds that premise to "no other
assignment exists in this package's non-test source". But the assertion **cannot verify
that `sess.mu` is held**, cannot distinguish a not-yet-reachable object, and fires
**after much of `clear()`'s in-memory reset has already run** — so it detects a violated
invariant rather than refusing at the transition boundary. The source guard bounds
direct assignment, not synchronization.

**The audited five-route ownership table**, by function extent at `cb26fa6`:

| Route | Extent in `mqtt.go` | Ownership refusal |
|---|---|---|
| PUBLISH `(*Server).mqttProcessPub` | 15467-15658 | **NONE** — zero `sess.c` references |
| PUBREL `(*Server).mqttProcessPubRel` | 16002-16308 | **NONE** — zero `sess.c` references |
| PUBACK+PUBREC `(*client).mqttProcessPublishReceived` | 16596-16961 | `sess.c != c` at 16611, near entry, under `sess.mu` |
| PUBCOMP `(*client).mqttProcessPubComp` | 16972-17077 | `sess.c != c` at 16994 and 17054 |

Two of the five carry no ownership term at all, and they are the two the second attempt
named. P is what would make such a term MEAN anything, because without it the displaced
client satisfies `sess.c == c` inside the gap.

**The three dispositions, and why the matrix is not closed.**

*D1 — durable-name effects.* Under P plus snapshot-at-admission — names derived from
values read inside the admitted critical section and never re-read — a displaced
continuation's names can only be the RETIRED generation's, which is the bounded class
F-062 (the `$MQTT_qos2in` purge bound) and F-034 (the `$MQTT_inflight` enumeration
watermark) already accept. Sound as far as it goes, but it is a CHANGE and the pass
named only one of its sites. There are at least two: `mqttQoS2InternalSubject`
(`mqtt.go:15971`) re-reads `sess.gen` in its own critical section, and the shared
handler re-reads `sess.pubRelSubject` after unlocking (`mqtt.go:16747`).

*D2 — in-memory writes into the fresh object.* **REFUSED, and the pass's candidate
disposition is REFUTED by the tree.** "Harmless because `clear()` nilled the maps" is
false: `retireSourceLocked` (`mqtt.go:11496`) lazily RECREATES `sess.srcAckOwed` and
inserts the retired source, so a stale PUBREC continuation writes stale incarnation
state into the FRESH object rather than becoming a no-op; `noteQ2AffCarrierRefused`
(`mqtt.go:17094`) recreates its cleared map the same way; and `secureRetiredSource` can
then update that entry after further I/O. The pass's other candidate — re-check at every
`sess.mu` re-acquisition — is not a closed boundary either: the 78/254 lock census
counts unrelated lifecycle and `subsMu` acquisitions and locks on other or transient
sessions, does not identify which acquisitions are continuations after admission, misses
helper writes under a caller-held lock, and cannot cover effects that take no session
lock at all. Threading client and token context through it, with exceptions, is the
hand-maintained site list the stopping rule forbids.

*D3 — success-shaped protocol responses: NOT DISPOSITIONED.* This is the pass's own
worst failure. It measured the class deliberately, BECAUSE the second attempt's final
review said a predicate phrased over durable targets and session writes misses it — and
then split its dispositions over the other two classes and left this one out. A
pre-detach PUBLISH can complete its store and send a success PUBREC (`mqtt.go:15651`),
and PUBREL's deferred response (`mqtt.go:16028`) has no ownership re-check; P governs
neither.

**THE MISSING DISPOSITION, and it is where a fourth attempt starts: operation-duration
quiescence at the object-reuse transition.** No re-mint until the displaced route's and
read loop's lifetime has DRAINED, with admission held closed through the transition.
That governs D1, D2 and D3 at ONE lifecycle boundary instead of site by site, and it
is neither the point-linearized admission that stopped attempts 1 and 2 nor the
generalised operation fence refuted as design 2 — that fence reopened on `clear()`'s
return, before the replacement bound, which is exactly the gap a drain closes. It was
not proposed, established or audited by this pass and is not claimed here to work.

**Route A and Route B, the two ways to discharge the one P-violating site.**

*Route A — detach the displaced client before the re-mint.* It does NOT inherently
violate F-089 or F-091, and that question is now answered: detaching makes
`pubCompOwed`'s owner and `retainedGen` inert, which is their documented semantics —
packet identifiers belong to separate connections, and F-091 says outright that once the
connection is no longer the session's "the orphaned frame is one PUBLISH under one
identifier on one socket and needs no reservation at all". The owner-independent write
token survives the sweep. **But Route A is UNSAFE WITHOUT QUIESCENCE**: it moves
`sweepDeadSendHoldsLocked`'s deletion earlier while an already-admitted old handler may
still respond, allocate or write, and the takeover's closure is asynchronous — `go
ec.closeConnection(DuplicateClientID)` (`sess_takeover_redrive.go:242`) — so a detach
supplies no drain of its own.

*Route B — abandon the object and bind a fresh one.* **Its claimed precedents do not
establish it.** `abandonReDrivenSession` (`sess_takeover_redrive.go:267`) runs after a
SUPERSEDED `clear()` and then restores authoritatively; it is not an example of
abandoning live clean-start state without teardown. `restoreSession`'s overdue arm
(construct at `mqtt.go:9899`, `clear()` at `9920`, return at `9934`) constructs a
transient object, clears it and returns THAT SAME reset object — it does not abandon a
resident object and bind a distinct fresh one. A distinct fresh object would stop stale
continuations corrupting the new object's memory, but stale old-object durable writes
and success responses would remain, so it still needs quiescence or an equally closed
permanent-retirement mechanism.

**What (b) through (e) still owe is unchanged**, and the pass adds one mutation to them
that a fourth attempt should keep because it needs no cluster, no crash and no peer:
**M-P** — delete whatever precondition holds the re-mint back and confirm the paired
test fails, where the test is the single-broker silent QoS 2 loss (a displaced
connection stores payload A on the re-minted incarnation's hold subject for identifier
P; the replacement binds, publishes B on P, and B is lost behind a success-shaped
PUBREC). That is the cheapest release-blocking oracle this finding has.

### F-036's seventh attempt (2026-09-03): unit 2, and the box CLOSES

**Unit 2 is implemented, and with unit 1 it closes this finding.** Working lane
`~/.claude/lanes/f-036-unit2/` (`plan.md` revision 5, `review-ledger.md`, `DECISIONS.md`,
`mutations.md`). Six plan-review rounds — blocking findings 8, 4, *no output*, 3, 0, then
CLEAN — and ONE scope consult, which decided both open design questions in a single pass.
**The `advisor` tool was AVAILABLE for that consult**, the first time in any attempt at
this finding; the sixth attempt's three consults and the fifth's were all Codex-only, and
that is recorded rather than passed over. Round 3 produced no review at all: its launch
succeeded and the job appeared in `running[]`, but every by-id lookup returned "No job
found" while the same call listed it as running, and it then vanished from the tracker —
so it was relaunched as round 4 rather than counted.

**The mechanisms are in `docs/ARCHITECTURE.md`** — "A deleted record is retired under a
licence, not under its absence" for site 1, the two new reader entries under "The
generation-retirement watermark" for sites 2 and 3, the `R → P → W → S` ordering under
"The release claim", and the proof gate plus safe tail under "Orphan reconciliation". What
belongs here is why each is that shape, and what the rounds killed.

**The consult's two rulings, and one of them answered my own objection.** Site 1 takes
route (i) — a licence-backed bound arm on the periodic pass — and NOT a GONE arm on
`evictDisplacedOnRemotePersist`, because that worker is driven by a MOVED-record SEQUENCE
observation and feeding it an absence would fabricate an observation level and corrupt its
coalescing accounting. Site 3 takes a narrow B3: the proof gate, plus a bounded
positive-only memo, plus a safe non-destructive tail — the last two being narrowings the
advisor had not proposed. I pressed for a bounded variant of the leak, on the grounds that
the single-slot watermark makes the unprovable population PERMANENT and `$MQTT_qos2in`
holds full payloads. **Codex refuted it with a reason rather than a preference:** a dead
generation with an absent record and a live remote generation with an absent record are
observationally IDENTICAL to that pass, so waiting longer distinguishes nothing, and an
exact bound would need permanent per-generation retirement history — trading unbounded
payload retention for unbounded metadata — or a distributed liveness lease. It also
refuted a candidate authorization I had not considered: an existing `discarded` claim
beside a surviving payload is NOT sufficient, because this sweep can itself create that
shape from false absence and the record encodes no provenance.

**Six things the rounds refuted or corrected, so a later change does not reintroduce
them.**

*(a) The site-1 ordering was DRIFT and impossible as written.* The plan had the licence
revalidated after `retireBoundIncarnation` returned — but that helper sets `sess.c = nil`
and mints the permit before returning, so a post-return owner check can never pass. The
proof runs in a PRE-DETACH callback instead, pausing after `prepareClear` has opened the
record transaction. *(b) The packet lease was OVERSTATED.* It pins the incarnation and
therefore the generation; it does NOT pin `sess.seq`, which other admitted owners and
local record writers still advance — so the classifier's sample takes the record
transaction, and the claim is two-part rather than one. *(c) The site-3 key claim was
FALSE.* `deleteOrphanPubRelConsumers` is keyed by `refs.pubRelDurables` and its own record
re-read, not by `refs.hasInbound`; the plan asserted all three arms shared one predicate,
was corrected in round 1 and STILL carried the contradictory sentence downstream, which
round 2 caught. *(d) The crash audit conflated process death with an abnormal Go exit* —
a `defer` does not run when the process dies — and a recovery claim was simply false: a
restarted broker does NOT "re-observe GONE and re-arm", because with the record already
absent there is no resident object left for the pass to look at. *(e) The safe tail's
justification was too broad.* It is not that a paused releaser "can only win a new claim":
one may already hold the claim and already have passed its own proof, and deleting the
claim does not revoke its in-memory progress. The defensible claim is only that with the
hold authoritatively absent, deleting the unchanged claim cannot destroy a payload and
cannot let a NEW releaser verify the old hold as Held. *(f) `licensed=false` needed
PROVING, not asserting*, because `disposeCaptured` prefers a capsule action and would then
leave a live connection's Will for the close path to publish again; parking consumes the
Will and nils `sess.c`, so the pre-detach owner check refuses a parked session outright,
and a successful retirement here captures `action == nil`.

**The structural guard was DELETED on its third review, and that is the transferable
lesson.** A guard over every orphan-sweep destructive operation was designed in revision 2
and narrowed in revision 3 to a primitive inventory plus per-helper call-site inventories.
Its narrow claim was honest; what did not follow was the conclusion the plan drew from it,
that "a fifth arm cannot be added silently" — an already-inventoried destructive helper
can acquire a NEW UNSAFE CALLER without adding any primitive or touching a monitored call
site, and closing that needs the bounded SSA reachability this repository has already
ruled disproportionate. Three rounds each killing one variant of one mechanism is the
signal to delete the mechanism rather than defend it again. The property now rests on the
recorded census, the behavioral negatives with exact payload assertions, and their
positive controls.

**A convergence gap that no review found, and the reason it is recorded here.** It came
from READING the existing `TestMQTTRejectedRecordPassLeavesBoundSessionsAlone` rather than
from a round: once the ownership marker is armed, `reconcileResidentRecordState` diverts
to the unbound settlement and never reaches the GONE arm again — so the site-1 route would
have fired exactly once and its FIRST refusal would have been final, silently. A bound
armed session now goes to the ownership backstop instead, and the unbound one still goes
to the settlement whose own test pins it at zero probes.

**31 mutations killed, ELEVEN initially SURVIVED**, and every survivor produced a new
discriminating test rather than a weakened assertion: the between-read seam (owed item (d)
— the pre-existing seam fired after both durable reads, so nothing could make the record
reappear before them), the deliberate split-index state (both manager indexes move
together normally, exactly as round 2 predicted), a `runtime.Goexit` inside the proof, the
Will-PUBLICATION observation (discard and consume leave the same terminal disposition, so
only the publication distinguishes them), a PUBREL on an unresolved session, and a
tombstone-PRESENT fixture (one starting from an absent hold cannot prove the tombstone
order at all). **One mutation is reported as NOT DISCRIMINATING rather than forced:**
making the terminal tombstone delete unconfirmed SURVIVES, because the mandatory
authoritative absence read that follows subsumes the confirmation — so the plan's own claim
that an asynchronous delete is insufficient proof was corrected, the confirmed form kept as
defence in depth, and no seam invented to manufacture a kill. Revision 1's `>` → `>=`
mutation was struck as invalid during plan review, `mseq == sess.seq` being unreachable for
a bound session.

**What unit 2 does NOT close**, both permanent and both in `docs/CONFORMANCE.md`: the
no-licence half of site 1, where a record GONE with an absent, corrupt, indeterminate or
insufficient watermark leaves the marker armed and a bound Keep Alive 0 client may strand
until it sends traffic; and the orphan-reconciliation retention leak, which is unbounded
because the single-slot watermark makes an unprovable generation permanent.
`discardAbsentWinnerClaim` stays ungated under the supported-scope ruling, and the
pre-existing post-proof paused-releaser residual is unchanged.

### F-036's sixth attempt (2026-09-02): unit 1 implemented

**UNIT 1 IS IMPLEMENTED**, after five attempts that produced no code. What changed was
not the analysis but the authority: the user OVERRODE the fifth attempt's binding
fallback ("do not try a sixth F-036 design ... select F-072") with a direct instruction
to solve this finding, with Codex leading and its stop triggers converted into revision
triggers. Working lane `~/.claude/lanes/f-036-sixth-attempt/` (`plan.md` revision 7,
`review-ledger.md`, `DECISIONS.md`). Seven plan-review rounds — blocking findings
12, 10, 10, 3, 4, 1, then CLEAN — and three scope consults. **The `advisor` tool was
unavailable for all three consults**, so every one was Codex-only, and that is recorded
rather than passed over.

**The mechanism is documented in `docs/ARCHITECTURE.md`, "The incarnation completion
lease".** What belongs here is why it is the shape it is, and what the review rounds
killed on the way.

**It answers the direction the third attempt named and the fourth could not implement**
— operation-duration quiescence at the object-reuse transition — by making ownership a
per-incarnation LEDGER whose identity is an object rather than a generation string, and
by acquiring it at three structural boundaries instead of at placements:

1. The single pre-dispatch point, per PACKET, against `c.mqtt.tracker` — the incarnation
   the CONNECTION was bound to. This is the term that discriminates where the fourth
   attempt's could not: after a re-mint the displaced connection still names the RETIRED
   tracker, so it is refused by VALUE rather than by a fence it can outrun.
2. Inside `sess.processSub`, per DELIVERY. The fifth attempt's challenge established
   that every production message-delivery handler registers through `client.processSub`;
   the boundary sits one level up, in the session-scoped wrapper, which is where the
   session is in hand — and it covers the class BY CONSTRUCTION, which is what the
   second attempt's four rounds of one-more-placement proved a list cannot.
3. Per timer FIRE, never per delay, and before the client-ID gate.

**Five things the rounds refuted or corrected, so a later change does not reintroduce
them.** *(a)* Reading `sess.leases` at the packet gate instead of the connection's
captured tracker — a displaced packet arriving after the re-mint would acquire the FRESH
incarnation. *(b)* Clearing `c.mqtt.tracker` at the detach — the displaced packet must
still find the retired tracker, not nil. *(c)* A copyable lease value: a retained copy
could resurrect ownership AFTER the drain proof existed, so the lease is a pointer whose
released bit is checked under the same mutex as the count. (The plan reached that
conclusion about a child-ownership API that scope consult 3 then removed entirely; the
revocation property outlived it and is what keeps the drain proof final.)
*(d)* Timer-instance identity
implemented by capturing the `*time.Timer` in its own callback — that is a data race on
the install itself, which the `-race` suite caught; the identity is an immutable install
token. *(e)* A non-waiting "discard permit" for the paths that discard rather than reuse
the object — refuted because an outstanding delivery continuation can still recreate a
map or open a record transaction after the reset, so EVERY caller drains.

**The regression the existing suite caught, and the transferable rule.** Detaching
before the destructive half made `sweepDeadSendHoldsLocked` see every F-089 send-hold
debt of the displaced connection as ownerless and retire a live one — freeing a packet
identifier the client still held. Both liveness predicates read `sess.c`, and the
detach nilled it. The fix keeps the displaced connection the AUTHORITY for its own holds
while nothing is bound in its place, self-clearing at the rebind, which is the event
F-089 already named as the one that retires the debt. **The rule: moving a detach
earlier changes every predicate that reads `sess.c` to decide liveness, and each has to
be re-derived rather than assumed.** Two structural guards also fired and were updated
without weakening what they demand — the F-084 phased-disposal count now counts both
phased helpers (the retirement's capsule-consuming one included, and the
`sess.c`-reading form it replaced was deleted), and the persisted-identity census was
re-pointed at the renamed expiry entry point.

**What unit 1 does NOT close.** The two-dimensional classifier and sites 1 and 3 are
unit 2. The lease is that unit's prerequisite: its unproved identity term — "the
caller's authority to act for the currently bound incarnation" — is exactly what a
lease acquisition establishes, and it is separate both from the subject-derived session
generation and from the per-exchange `rec.gen` a round of the second attempt confused it
with. **Scope consult 3 corrected the fifth attempt's queue inventory — its UNIT-1
IMPLICATION, not its mechanical census, which stands.** That census (four
fire-and-forget classes, not two; shared-consumer identities deterministic and
group-wide; consumer deletes and an application publication riding `sendMsg`) is
accurate and was verified again here. What it does not establish is that those classes
are unit 1's to fence. The controlling boundary is LOCAL EXECUTION that can outlive the
retirement and then observe the re-minted session, mutate the reused object, derive a
name from the fresh incarnation, or emit the old route's protocol result. A transport
entry fully captured while the parent lease was held is none of those, and its safety is
a property of its target — per-consumer-unique names for the plain and drain deletes
(`idHash + "_" + nuid.Next()`, both restricted to non-shared consumers), exact sequences
for the deletes, an uncertainty refusal for a timed-out record write, the documented
group-lifecycle residual for every deterministic group target, and ordinary application
semantics for the Sparkplug publication. The consult therefore ruled the child-completion
design OUT and had the types DELETED rather than kept unused; a JSA timeout abandons the
waiter, not necessarily the queued operation, and that shape predates the lease.

Scope consult 2 additionally ruled the teardown-owned release of shared group
durables OUTSIDE this boundary: it is retirement work created after the displaced
incarnation drained, its scan-then-delete race on a deterministic GROUP name is the
pre-existing shared-subscription residual in `docs/CONFORMANCE.md`, and closing it needs
group-scoped arbitration rather than an incarnation lease. An earlier revision of the
plan had made that release awaited; that was withdrawn, because awaiting is bounded
waiting and not effect completion — a caller that times out leaves the request to
publish afterwards.

### F-036's fifth attempt (2026-09-02): the premise audit, and why it stopped

**STOPPED UNRESOLVED for the fifth time, with no implementation code and no design**,
in the premise phase of a gate pre-registered before any design existed. HEAD
`8e3c3db`, tree clean throughout. Working lane
`~/.claude/lanes/f-036-completion-ownership/`, which holds the gate (`gate.md`), the
premise audit (`premise-audit.md`), the ledger and the decisions. **The `advisor` tool
was available for the gate consult and went unavailable before the audit challenge**,
so the challenge was Codex-only, as the fourth attempt's was. Every checkable claim in
the challenge's verdict was verified against the tree before it was accepted; all
eight held.

The attempt took up the fourth attempt's constraint statement — completion ownership
extending through queued JetStream work and off-read-loop continuations, plus safe
completion-owned cleanup after timeout — under a gate that made a **written premise
audit a deliverable of its own**, because the fourth attempt died on three premises
checkable against the tree in seconds. The gate ruled unit 1 not architecturally
blocked (the boundary "appears finite": one pre-dispatch gate, one `processSub`
registration wrapper, one `mqttJSPubMsg` type and send loop, one bound `clear()`
caller) and required the audit to prove that boundary finite and enforceable. The
audit was written; its adversarial challenge **failed it under the gate's §2 and
fired stop trigger 1** — the boundary as audited was not the boundary — and ruled the
findings ineligible for the single permitted errata pass because they change the
proof boundary. Under the gate's own rule the attempt stops with no design round.

**What the challenge established, each verified against the tree, so a sixth attempt
starts from the corrected boundary rather than re-deriving it.**

1. **Timer-installed callbacks are a unit-1 ingress the four inventories missed.**
   A DISCONNECT/close route calls `scheduleSessionExpiry` (`mqtt.go:4099`), which
   installs `time.AfterFunc(delay, ... expireSession)` (`mqtt.go:6526`) — a
   session-state-touching callback a packet route leaves behind, tracked by no
   WaitGroup (`willWorkerWG` does not cover it). The authorization-expiry and flapper
   `AfterFunc`s and the NATS disconnect/reconnect handlers are further members. The
   expiry callback is serialized against CONNECT by `sessLocked`, so the class is
   finite and enforceable — but it is part of the boundary, and an audit that misses
   it has not bounded the ingress set.
2. **The fire-and-forget `sendq` population is four classes, not two.** Beyond the
   exact-sequence `deleteMsg` (identity-safe: JetStream never reuses an absolute
   sequence) and consumer acks: **consumer deletion** rides `sendMsg`
   (`deleteConsumer(..., noWait=true)`, `mqtt.go:5097-5101`; live callers
   `mqtt.go:12084`, `:19619`, `unsub_drain.go:888`), and an **application-visible
   derived publication** rides it too (`mqtt.go:16430`). Neither is
   generation-named. Worse, shared-consumer identities are **deterministic
   group-wide names independent of any session generation**: a queued shared ack can
   acknowledge a delivery re-offered to another member, and
   `deleteSharedGroupTargetIfUnheld`'s queued no-wait delete (`mqtt.go:19619`) can
   land after a new incarnation or another member has recreated the same durable,
   deleting a live consumer and its interest-retained backlog. The premise that
   fire-and-forget effects are safe *by naming* is therefore false as a class
   statement; it holds only for the exact-sequence delete.
3. **The delivery-callback inventory has seven members, not four.** The three
   account-internal callbacks — `processJSAPIReplies`, `processRetainedMsg`,
   `processSessionRecordDelivery` (`mqtt.go:4627`, `:4639`, `:4653`) — register
   through the same `createSubscription`/`processSub` choke point, and the record
   callback in particular is unit-1-relevant. The choke-point claim itself
   SURVIVES in a narrower form: every production asynchronous NATS
   *message-delivery handler* is registered through `client.processSub`
   (`shim_engine.go:1363`); the challenge found no cloned subscription, reused
   `nsub`, or alternative handler registration. (`mqttFetchIdentity`'s
   `SubscribeSync` (`authz_identity.go:132`) makes the literal "sole creator of a
   NATS subscription" wording false, but it registers no callback.)
4. **Queue shutdown abandons rather than discards.** `sendJSAPIrequests` returns on
   `closeCh`/`quitCh` without popping or draining; entries stay in `q.elts`,
   abandoned with the manager. Awaited callers observe `ErrServerNotRunning` on
   `quitCh` or wait out their timeout on the initialization `closeCh`, and
   `newRequestExMulti` does not delete its reply-map entries on the `quitCh` arm.
5. **Three audited inventories are confirmed and stand**: exactly one of the six
   production `clear()` callers runs on a bound object (the CONNECT clean-start
   arm); exactly two non-test sites assign `sess.c` non-nil (`mqtt.go:12931`,
   `:13000`); both `setIncarnationLocked` callers are the constructor and
   `clear()`. The two in-flight fan-outs (`inflight_register.go:935`, `:4362`) are
   joined by `wg.Wait()` in the same call.

**A binding ruling the stop leaves behind: the pre-`clear()` takeover-tail move is
REFUTED, so it is not tried again.** The obvious unit-1 remedy — run
`asm.discardTakenOverClient` before `clear()` instead of after the rebind, so the
re-mint happens on an unbound object and property P holds by construction — fails
three independent ways. *The tail does not detach*: `discardTakenOverClient`
(`sess_takeover_redrive.go:228`) discards the Will, notifies and starts
`go ec.closeConnection`, but never assigns `sess.c = nil`, and `closeConnection` does
not either — detachment happens later, in `mqttHandleClosedClient`, behind the
teardown gate — so `sess.c` would still be the displaced client while `clear()`
re-mints. *Even an added synchronous detach does not stop admitted completions*: a
delivery callback past its `sess.c == cc` check and blocked in a JetStream
continuation resumes after the re-mint and mutates the reused object; closing the
socket and unsubscribing do not join an already-running nats.go handler; and
`recordWithheldDeliveryLocked` (`sess_restore_gate.go:387-405`) deliberately accepts
`sess.c == nil` and lazily recreates `sess.deferred`, so nil is an ADMITTING value on
that arm, not a quiescence fact. *Run before `clear()`'s unresolved-refusal arm it
violates F-084*: the tail would kill client A and discard its Will, then `clear()`
returns `errMQTTClearRefusedUnresolved` having deleted nothing and the CONNECT is
rejected — no successor was established, yet the incumbent is dead and its Will
suppressed, and the tail performs neither a disconnect stamp nor an expiry
arrangement. The existing arm deliberately preserves the bound client precisely for
this case.

**The corrected constraint statement for any sixth attempt**, which is the
challenge's required-direction list and supersedes the fourth attempt's narrower one:
close acquisition at the structural packet and delivery boundaries; acquire ownership
before the first mutable session observation and retain it through synchronous helper
tails; transfer it into every queued effect **including shared deletes/acks and
application publications**; capture the displaced client and its Will/disconnect/
expiry state before any detach; treat socket close and unsubscribe as cancellation,
never as completion; permit `clear()`, the re-mint and the bind only after old
ownership reaches zero; preserve the unresolved-refusal arm without suppressing the
incumbent's Will; and give queue shutdown, timeout, reply, publish, suppression and
drop exactly-once owner release. Disposal is not quiescence; detach is not
quiescence; `sess.c == nil` is not quiescence.

**What this attempt did NOT do.** No design was written to the gate (the pre-gate
sketch was refuted by the ruling above and is recorded only in the lane). No code, no
tests, no mutations. Nothing in F-036 is fixed or narrowed: the incarnation admission
invariant, the two-dimensional classifier, sites 1 and 3 and every recorded
manifestation stand exactly as the fourth attempt left them.

### F-036's fourth attempt (2026-09-02): the quiescence design, and why it stopped

**STOPPED UNRESOLVED for the fourth time, with no implementation code**, after one
design pass and one consolidated adversarial challenge, under a gate pre-registered
before the design existed. HEAD `ea04a0b`, tree clean throughout. Working lane
`~/.claude/lanes/f-036-quiescence/`, which holds the design (`design.md`), the ledger
and the decisions; the selection consult that set the gate is in
`~/.claude/lanes/selection-20260902/`. **The `advisor` tool was unavailable for this
gate, so it was Codex-only** — the selection that preceded it had both, the gate itself
did not. This section is canonical for what the attempt established; `docs/ROADMAP.md`
carries only the gate result.

The attempt took up the direction the third one named, **operation-duration quiescence
at the object-reuse transition**, scoped to unit 1 — the incarnation admission invariant
— with the classifier and sites 1 and 3 left to unit 2.

**The design that was rejected**, in one paragraph so a fifth attempt recognises it: a
third fence of the shape the tree already uses twice, admitting packets at the single
pre-dispatch point `mqttGateDispatchOnRejection` on predicates `sess.c != c` and a new
`routeClosing`, incrementing an in-flight counter and decrementing it after the handler
returned; the CONNECT clean-start arm would then **detach the displaced client before
the re-mint** (Route A), wait for the counter to reach zero under a wall-clock bound,
run `clear()`, bind the replacement, and only then reopen admission — the reopen moving
from `clear()`'s return, where refuted design 2 left it, to after the bind.

**The gate's verdict: criteria 1, 2, 5 and 6 FAILED; 3 and 4 PASSED; stop triggers 2, 3
and 5 fired.** Under the pre-registered rule a fired stop trigger permits no revision,
so the single permitted revision went unspent, as the third attempt's correction budget
did.

**The five findings that stopped it, each verified against the tree before the verdict
was accepted.** Three of them refuted claims the design itself asserted, so they are
recorded as corrections rather than as opinions.

1. *Fire-and-forget effects survive the handler.* `deleteMsg(stream, seq, false)`
   (`mqtt.go:5552`) pushes onto `sendq` and `sendJSAPIrequests` (`:7163`) publishes
   later on another goroutine. `confirmInflightRegister`'s own comment already names the
   pattern — "the ordinary terminal delete which may be fire-and-forget"
   (`inflight_register.go:3298`). So a counter decremented after the handler returns is
   **not** after every durable effect that handler caused.
2. *Delivery and register continuations run outside any read-loop counter.* An ordinary
   QoS 1/2 delivery callback passing `sess.c == cc` reaches `confirmInflightRegister`
   (`inflight_register.go:3292`) from the JetStream callback, not the readLoop.
3. *`voidRegNonceLocked` mutates before the record fence, and can recreate a map
   `clear()` nilled.* `persistVoidedRegs`'s own comment states the inversion outright:
   "The nonce set is mutated BEFORE the fence is taken (`voidRegNonceLocked`), which
   inverts the usual discipline" (`inflight_register.go:1310-1314`). Reached at `:3330`,
   it repopulates `cancelledRegNonces` on the reset object. **This is a third instance
   of the class the third attempt found twice** in `retireSourceLocked` and
   `noteQ2AffCarrierRefused`; the design had asserted the opposite, that session-record
   writers are already covered by the record fence.
4. *The early detach strands the displaced client on every failure arm.*
   `abandonReDrivenSession` (`sess_takeover_redrive.go:267`) calls
   `disposeForeignStateLocked(asm, nil, true)`, which sources `stale := sess.c`. With
   `sess.c` nilled before `clear()` this is T1a, not T1b: residency goes, but the
   displaced connection is never closed or notified, its armed Will is never discharged,
   no disconnect is stamped and no expiry is arranged. The same re-read breaks `clear()`'s
   existing superseded arm (`mqtt.go:12875`) and its post-reset arms (`:12917`), and the
   unresolved refusal — which today returns having mutated nothing — would newly leave a
   resident unbound session behind.
5. *Disposing on timeout races the operation whose non-completion caused the timeout.*
   Timeout means the in-flight count is above zero by definition, so disposal tears down
   mutable session state while a continuation still owns the object. Deferring disposal
   to route completion is a **new proof boundary**, which is the gate's own discriminator
   for a stop rather than a revision.

**The mutation audit, which is the reusable part.** Three of the design's seven mutations
do not discriminate, and the reason is the same each time. **M1** (ignore `routeClosing`)
and **M4** (reopen at `clear()`'s return) are both masked by the `sess.c != c` term,
because the closing flag and the detach are set in one `sess.mu` critical section — so
the paired test passes against the mutated code. **M3** (defer the decrement inside the
handler) is not a mutation at all: a Go `defer` runs after the handler body, PUBREC
enqueue included, and before the handler returns, so it *preserves* the ordering it was
supposed to break. And **M-P as written does not compile**, because the displaced client
is sourced from the very call the mutation deletes. That left the design with no
discriminating mutation for its central admission term, which is itself evidence the term
did less work than the design claimed.

**What the attempt ESTABLISHED, so a fifth does not re-derive it.**

1. **The ownership term is the admission barrier; the closing flag is not.** With
   `sess.c != c` evaluated at the pre-dispatch point, moving the reopen back to
   `clear()`'s return does **not** reproduce the sibling PUBREL manifestation, so the
   reopen-after-bind boundary is not load-bearing and `routeClosing` is redundant *as an
   admission predicate for the displaced client*. This governs admission only — it says
   nothing about effects already admitted, queued, or raised off the read loop.
2. **The examined wait-for graph is acyclic, for a corrected reason.** No cycle exists,
   but not because teardown is the only displaced-side consumer of the client-ID gate —
   that claim was false. SUBSCRIBE and UNSUBSCRIBE's plain acquisition, the rejection
   settlement's bounded acquisition, the resolvers and the void-verdict handoff all
   contend too; each fails or yields rather than closing a cycle. `mqttPublishOwedWill`
   (`mqtt.go:14905`) uses client and account state, not `sess.mu`, the session gate or
   the record fence.
3. **A wall-clock ceiling is achievable.** A `select` against a timer bounds the wait
   even when the in-flight JetStream call's own timeout is longer; that makes a timeout
   more frequent, not unbounded. What is **not** designed is a safe disposition once it
   fires — see finding 5.
4. **Three positive facts hold and can be relied on.** A packet following CONNECT in the
   same `mqttParse` buffer *is* gated, because `connected` is set before the loop
   advances (`mqtt.go:3691`). PUBREL *does* evaluate the pre-dispatch gate before
   deriving its hold subject (`:3489`, `:3558`). `mqttEnqueuePubResponse` *is*
   synchronous with the handler, appending under `c.mu` before returning (`:16480`,
   `:14470`), and the later socket flush does not re-read the session.
5. **Two effect classes defeat any read-loop-scoped signal** — queued JetStream effects
   (finding 1) and off-read-loop delivery/register continuations (findings 2 and 3).
   Either alone is fatal; the record fence does not cover the second, and neither closing
   nor draining the read loop drains the first.
6. **A timeout arm requires completion-owned cleanup**, and that is a new proof boundary
   rather than a placement, an error code or a lock-release adjustment.

**Constraints on a fifth attempt.** It must govern **both** escaped effect classes, not
only readLoop routes, and it must supply a **safe post-timeout disposition** — one that
neither races the outstanding operation nor loses the displaced client's close,
notification, Will, disconnect stamp and expiry. Nothing here shows operation-duration
quiescence to be impossible in general; what it shows is that a signal scoped to the read
loop cannot express it, and that Route A's detach cannot be moved earlier without
rebuilding every failure arm that re-reads `sess.c`.

**What this attempt did NOT do.** No implementation code was written. No test suite, no
mutation campaign and no holistic code review were run — the mutations above were audited
on paper and found wanting, not executed. Nothing in F-036 is fixed or narrowed: the
incarnation admission invariant, the two-dimensional classifier, sites 1 and 3 and every
previously recorded manifestation stand exactly as they did.

- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 36

### Original finding record

- [ ] Investigate: a resident session NEVER re-validates its ownership absent a local
  rejection, so a remote takeover whose persist notification is lost still strands the
  object when this broker made no failing write. The rejection settlement above bounds
  convergence only for rejection-caused staleness: its marker is armed by the store
  boundary, so a session that simply never writes again is never looked at. The expiry
  sweep cannot cover it either (it skips never-expire outright, and defers to a resident
  object for the rest). Any fix has to arbitrate rather than read — the same 10164
  in-process argument applies — and a periodic unconditional probe of every resident
  session is a write per session per interval, which is why this is filed rather than
  folded in. Found while planning the rejection settlement.

### Two manifestations added by F-032's audit (2026-08-17), which WIDEN this finding's shape

This finding was stated as the **absent-record** tail: a bound, silent client holding a
session another broker tore down. F-032's audit found two further consequences of the same
underlying condition — the previous owner keeps serving, or is misread as gone, after
ownership has moved — and they begin *before* the record is absent. **The shape is therefore
widened from "an absent record under a bound session" to "the previous owner continues to
serve, or is treated as absent, after durable ownership moved".** These are recorded here
rather than as new IDs because both are consequences of this finding's own condition; they
are not F-062, whose two mechanisms are teardown-side reuse hazards.

- **Post-enumeration serving.** `arbitrateQoS2ReleaseClaims` enumerates hold subjects
  once, through `STREAM.INFO`. A hold on a subject **absent from that snapshot** is never
  passed to `discardQoS2Hold`, so it is missed rather than destroyed. (A replacement on a
  subject the snapshot DID contain used to be reached by name and destroyed by the pending
  purge — F-062 mechanism 1, not this. Since F-062 closed, that replacement is SPARED
  instead, which removes the loss and leaves the peer-still-serving direction below.) If
  the peer has not yet observed the ownership
  marker, it wins an ordinary `releasing` claim, passes its authoritative re-read and
  **delivers onward after the session was discarded**. Note the direction: this is a
  delivery that should not have happened, not a loss. F-062's purge cutoff, now in place,
  does not address it and must not be described as though it did: a cutoff at
  `Sequence: E+1` excludes never-enumerated holds and bounds purging to sequences at or
  below `E`, but it does **not** make teardown own exactly the enumerated set — `LastSeq`
  and `Subjects` are not one snapshot, so it also spares a subject that WAS enumerated
  whose record landed above `E` (F-062's first residual). **The cutoff therefore widens
  this manifestation rather than narrowing it**: the population of holds that survive a
  teardown and stay serviceable now includes enumerated-but-above-cutoff ones, not only
  never-enumerated ones. That is the correct trade — sparing an acknowledged payload is a
  leak, destroying it was a loss — but a fix here must account for both, not only for a
  never-enumerated hold.
- **The orphan sweep's false-absence inference.** The sweep's `purgeQoS2Hold` runs under no
  teardown claim at all, and `buildSessionRefs` infers liveness from **this broker's
  residents plus persisted session records only**. A live remote generation whose persisted
  record is absent is therefore invisible to it, and its hold is condemned. That is this
  finding's precondition exactly — including when the record was made absent by a
  bridge-authored teardown, since `releaseTeardown` deletes it. Generation uniqueness does
  not help: the peer is *continuing* to use the generation, not re-minting it.

A fix for this finding must therefore answer both, not only the silent-Keep-Alive-0
case, and its tests should stage the post-enumeration ordering explicitly.

**The prose across the orphan sweep rests on this finding's premise, and is bounded rather
than individually corrected.** F-032's audit had a reviewer enumerate, from the concept
rather than from phrasings, every statement in the repository asserting that a late or
repeated deletion can only reach dead state, or that absence from local evidence proves a
generation gone. Twenty-six sites came back. They were sorted by **whose falsified premise
each rests on**: those resting on F-062's (teardown-by-name is safe against a peer that
resumed the retiring generation) were corrected under F-032; about sixteen rest on
**this** finding's premise — that persisted records plus one broker's residents prove a
remote generation absent — and span `internal/mqtt/orphan_qos2_sweep.go`,
`docs/ARCHITECTURE.md`'s orphan-reconciliation section, `docs/findings/resolved.md` and two
test helpers.

Those sixteen are **not** rewritten one by one. `orphan_qos2_sweep.go`'s header now defines
what "AUTHORITATIVE" means throughout that file — authoritative over persisted records and
this broker's residents, explicitly not over a session live elsewhere whose record is
absent — and every later use inherits that bound. Restating the boundary at each site is
what made F-032's own review loop self-feeding, and the same discipline is applied here:
one statement, many sites bounded.

**Whoever closes this finding should re-run that conceptual enumeration** and correct the
sites individually, because at that point the premise is no longer merely bounded but
fixed. A further five sites assert consequences for `$MQTT_inflight`, the affine path and
the PUBREL durables; those were deliberately converted to refusals-to-answer rather than
corrected, because their consequences belong to **F-033, F-034** and this finding, none of
which had been audited when they were written.

**The affine ones among those five have since been answered.** F-033's audit closed on
2026-08-18 and rewrote its four markers across three sites to state their answer rather
than defer it. Two of the affine consequences sort onto **this** finding's premise and are
recorded as its population — a contender discarding an assignment on an absent winner
record, and the different-generation reading whose residue is a previous owner still
serving. The rest are defects of ours with premises of their own: **F-063**, **F-064** and
**F-065**. The `$MQTT_inflight` and PUBREL refusals are ANSWERED since F-034's audit
(2026-08-22); the manifestation it added is below.

### A fourth manifestation added by F-034's audit (2026-08-22): register contention

F-034 audited every route by which an `$MQTT_inflight` register could be destroyed by an
actor not entitled to it, and closed all of them: the drain's release addresses the register
its own packet identifier names and deletes only a matching occupant at the exact sequence
it read; the teardown's reap enumerates, loads, classifies and deletes only at or below the
watermark its own enumeration returned; and the orphan sweep's exact-sequence revalidation
was found sufficient, with identity conditioning there REFUTED because no mismatching record
can occupy that sequence within one stream incarnation, so no mutation could prove it.

**Every destructive path that reads a body binds it to its subject** — where destructive
means DELETE OR OVERWRITE. That invariant took three review rounds to state correctly, and
the intermediate versions were wrong in a way worth recording: rounds 4 and 5 swept *delete*
sites and concluded "no other delete site reads a body it classifies", which was true and
insufficient. The read-after-CAS reconciliations delete nothing — they CAS-REPLACE at the
sequence they just read — and that is destructive by the same route, because it turns a
record recovery calls unreadable, whose identifier is therefore RESERVED, into a valid
tombstone for the subject's own identifier that a later resume reads as freeable. Seven
sites now carry the binding: the drain's release, the teardown's reap, the store's
ambiguous resolution, the retirement and both cancellation marks, and the resume claim
contest. Each refuses and KEEPS, which differs deliberately from the reader, which reserves
and keeps: both preserve the record, and only the reader is in a position to claim the
identifier as well.

**What none of that closes is contention with a genuinely live owner**, and the reason is
structural rather than an omission. A register a peer is still using presents *exactly* the
shape a legitimate release expects: the same subject, the same `(Dur, Seq)`, a valid
identity, a body that agrees with its subject. Naming cannot separate the two actors,
because they do not disagree about identity — they disagree about who owns the session. The
predicate that would separate them is this finding's: **a resident session object can run
register recovery against a generation whose durable ownership has moved, because a resident
object never reads the retirement watermark that the record-restoring CONNECT path does**
(`mqtt.go:8711-8741`).

The necessity result is carried as a **passing reproducer** rather than prose:
`TestInflightLiveOwnerContentionNeedsAnOwnershipPredicate` asserts positively that every
F-034 check is satisfied by such a register, so none of them is what would stop the
deletion. If a future change makes any of those assertions fail, the residual has been
closed by something other than this finding and the routing must be re-derived.

### A fourth manifestation (2026-09-01), found by course E's own structural guard

**Teardown adoption's affine consumer delete.**

**No new finding ID and no second checkbox**, per this finding's own precedent for
consequences of its own causal defect — the previous owner keeps serving after durable
ownership moved.

**How it was found, which is worth recording.** Course E added
`TestQ2AffineConsumerDeletesAreInventoried`, a package-wide inventory of every
`$MQTT_out` consumer delete. Writing it surfaced that
`deleteTeardownTargets` IS reachable from the sweep, because the sweep adopts abandoned
teardown markers and finishes them (`finishAdoptedTeardown`). Five prose review rounds had
not noticed. The guard's sixth review round then produced the interleaving below. A property
found what an enumeration could not.

**The window.** `deleteTeardownTargets` runs `discardQ2AffClaimsFor` and then deletes
`t.q2AffDur` (`sess_teardown.go:847` then `:852`). A won `claimTeardown` arbitrates the
**session record**; it does **not** fence a stale peer still serving the same AFFINE
generation. So that peer can `beginQ2AffOp` and win a create-only claim CAS after the scan
returns and before the delete lands, materialize onto the consumer, and then commit the
irreversible source acknowledgement on a copy-presence proof taken before the delete —
leaving a `materialized ∧ SourceAcked` claim whose payload was destroyed.

**Both halves were already recorded, which is why this is a manifestation rather than a new
defect.** The unfenced scan is this finding's own refuted-design note: "teardown's
`discardQ2AffClaimsFor` is a stream scan plus per-claim CAS and installs NO fence against
future claim creation." The proof-to-ack interval is **F-063's stated residual** in
`docs/CONFORMANCE.md` — "a consumer deleted between the final proof and the ack" — and is
cross-referenced rather than restated as a second residual.

**It is PRE-EXISTING and path-independent, and course E neither introduced nor widened it.**
`sess_teardown.go` is unchanged from HEAD `4afa533`, and the scan-then-delete sequence lives
inside `deleteTeardownTargets`, reached identically by ordinary teardown (`mqtt.go:10876`,
`:10930`) and by the sweep's adoption (`sess_teardown.go:1124`). Removing only the sweep's
route would change which driver reaches the window without closing it, while permanently
stranding the consumer and its copies for every client that never returns — which is why
that course was **refused** rather than deferred.

**What closing it needs, and why it is not course E's:** a distributed fence preventing every
same-generation session from starting or completing a move, then a complete post-fence
discard scan. That fence WAS F-036's remaining unit at the time — the incarnation admission
invariant whose placement closure stopped the 2026-08-27 and 2026-09-01 attempts with no
code either time; unit 1 landed it on 2026-09-02. This AFFINE site was nonetheless left
outside F-036's 2026-09-03 closure, deliberately and for a structural reason: the
retirement licence unit 2 rests on names the SESSION generation, while the affine one
outlives incarnations, so no retirement proof can gate it. The guard's inventory therefore
carries this site as *known pre-existing, outside course E's behavioral change, retained
without any claim that it is race-free* — and it is owned by ROADMAP chapter P rather than
by this finding.

### A third manifestation added by F-033's audit (2026-08-18): the adoption blind spot — DISSOLVED

**DISSOLVED, not narrowed, by F-036 course E.** This manifestation existed only through a
by-name delete of a consumer a resuming session adopts, and that delete no longer exists:
`deleteOrphanQ2AffConsumers` was removed outright. There is no window left to guard, and no
fix is owed for it. The account below is kept because its reasoning is what refused every
guarded version of the delete — an instance fingerprint cannot see an adoption — and a
future reader proposing one needs to meet that argument rather than rediscover it. Read it
in the past tense.

`deleteOrphanQ2AffConsumers` deleted an orphaned affine consumer **by name**, after two
guards: a final session-record re-read (step 4) and a check that the consumer is still the
INSTANCE quarantine observed, by `ConsumerInfo.Created` (step 5). **Neither discriminates
a
session that RESUMES onto the consumer between them.** A resume preserves the incarnation
generation and derives the same durable name, and affine provisioning is a
create-or-**UPDATE** — so no new instance is created and `Created` is unchanged. Step (5)
therefore passes, and step (6) destroys a live session's consumer together with every copy
interest retention was holding for it.

This is **this finding's** population, not a separate one, and the audit corrected itself
on
exactly that point. It first read the window as independent of false absence, on the grounds
that the record was genuinely absent when step (4) read it. But `sess.q2AffGen` is only ever
populated from a persisted record (`mqtt.go:6045`, `:8702`, `:8755`) and provisioning mints
a FRESH generation when it is empty (`qos2_shared_affine.go:1072`), so a session resuming
with no record cannot adopt the condemned generation's consumer at all. Adoption requires
a
session **already resident with that generation while its record is absent** — and the
sweeping broker's reference set covers only persisted records and its OWN residents. That
is
this finding's premise verbatim.

**What is new, and what a fixer must not assume.** The `Created` fingerprint reads as a
defense against acting on a stale candidate, and its own comment describes the case it was
built for — a durable deleted and recreated under the same name. Against an adoption it is
not a defense at all. Any fix for this finding that leans on the instance fingerprint to
bound the consumer delete will still delete a live adopted consumer; the delete needs
arbitration, not a third read. The resulting state — a live matching record whose affine
consumer is gone — is also precisely **F-064**'s precondition, so this manifestation
reaches that finding's permanent stall as well as destroying the copies directly.
`TestQ2AffResumeAdoptsConsumerKeepingItsCreated` pins the adoption mechanism, and is kept
after course E precisely because it is the standing refutation of any future by-name delete
guarded by an instance fingerprint.

## F-062

**Title:** Teardown reuse hazards on an inbound QoS 2 hold subject after ownership moved

- Status: **Closed, 2026-08-18.** Found by F-032's consequence audit, 2026-08-17. Both
  mechanisms are remedied; see "The remedy as built" and the residuals below.
- Classification: HA arbitration architecture / data loss
- Release treatment: **Alpha blocker.** Multi-frontend HA is supported alpha scope, and the
  PUBLISH that is lost was **acknowledged**: PUBREC follows the hold landing, so the client
  has been told the message is held.
- Roadmap section: §0.2 (multi-frontend HA blockers)

### The finding

Not a JetStream shortcoming. `purgeQoS2Hold` sends only a subject filter, so the server has
no condition to enforce and removes whatever occupies the subject when the purge
applies. We never asked for anything narrower. See `docs/ARCHITECTURE.md`, "Trusting
the backend", for why that distinction matters.

**Shared precondition.** Broker A **wins** `claimTeardown` against B's live session on
generation `G` — the won-claim ordering, not the lost-claim one that
`TestSessTeardownLostClaimDeletesNothing` already fences — and B's client reuses a packet
identifier while A's teardown walk is in progress. A resume preserves the generation, which
is what makes "a peer still serving `G`" reachable at all.

**Mechanism 1 — an unbounded subject purge destroys a newly accepted exchange.** A reaches
`awaitQoS2Release`, observes B's `releasing` claim vanish when B completes exchange E1
normally, and commits to the purge. B's client PUBLISHes E2 on the same identifier and **is
answered PUBREC**; its create-only hold succeeded. A's purge then destroys E2's acknowledged
payload. **Direction: loss.**

**Every arm, because a per-arm remedy would leave the others unbounded.** `purgeQoS2Hold`
was reached from four places **as found**, and mechanism 1 is a property of the SHARED purge
rather than of any one caller. (As built it is reached from three: A3 no longer purges — see
the remedy below. The four-arm analysis is still how the finding is established.)

| Arm | Route | What A holds when it purges |
|---|---|---|
| A1 | `discardQoS2Hold` wins a fresh `discarded` claim (`qos2_release_claim.go:689`) | the teardown claim, and a freshly won marker. **Not** evidence that the hold subject was empty: the CAS is on the sibling CLAIM subject, so it can be won while a hold occupies the hold subject — see the `A1 spares a hold above a stale watermark` subtest, which stages exactly that |
| A2 | `discardQoS2Hold` observes an existing `discarded` marker, its own from an earlier attempt or a peer's (`:718`) | the teardown claim, and a marker that may predate the current occupant entirely |
| A3 | `awaitQoS2Release` observes the claim subject EMPTY (`:750`) | the teardown claim, and **no release claim at all** — it LOST that one, which is why it was awaiting. And the claim it lost to may have belonged to an EARLIER exchange, so the subject going empty says nothing about the current occupant |
| A4 | the orphan sweep (`orphan_qos2_sweep.go:1393`) | no teardown claim either; fenced by the reference walk instead — that arm is **F-036**, not this |

**Distinguish the two claims, because A3 is where conflating them misleads.** The
**session teardown claim** is what licenses the walk; the per-subject **release claim** is
what arbitrates one exchange. In A3, A holds the first and never acquires the second, so
E2 can win its own `releasing` claim and pass `verifyQoS2HoldForRelease` — returning
`Held` — and that newly won claim cannot cancel A's already-committed purge. In A1 and A2,
A does hold a marker, but holding one says nothing about which occupancy it covers: the claim
CAS is on the sibling subject and settles only who acts, never what may be acted on (see the
per-arm criterion under the remedy). **In none of the three, as found, was the purge fenced
against the subject's CURRENT occupant** — which is the defect, and what the cutoff supplies.

**The cutoff closes A1, and NOT A2.** For A1 the marker belongs to *this* walk, so any hold
accepted after this walk's enumeration sits above `E` and is spared — which is why the cutoff
belongs **inside `purgeQoS2Hold`**, at the shared boundary, rather than in one caller.

**A3 needed more than a cutoff, which the implementation review established after the plan
was approved.** Its purge was licensed by the claim subject going empty, and that is not
evidence about the enumerated exchange at all: a releaser reaping an earlier generation's
stale `releasing` claim empties the subject too, and the exchange occupying the hold
subject may be a LATER one, accepted and PUBREC-acknowledged, sitting at or below this
walk's watermark. The cutoff cannot spare it. Worse than the destruction is what that
exchange's releaser was then told: with no marker on the subject its create succeeds, its
re-read returns `mqttQoS2VerifyGone` — "terminated and already reaped" — and the PUBREL
answers **0x00** for a message that was destroyed and never delivered. A3 now re-competes
instead, so the same exchange is fenced by a marker the walk actually won and is answered
0x92. Fixture: `TestQoS2AwaitedReleaseDoesNotInheritAStaleClaimsEntitlement`; mutation:
restore the direct purge, which must fail it.

A2 defeats it, and this is the arm most likely to be missed by a remedy that looks
complete. The marker there is from an **earlier, partial teardown**, so the fence it
represents is older than the current walk's watermark. Interleaving: an earlier walk leaves
a `discarded` marker without completing the purge; B then accepts E2 and answers PUBREC;
a repeated walk enumerates, and **its `LastSeq` now includes E2**, so `E` is above E2's
sequence; `discardQoS2Hold` observes the pre-existing marker and purges with `Sequence:
E+1`, which deletes E2. The current watermark is simply the wrong fence for a marker that
predates it.

So A2 needs the marker's **own** fence — the value the marker itself records — which is the
same evidence mechanism 2's remedy has to supply. **A1 and A2/mechanism 2 therefore share
two remedies, not one**, and a fix that implements only the cutoff still loses acknowledged
payloads through A2. The fixture must fail if only the cutoff is implemented. (A3 needed a
third thing entirely, established after approval — see above.)

**Mechanism 2 — a persistent `discarded` claim poisons a later exchange.** A creates the
`discarded` marker and the teardown proceeds. B's PUBREL for E1 observes it, releases E1's
hold and answers 0x92. B's client then reuses the identifier: E2's create-only hold succeeds
on the emptied subject and PUBREC is returned. E2's PUBREL reads the **same, still present**
marker and is answered 0x92 with E2's hold released. **Direction: loss**, established by
exhausting every arm that reads a claim state — the two delivering arms require
`State == releasing`, which a `discarded` marker never satisfies; the repeated-teardown arm
purges the hold; and, **as found**, nothing reaped a `discarded` marker except the aged
orphan sweep, hold-first — the remedy below adds the one conditional releaser reap.
 Successful cleanup does send PUBCOMP (0x92 on MQTT 5) and frees the identifier,
so this is loss and repeated poisoning, **not** a protocol wedge.

**Mechanism 2 is not closed by bounding the purge.** The marker survives independently
of any purge, which is why these are two mechanisms rather than one defect with two
symptoms.

**Recovery is conditional, not automatic.** Each affected payload is permanently lost. The
poisoning is only temporarily so: an eligible orphan sweep can retire the marker hold-first,
but that depends on the sweep running, and its liveness has three documented limits — a
session-record identity failure aborts every destructive phase account-wide and
permanently, a transport failure aborts a pass, and a broker restarting more often than
the grace never reaps at all.

**Why no existing residual owns this.** `docs/CONFORMANCE.md`'s marker-arming concession says
only that *a packet racing the marker's arming is served*; neither purging acknowledged state
nor poisoning a later exchange is that. And the "a discarded session leaves one small claim
record per inbound QoS 2 exchange **that was in flight**" statement understates the reach:
mechanism 2 is one marker affecting an exchange that was not in flight when it was written.

**What is not claimed.** No universal over interleavings. Each mechanism carries its own
direction, established by walking the arms above; nothing here asserts no other ordering
exists.

### The remedy as built

Two remedies sharing one durable field, because A1 and A2/mechanism 2 need different
evidence — plus a third change to A3, which needed neither of them
(`internal/mqtt/qos2_release_claim.go`, `mqtt.go`, `orphan_qos2_sweep.go`).

**1. Every hold removal carries a bound.** `streamSubjects` now returns
`State.LastSeq` alongside the subject map, and `purgeQoS2Hold(holdSubject, upTo)` emits
`JSApiStreamPurgeRequest{Subject, Sequence: upTo + 1}`. There is **no unbounded form of
that function**, so no caller can opt out.

**Which bound each arm uses. This mapping is authoritative; every other site points here
rather than paraphrasing it, because "the walk's watermark" is true of only two of the
four arms and three review rounds were spent on paraphrases that forgot which.**

| Arm | Bound passed to `purgeQoS2Hold` | Established by |
|---|---|---|
| A1 — fresh `discarded` win | the caller's proven cutoff: the walk's enumeration watermark for a session teardown, the re-confirmed record sequence for the orphan sweep | this call's own evidence |
| A2 — adopted pre-existing marker | **`cur.Fence`**, the adopted marker's own recorded cutoff, which **may exceed this walk's watermark** | the entitlement the marker's writer enumerated |
| A3 — awaited release | **none — it purges nothing.** On seeing the claim subject free it RE-COMPETES, so any destruction happens under A1's row | — |
| A4 — orphan sweep's direct purge | the exact hold sequence the pass loaded and re-confirmed | reference walk, grace, re-confirmed sequence |

So a hold accepted after **this** walk's enumeration is spared on A1, and may be destroyed
on A2 — correctly, when the adopted marker's deciding watermark covered it. A3 destroys
nothing, so the question does not arise there until it has re-competed and become A1.

**What licenses a destruction is coverage by the deciding watermark PLUS a fence, and the
fence is not the same object on every arm.** It is not universally a won `discarded` marker;
A3 explicitly holds none and A4 never requires one:

- **A1 and A2** are marker-backed: coverage, plus winning (A1) or adopting (A2) the
  `discarded` marker. The timing of the winning CAS within an occupancy settles nothing —
  only coverage decides what may be acted on.
- **A3** has no entitlement of its own and destroys nothing. An earlier design gave it
  one — coverage plus an *observed completed release*, on the grounds that a releaser
  deletes its claim only after the terminal commit — and **that premise is false**: a
  releaser also exact-deletes a FOREIGN generation's `releasing` claim before retrying
  its own create, so an empty subject can mean "someone reaped a stale claim", not "the
  exchange terminated". A3 therefore re-enters the create-only competition, and destroys
  only as A1.
- **A4** has its re-confirmed exact sequence, plus a *standing claim of any state* as the
  fence — `discarded`, `releasing`, or unreadable are equally effective, since the claim write
  is a create-only CAS that fails while any record occupies the subject — plus the
  stable-observation grace.

So "coverage and the marker win" is right for A1/A2 and wrong as a universal rule; do not
generalise it across the table.

Two values are refused rather than sent — `0`, which would emit `Sequence: 1`
and report success for a purge both stores treat as a no-op (false success, the
terminal-cleanup-before-completion mistake), and `MaxUint64`, whose `upTo + 1` would wrap
back to the unbounded shape.

**2. The `discarded` marker records the cutoff its writer was entitled to purge through.**
Not the cutoff it *did* purge to: the marker is written durably BEFORE `purgeQoS2Hold` is
called, so a crash or a failed purge can leave a marker whose purge never ran. The fence is
a statement of entitlement, never evidence that the hold is already gone, and no code may
treat it as the latter. `mqttQoS2RelClaim` gains
`Fence uint64`, required for `discarded` and absent for `releasing`, enforced by
`mqttQoS2RelClaimIdentityOK`. It is identity, not forensics — it is read and it decides —
so a fence-less marker is an unsupported persisted shape and is **refused** (classifying
unreadable), never read as fencing nothing. What it is not: an identity of the exchange the
marker competed for. It associates the marker with a **range** of hold occupancies, since
the create-only CAS is not atomically tied to any one of them.

That field then does two jobs:

- **A2 purges to `cur.Fence` alone**, never `min(cur.Fence, upTo)`. The walk enumerates
  every subject up front but does not load a sibling claim until it reaches it, so a
  *concurrent* teardown can enumerate higher and win the marker in between; clamping to
  our own watermark would spare a hold that marker's decision covered while we report the
  walk complete. Adopting a marker means finishing an already-durable decision, so the
  bound is the entitlement that decision recorded.
- **A releaser reaps a marker it can prove is not about it**: in `claimQoS2ReleaseInner`,
  `holdSeq > cur.Fence` reaps at the marker's exact sequence and retries the create. This
  is the sole exception to scope rule 3, and exactly as narrow as the rule states —
  conditional, exact-sequence, and by a releaser holding the evidence.

**The keystone, because the reap rests entirely on it.** The property is **strictly
increasing occupant sequences** on one hold subject within one `$MQTT_qos2in` incarnation
— *not* "monotone occupancy", which is false: occupancy alternates between occupied and
empty, and that is the point. It holds because a hold write is a create-only CAS, so a new
occupant lands only on an empty subject at the stream's next sequence; the terminal
tombstone is not a create but an exact-sequence CAS replacement, which under
`MaxMsgsPer: 1` + `DiscardOld` lands at a **new, higher** sequence; and every removal — an
exact-sequence delete, or a filtered purge — leaves `LastSeq` where it was, so no earlier
occupant returns and no sequence is issued twice. A hold above `Fence` therefore proves
every occupancy the marker's writer was entitled to destroy ended before it began. The
evidence is permanent, so a stale `holdSeq` does not weaken it, and the reap is not a
shortcut past the fences: the retried create must still win and `verifyQoS2HoldForRelease`
must still find the record at its sequence.

**Scope: one stream incarnation.** A `$MQTT_qos2in` whose sequence space restarts breaks
*every* sequence identity in this protocol with it — the per-subject CAS expectations,
scope rule 2's exact-sequence claim deletes, `releaseQoS2Record`, the sweep's
`qos2ClaimUnchanged`, and now this fence. That precondition is pre-existing and
repository-wide, not introduced here. Recreating that stream ALONE is no longer a
supported operation (F-066: the nine broker-owned streams are one recreation unit and a
proper subset is refused at startup); the supported operation that restarts the space is
an all-brokers-stopped reset of the whole bundle, under `docs/ARCHITECTURE.md`'s
compatibility boundary. The
change is a strict improvement inside it: the previous purge carried no bound at all.

**An enumeration that cannot bound a purge is never acted on.** `STREAM.INFO` builds
`State` before it fills `State.Subjects` from a separate store read, so a walk begun
against an empty stream can legitimately receive a non-empty subject map with
`LastSeq == 0`. Acting on it would write a `Fence: 0` marker, which the identity rule makes
unreadable, and the unreadable disposition on the PUBREL path then deletes the
acknowledged hold and answers 0x92 — manufacturing the exact loss this finding closes. So
the watermark is validated **before any write** and the sample re-taken, with nothing
written in between. Exhausting that budget **errors and fails the clear**; it is not
reachable in contract, since `LastSeq` never rewinds under a delete, a replacement, a
filtered purge, a compact at or below the tail, or expiry (and this stream refuses
`MaxAge`).

A per-write coherence check asserting `0 < Fence < markerSeq` was designed during review
and **deleted**; do not re-propose it. It caught numeric inversion but not stream
recreation, and it did not prevent the loss even in the case it caught — A1 refuses its
purge while the incoherent marker stands durable, and the releaser's unreadable
disposition then destroys the acknowledged hold anyway. It postponed the loss; it closed
nothing.

### Fixtures and discriminating mutations

All of the below are in `internal/mqtt/qos2_release_claim_test.go` unless noted.

- **`TestQoS2TeardownPurgeSparesAPostEnumerationHold`** — mechanism 1, and it must
  discriminate **every arm in the table above, not one branch**, because the arms do not
  share one remedy. Structure it at the shared boundary — assert `purgeQoS2Hold` never
  removes a record the teardown was not entitled to. Five subtests, covering three arms —
  A1 twice, A3, and A2 twice — because two arms have a second case that discriminates a
  rejected remedy:
  1. *A1, fresh `discarded` win.* E2 accepted between this walk's enumeration and the
     purge. Closed by the cutoff: E2 sits above this walk's `E`.
  2. *A3, `releasing` claim disappears.* Same staging, entered through `awaitQoS2Release`.
     The vacancy licenses no purge: the walk RE-COMPETES, wins a fresh marker, and the hold
     accepted after enumeration is then spared by that marker's cutoff — so this subtest
     exercises the post-wait re-competition and the A1 purge it leads to.
  3. *A2, marker from an EARLIER partial teardown.* Stage it in that order: earlier walk
     leaves the marker without purging, **then** E2 is accepted and answered PUBREC,
     **then** a repeated walk enumerates — so E2 is BELOW this walk's watermark. **This
     subtest must fail against a cutoff-only implementation**, since the cutoff cannot
     fence a marker older than the watermark; it passes only once the marker carries its
     own fence (mechanism 2's remedy).

  4. *A2, CONCURRENT teardown fenced HIGHER.* The mirror of 3 and the case `min` got
     wrong: the adopted marker's fence is above both E2's sequence and this walk's own
     watermark, so E2 **must be destroyed**. The assertion is inverted deliberately —
     it is what stops a clamp being reintroduced as the safer-looking rule.

  5. *A1 with a STALE watermark.* The non-snapshot gap's own direction, measured rather than
     only asserted: the `Subjects` sample includes a hold above the returned `LastSeq`, so
     the marker's CAS is won DURING that hold's occupancy and the hold is spared anyway.
     Asserts the hold survives, the marker is fenced below it, and the exchange is still
     serviceable — a leak belonging to F-036, not a loss. This is the subtest that shows
     winning the CAS is not what makes an occupancy discardable.

  Subtests 1-3 and 5 assert the hold survives at its own sequence **and its PUBREL
  completes**,
  driving `claimQoS2Release` plus `verifyQoS2HoldForRelease`: an intact record whose claim
  is refused or whose re-read returns `Superseded` delivers nothing, so record survival
  alone would not be the property. Subtest 4 asserts the hold is gone and the walk still
  reports success. Arm A4 is the sweep's and is covered by the two orphan mutations below.
  Dropping the purge's `Sequence` bound fails subtests 1, 2, 3 and 5 — subtest 5 included,
  which is what makes it a measurement rather than a restatement.

  **Mutations, one per remedy so neither can be dropped:** drop the purge's `Sequence`
  bound — A1 and A3 must fail; purge A2 at `upTo` instead of `cur.Fence` — **both** A2
  subtests must fail; purge A2 at `min(cur.Fence, upTo)`, the rejected draft rule — only
  A2-concurrent must fail, which is what makes that subtest load-bearing. A remedy
  implementing only the cutoff leaves A2 red, which is the point of splitting them.
- **`TestQoS2TeardownStaleDiscardedClaimDoesNotPoisonReuse`** — mechanism 2, and it must
  cover **both orderings**, because the harder one is what discriminates a real remedy:
  1. *Marker first* — A writes `discarded`, E1 answered 0x92, E2 accepted afterwards, so
     E2's hold sequence is **above** the marker's.
  2. *Hold first* — A captures its cutoff while enumerating E1; E1 completes and vacates the
     subject; E2 lands and is answered PUBREC; **only then** does A write the marker; a
     bounded purge correctly spares E2; E2's PUBREL is poisoned anyway. Here E2's hold
     **precedes** the marker.

  Ordering 2 exists in this dossier for a specific reason: the obvious repair — *reap the
  marker when `holdSeq > claimSeq`* — **passes ordering 1 and fails ordering 2**. A fixture
  staging only ordering 1 would bless a broken remedy. **Mutations:** remove the reap
  (always `Discarded`) — **both** subtests must fail, so a partial remedy cannot score the
  kill; and reap on the naive `holdSeq > smsg.Sequence` instead of the fence — only
  ordering 2 must fail, which is the discriminator the ordering exists for.
- **`TestQoS2DiscardZeroWatermarkWritesNoMarker`** — the `LastSeq == 0` corner. One
  subtest re-enumerates and completes on the retry, asserting no marker is ever written
  with a zero fence; one holds the sample incoherent for the whole budget and asserts the
  walk **errors**, the hold and its payload survive, and no claim record exists.
  **Mutations:** drop the pre-write validation, so a `Fence: 0` marker is written; and make
  exhaustion return `nil` instead of an error.
- **`TestQoS2HoldPurgeRefusesAnUnusableCutoff`** — one subtest per refused value. It
  deliberately does **not** assert that no request reached the stream: a surviving record
  cannot prove that, since `Sequence: 1` is itself a successful no-op, and there is no
  observation seam on `purgeStream`. The error is what discriminates. **Mutations:** drop
  the zero refusal; drop the `MaxUint64` guard so `upTo + 1` wraps.
- **Orphan-sweep arms**, against the existing fixtures in `orphan_qos2_sweep_test.go`.
  **Mutations:** pass `0` from the `default` arm — `TestOrphanSweepRetiresAnAgedReleasingClaim`
  must fail; pass `0` from the `claimMsg == nil` arm —
  `TestOrphanSweepFencesAnUnclaimedHoldBeforeDestroyingIt` must fail.
- **Identity**, in `TestQoS2RelClaimIdentityValidation`: the `Fence` pairings, plus a
  `TestQoS2ClaimVerdicts` row proving a fence-less marker classifies **unreadable** rather
  than as one covering everything. **Mutations:** accept a fence-less `discarded` marker;
  and accept a MAXIMAL one, which `TestQoS2DiscardRepairsAMaximalFenceMarker` must catch.
  A readable maximal fence is adopted by A2 and then refused by the purge, failing the
  clear on every adoption and sweep pass behind a marked record — a permanent client-ID
  lockout. The identity rule and the purge share one predicate, so a marker can never
  carry a fence its own purge would refuse.
- **`TestQoS2AwaitedReleaseDoesNotInheritAStaleClaimsEntitlement`** — the A3 race above.
  **Mutation:** restore the direct purge on observed absence.

### Designs that were refuted, so they are not re-proposed

- **The cutoff alone.** Leaves A2 and all of mechanism 2. This is why the A2 subtests are
  built to stay red against it.
- **Reap the marker when `holdSeq > claimSeq`** (the marker's own stream sequence). Passes
  the marker-first ordering and fails the hold-first one, where E2's hold lands before the
  marker is written so `holdSeq < claimSeq` while E2 is still none of the marker's business.
- **Reap the marker unconditionally.** Its persistence is the only durable proof that a late
  releaser of the retired incarnation must lose (scope rule 3).
- **`min(cur.Fence, upTo)` on A2.** Refuted above: a pre-existing marker need only predate
  this walk's `.rel` **load**, not its enumeration. It is also no defense against a
  corrupt-huge fence, where it degrades to exactly the behavior it was meant to prevent.
- **Scoping the claim subject by exchange generation.** The releaser knows the exchange
  generation; the payload-free discard enumeration does not, and
  `TestQoS2DiscardReadsNoPayloads` forbids adding a hold load there.
- **A durable WALK-WIDE cutoff, frozen at the first walk's watermark and inherited by every
  adopter.** Proposed by the holistic review (round 2) to make an interrupted walk's
  entitlement total rather than per-subject. It is **actively unsafe**, and the interleaving
  is worth keeping because the proposal is a natural one. T1 enumerates at 10, marks subject
  `S_a`, and crashes before reaching `S_b`. A peer vacates `S_b` and accepts E2 at 12,
  answering PUBREC. T2 adopts, reaches `S_b`, finds no marker, and takes arm A1 — but with
  the frozen cutoff it writes `Fence: 10` and purges at `Sequence: 11`, which removes
  nothing. T2 completes and `releaseTeardown` deletes the session record. E2's PUBREL then
  loads that marker, computes `holdSeq 12 > Fence 10`, and **reaps a marker that is seconds
  old**, wins a `releasing` claim, gets `Held` from `verifyQoS2HoldForRelease`, and fans out
  a message from a session that no longer exists. That is delivery-after-discard — the
  original defect this whole file exists to prevent. The cause is that the frozen cutoff
  EXCLUDES an occupancy the deciding walk's own enumeration covered: T2 enumerated E2 at 12
  and was entitled to it, but records a fence of 10, so it neither destroys E2 nor leaves
  a fence that describes it. The reap condition then cannot tell that stale-looking marker
  from a genuinely stale one. **A marker must record the entitlement of the enumeration
  that decided it**, which is what per-walk bounding does. (Note what this is NOT refuted
  on: the CAS landing during E2's occupancy is not what would have made E2 discardable.
  Discardability is coverage by the deciding watermark **and** the marker win, per the
  mapping above — the CAS win settles who acts, the coverage settles what may be acted on,
  and the timing of the CAS within the occupancy settles nothing.)

### Residuals

None of these is conditioned on a rollout, and none is a migration.

- A hold that appears in the enumerated `Subjects` map but sits **above** `LastSeq` — the
  non-snapshot gap, since `State` and `State.Subjects` are separate store reads. It is
  **spared** and left to the orphan sweep. Leak, not loss. The honest claim is "the purge
  is bounded to sequences at or below `E`", never "the teardown owns exactly what it
  enumerated".
- A **stranded post-enumeration tombstone**: an exchange completing between the enumeration
  and the purge leaves its tombstone above the cutoff, so the bounded purge spares it. Leak;
  the sweep's `claimMsg == nil` arm reaps it, and a next PUBLISH takes the existing
  reap-and-retry path.
- A **corrupt but readable fence** can over-purge up to its own value on the A2 arm. Loss,
  and requires record corruption — outside this finding's condition. Nothing here bounds a
  corrupt readable fence; the `MaxUint64` refusal only stops it degenerating into an
  unbounded purge.
- The **one-stream-incarnation precondition**, which applies to every arm alike and to every
  other sequence identity in this protocol.
- A refused watermark leaves a **marked, partially torn-down session**, which refuses that
  client ID's next CONNECT until an adoption obtains a usable watermark. Fail-closed and
  self-healing, and not reachable in contract.

**What this closes elsewhere, stated exactly.** The inbound QoS 2 hold walk is bounded **per
walk**, which `sess_teardown.go` previously described as unbounded. On a subject carrying
an earlier attempt's marker, a repeat purges to the fence **that** marker recorded, so it
destroys nothing that attempt was not entitled to destroy.

**Per walk is not "frozen at the first walk's watermark", and the difference is deliberate.**
A subject an interrupted walk never reached carries no marker and therefore no transferred
entitlement, so an adopter competes for it afresh at its own watermark. An occupant is
destroyed only where **both** conditions hold: its sequence is at or below that watermark,
and the adopter wins the marker. Then it is the arbitration working rather than destruction
escaping it — the occupant was inside the entitlement the winner enumerated, and its PUBREL
loses and is answered `0x92` with nothing delivered.

**Coverage is the criterion; the CAS win only settles who acts on it.** Winning the marker
during an occupancy is not on its own what makes that occupancy discardable, and this design
contains the counterexample: a hold above the walk's watermark, via the non-snapshot gap,
has its marker won during its occupancy and is SPARED, staying serviceable afterwards
(F-036, not loss). Mechanisms 1 and 2 were losses because destruction escaped coverage:
A1's marker was created while the subject was EMPTY and A2's belonged to an earlier
occupancy, and an unbounded purge then destroyed an occupant neither entitlement included.

**Scoped to that walk only.** The same teardown then purged `$MQTT_inflight` by filter with
no bound, over subjects a peer serving the retiring generation can re-occupy, so the deletion
set as a whole was not repeat-safe. That was **F-034**'s, not this finding's, and F-034 paid
it on 2026-08-22: the filter purge is now an enumerate-load-classify pass bounded by the
watermark its own enumeration returned, deleting only a record whose identity and subject
agree.

## F-063

**Title:** A source acknowledgement bound to a copy store that never proved retention

- Status: **Fixed, 2026-08-19**, with its stated guarantee **re-scoped** — see "What was
  done" and "The re-scope" below. Found by F-033's consequence audit, 2026-08-18.
- Classification: HA arbitration architecture / data loss
- Release treatment: **Alpha blocker.** The message lost was accepted — the publisher was
  answered for it — and multi-frontend HA is supported alpha scope.
- Roadmap section: §0.2 (multi-frontend HA blockers)

### The finding

*Stated as it was found, in the past tense. The citations here name FUNCTIONS rather than
line numbers, because the code they describe no longer exists at any line — see "What was
done".*

`$MQTT_out` is interest-retained: a store onto a subject no consumer filters **succeeds
and keeps nothing**. The affine protocol nevertheless treated a successful create-only copy
store as proof the copy existed, advanced the claim to `materialized` on it, and then
acknowledged the group source — which is irreversible and releases the message from the
shared group forever. Nothing between those two steps established that any consumer had
retained the copy.

**The premise, stated so it covers all three routes:** *nothing binds evidence that the
copy was RETAINED to the irreversible source acknowledgement that follows it.* The routes
differ in which link is missing, not in the premise:

- **Own-claim path — the evidence was never gathered at all.** `beginQ2AffOp` admits a move
  on LOCAL session state only: a readiness flag, a non-empty generation and a non-nil
  `q2AffConsumer` POINTER. Nothing consults the durable consumer, and that is still true —
  admission is not evidence. `materializeQ2AffCopy` then stored and did not verify: the only
  re-read was on the CAS-REJECTION path, so a store that SUCCEEDED was never checked, and
  `completeQ2AffOwnClaim` acknowledged the source on the strength of it.
- **On-behalf path — the evidence was gathered and then not bound.** The contender DOES look
  the consumer up in `materializeQ2AffOnBehalf` step (2), but nothing tied that lookup to
  the store that followed it; a deletion in between produced the identical leftover.
- **The materialized observation — evidence about a copy that had since gone.** A consumer
  deleted between observing `materialized` and acknowledging the source — in the mover's
  foreign-materialized arm, in `completeQ2AffOwnClaim` and in `materializeQ2AffOnBehalf` —
  left the same state, with no missing consumer at the moment of the check.

**Leftover in every case: a `materialized + SourceAcked` claim with no copy**, and only a
later owner resume can turn into `Lost` — and if that session never resumes, the loss is
never even recorded. The source is gone, so no redelivery re-elects.

**Why an owner-independent reconciler does not close this**, and why it is therefore not
F-064: a periodic reconciler at F-064's two gates cannot make consumer existence atomic
with the copy store and the source acknowledgement. The remedy here is arbitration on that
ordering — proving retention before acknowledging, or making the acknowledgement
conditional on it — not reconciliation afterwards. That difference in remedy is what
establishes the premises are distinct.

### What was done

Three changes, in `qos2_shared_affine.go`. No new durable field, no new claim state, no new
protocol, and nothing conditioned on a rollout.

1. **The proof is a presence check, and both store outcomes converge on it.** A successful
   store is now verified exactly as a rejected create-only CAS already was. That closes the
   own-claim and on-behalf routes **together**, because both stores funnel through
   `materializeQ2AffCopy` — the own path via `completeQ2AffOwnClaim` and the contender via
   `materializeQ2AffOnBehalf`.
2. **An already-`materialized` claim is re-verified, and never re-stored.** Re-verified
   because the phase records that a copy was proven at an earlier instant, not that one is
   there now. Never re-stored because an absent copy under it is ambiguous — consumed by its
   own delivery, or dropped with its consumer — and a replacement would be a copy no
   delivery may publish. Deleting the phase guard outright, which an earlier revision of the
   plan proposed, would have re-stored a copy for `materialized ∧ AffineDone ∧ copy absent`,
   since `materializeQ2AffCopy` tests only `Discarded`. That is a defect review caught
   before any code existed; `TestQ2AffMaterializedAffineDoneIsNotRestored` pins it.
3. **The release is authorized by `sourceAckAuthorized`**, on
   `copyPresent ∨ AffineDone ∨ Discarded ∨ Lost ∨ SourceAcked` and nothing else, evaluated
   on FRESH sequential reads taken by the decision that acts on them. `Discarded` is
   *required*, not merely permitted: `reapable` refuses without `SourceAcked`, so gating it
   would make every discarded claim permanently unreapable — F-064's accumulation half,
   manufactured. `Lost` is kept for the same kind of reason: a lost claim still releases the
   group delivery, and omitting it would over-refuse.

**Both callers were restructured, and that was not optional.** They returned on a failed
materialization *before* reaching their terminal-fact handling, so once an
already-materialized absent copy made materialization return false, `SourceAcked`,
`AffineDone`, `Discarded` and `Lost` would have become unreachable for exactly the claims
that need them — and the group delivery would be re-offered forever with nothing able to
release it. Each caller now re-reads the record (its snapshot predates every JetStream round
trip above it) and proceeds only if that fresh record authorizes the ack.

The contender's materialized arm does not route through `materializeQ2AffCopy`, so it
takes its own decision-point copy read and consults the same predicate. That is the third
route, and it is where the fix would otherwise have been missing entirely.

Where nothing is authorized the move takes the function's existing indeterminate outcome:
prove nothing, write nothing, ack nothing. **Refusal is F-063's disposition; F-064's
owner-independent reconciler is the resolution** of the nonterminal claim it strands, which
is F-064's premise verbatim. A note F-064 must inherit: assigning that resolution to F-064
does **not** license a naive `Lost` CAS there — see the refuted designs below.

### The re-scope

**The dossier's premise as filed cannot be met by any finite check, and the repository
owner decided explicitly to tick the item with the guarantee re-scoped** from "bound" to
"conditioned on a fresh but non-atomic observation".

The claim lives on `$MQTT_qos2assign` and the copy on `$MQTT_out`, so a claim read and
a copy read are separate JetStream round trips with no atomic snapshot across them; and
JetStream's `CONSUMER.DELETE` carries no precondition field at all, which F-033's audit had
already established as *the* unconditioned operation. So a proven copy can be gone by
the time `confirmAck` commits. The span between the two contains a claim LOAD on every
route through `materializeQ2AffCopy` — `advanceQ2AffClaim` loads before it can find the
phase unchanged — and on a fresh or on-behalf materialization the full load-and-CAS with a
three-attempt budget. Only the contender's foreign-materialized arm, which never enters
that function, has the ack alone. What the change does is replace an acknowledgement
resting on a store
response — or on evidence gathered before the store, or on the phase alone — with one
resting on a fresh proof; it does not make that span zero, and calling it "one round trip"
was an overclaim the holistic review caught. `docs/CONFORMANCE.md` states the residual with
a worked interleaving that includes the phase transition.

### Designs refuted in review, so none returns

- **Vary the copy's `Nats-Msg-Id` per attempt.** A stale materializer with a different
  delivery count stores a SECOND copy, because expect-0 succeeds on a now-empty subject: the
  create-only CAS does not remember historical occupancy. If the claim has since been
  reaped, that copy NAKs forever. The delivery count is not an attempt identity either —
  recreating a consumer resets it.
- **Write `Lost` owner-independently when the copy is absent.** Prescribed by one review
  round and refuted by the next. `advanceQ2AffClaim` arbitrates claim-record writers only,
  so a stale `claimed`-snapshot on-behalf materializer can store a copy between the
  observer's absent-copy read and the `Lost` CAS; the CAS then sees a nonterminal
  materialized claim and records `Lost` although a copy now exists. `Lost` is terminal and
  never reapable, so the mistake is permanent, and the delivery gate then disposes of the
  copy under its terminal branch. A conditional mutate closure can order this against
  `Discarded` and `AffineDone` but **cannot** validate a fact on the other stream. Salvaging
  it needs a real materializer fence, which is cross-stream coordination belonging to
  F-064's reconciler and not to this ack gate.
- **Check the consumer exists before storing.** The on-behalf path already did, and lost
  anyway. That is the route this finding filed as "gathered and then not bound".
- **Hold the source until the client PUBCOMPs.** Makes a never-returning winner hold the
  source forever, manufacturing F-064's stall as the normal path.
- **Give `$MQTT_out` limits retention.** Foreclosed: interest retention is how a discarded
  session's undelivered copies disappear with its durable.
- **Refuse, and call the finding closed on its filed premise.** Refuted twice, which is what
  forced the re-scope above to be an explicit decision rather than a claim this work makes
  for itself.

### What pins it

One fixture per route, each naming *its* route rather than "the finding is fixed", in
`qos2_shared_affine_retention_test.go` unless stated:

| Route or rule | Fixture |
|---|---|
| Own-claim: a store that kept nothing | `TestQ2AffOwnClaimRefusesSourceAckWhenTheStoreKeptNothing` (lifecycle file) |
| On-behalf: consumer deleted after the lookup | `TestQ2AffOnBehalfRefusesSourceAckWhenConsumerVanishesAfterLookup` |
| The materialized observation | `TestQ2AffMaterializedNoCopyRefusesSourceAck` |
| Present-now, not present-once | `TestQ2AffMaterializedCopyIsVerifiedNotAssumed` |
| No replacement copy for a completed exchange | `TestQ2AffMaterializedAffineDoneIsNotRestored` |
| Predicate disjuncts | `TestQ2AffAcksOnCopyPresent`, `…OnAffineDone`, `…OnDiscarded`, `…OnLost` |
| Caller restructure, own vs on-behalf | `TestQ2AffOwnClaimAcksOnTerminalFactWhenCopyGone`, `TestQ2AffOnBehalfAcksOnTerminalFactWhenCopyGone` |
| Fail closed on an unreadable copy | `TestQ2AffCopyLoadErrorRefusesSourceAck` |
| The dependency behaviors the retry bound rests on | `TestQ2AffCopyStoreWithNoInterestSequenceAndDedupe` |
| Cross-broker: no broker releases it | `TestQ2AffClusterCopyLossOnDeletedConsumer` (cluster file) |

The F-064 pin (`TestQ2AffLiveRecordLeavesAStrandedClaimToOwnerResume`) and the F-065 pin
(`TestQ2AffReapUsesAStaleCopyReading`) must keep reproducing their findings. They are
regression guards and **not** mutation targets: F-064's runs only sweeps and F-065's calls
`reapQ2AffClaim` directly, so neither executes this change's decision.

**Two pairings differ from the plan, and both are recorded rather than dropped.**

- `TestQ2AffAcksOnCopyPresent` covers the CONTENDER route only, not both. On the own-claim
  route a *successful* materialization goes straight to the ack without consulting the
  predicate, so `copyPresent`'s only decision point is the contender's materialized arm and
  an own-route fixture could not fail the mutation that drops it.
- The plan's separate `TestQ2AffOwnClaimAcksOnTerminalFactWhenCopyGone` would have duplicated
  the own-route `AcksOn…` staging, so it was given the one interleaving those do not have:
  the terminal fact arrives DURING the copy store, which is what the caller's re-read exists
  for. It pins the re-read as well as the early return.

### Two sites named in review that are NOT members of the corrected comment class

Both assert that the on-behalf match arm materializes a copy and acknowledges the group
source, which stays true — the ack now additionally carries a proof, and the hazard being
described (acknowledging irreversibly for a session that can never resume) is unchanged.
Verified against the tree rather than dropped silently: `materializeQ2AffOnBehalf`'s
record-validation comment, and its mirror in `docs/ARCHITECTURE.md` under "Compatibility
boundary". Neither states that the phase implies a present copy, nor that the ack follows
from materialization.

## F-064

**Title:** A live generation reference leaves every assignment of that generation to owner
resume

- Status: **Partly remedied, still open, and parked outside the alpha since 2026-09-02.**
  Found by F-033's consequence audit, 2026-08-18. The scope decision preserves the
  implementation and this evidence; it does not declare the finding fixed.
- Classification: HA arbitration architecture / liveness
- Release treatment: **Experimental shared-QoS-2 work, post-alpha / maybe someday.** The
  stall and accumulation are gathered under ROADMAP chapter P and do not block supported
  shared QoS 0/1 or ordinary QoS 2.
- Roadmap section: chapter P (historical §0.2 record retained)

### What landed, and what it deliberately does not touch

**On-behalf restoration of a destroyed affine consumer.** A contender taking the group
redelivery rebuilds the winner's durable from its record and materializes onto it, so
consequence 1 is closed **for assignments that never reached `materialized`** — without
the owner, and without going through the sweep at all. Neither gate below is addressed,
and neither needs to be: the redelivery carries the payload, which is the thing the sweep
does not have.

The mechanism, its refusals and its residuals are in `docs/ARCHITECTURE.md`
("On-behalf restoration of a destroyed affine consumer") and `docs/CONFORMANCE.md`.

**Three things did not land, and each is recorded so it is not rediscovered as an
oversight.**

1. *The stall for a `materialized` assignment whose copy is gone.* That population takes a
   different dispatcher arm and still waits for its owner.
2. *The accumulation, consequence 2.* **Partly closed 2026-08-21.** Under a live
   reference the sweep now reaps claims that are already durably TERMINAL, and collects
   the retired affine `$MQTT_inflight` register class together with the copy and the
   claim each register names. Two of the three windows are closed. The third — a
   NONTERMINAL claim whose source is gone — stays owner-only, and the reason is the new
   refuted design above rather than unfinished work.
3. *The `Lost`-truthfulness defect.* Its own roadmap bullet.

### Refuted designs, recorded so none returns

- **A sweep-side `Lost` writer.** The sweep has no payload, so it can only resolve a claim
  by asserting a fact it cannot prove.
- **A reconciler-side sourceless settle of a NONTERMINAL claim**, by any actor whose
  concurrent set can include a dispatched materializer. This is the remedy F-064's own
  roadmap bullet prescribed for the accumulation half, and it is unsound. Worked, and
  measured against the code rather than argued: (1) the pass proves the source
  authoritatively gone and the copy absent; (2) a materializer dispatched earlier, holding
  the payload, STORES the copy — `materializeQ2AffCopy` stores before its own phase CAS,
  so the record is still `claimed`; (3) the pass's own fresh read therefore sees `claimed`
  and commits `Discarded ∧ SourceAcked`; (4) the materializer's phase advance merges
  `Discarded` in, because `mergeProven` ORs it and never clears it; (5)
  `q2AffCopyIsDisowned` reads a TERMINAL claim of this very incarnation and
  `withdrawQ2AffCopy` deletes the copy UNPUBLISHED. The session record is live by
  construction in this arm, so that disposal falsifies the `[MQTT-4.8.2-5]` premise
  `q2AffCopyIsDisowned` itself states, and the write is the "discard on a live record"
  this list already refutes.
  **The guard the bullet proposed does not save it.** An in-`mutate` "still
  nonmaterialized" check cannot veto anything: `advanceQ2AffClaim` calls `mergeProven`
  after `mutate` unconditionally, and the phase is `claimed` at the harmful step anyway.
  **And no positive fact licenses the write:** source absence does not revoke an
  already-dispatched payload, copy absence is an instantaneous observation, a `claimed`
  phase does not prove no store is in flight, a live record supplies no quiescence, and
  there is neither a cross-stream transaction nor any bound on how long a materializer may
  be in flight. **Nor is there a truthful terminal fact to write instead:** `AffineDone`
  asserts a delivery that never happened, `Lost` is unprovable without the payload and is
  never reapable, and `SourceAcked` alone leaves the claim nonterminal so `reapable`
  still refuses it. The rule therefore has exactly ONE caller — `recoverQ2AffClaims`, on
  the owner's own resume, where it predates this finding — and is hardened there rather
  than presented as fixed.
- **Discard on a live record.** The rationale is **two-part**, and the blanket version an
  earlier draft carried overclaimed. Where nothing proves PUBREC, writing `Discarded`
  deprives a client still owed the PUBLISH and is non-conformant under `[MQTT-4.8.2-4]`.
  Where a correlated PUBREL **does** prove PUBREC, the PUBREL is persisted independently
  and replayed, so the handshake completes and the client is not deprived — there it is
  untruthful rather than non-conformant.
- **Re-storing a copy for a `materialized` claim.** Refuted twice, in successive review
  rounds, and the second time unpatchable from the claim side: the copy CAS and the claim
  CAS are on different streams, so no claim-side fence can order a contender's store
  against the winner's authorization of the original PUBLISH. The replacement copy can
  become a second PUBLISH for an exchange already past PUBREC. This is the same
  cross-stream non-atomicity that refuted the owner-independent `Lost` writer in F-063.
- **An enumerated field predicate for replayability.** Five successive lists were each
  falsified by the next review round — `MaxDeliver`, `HeadersOnly`, `DeliverGroup`,
  `PauseUntil`, `FlowControl`+`Heartbeat` — and `MaxDeliver` alone reopens the stall. A
  list cannot close a struct a dependency bump can extend; whole-struct equality against a
  single constructor can.
- **Refusing an unconvergeable durable on the OWNER's path too.** Sound in isolation, but
  a refusal costs a live connection there: a CONNECT-time restoration failure is silently
  skipped because there is no SUBACK on that path, and the only remedy that removes the
  silence closes the connection, publishing the Will and, at a zero expiry interval,
  deleting the session. Confined to the contender instead.
- **Owner-side storage ADOPTION.** Rejected in favour of a post-failure recovery: adoption
  is not monotonic, because it needs a lookup that can fail and because adopting one value
  can meet a durable recreated with another, refused as immutable where today's
  local-option create would have succeeded.
- **A preflight lookup on the owner's path.** Turns a failing lookup into a failed
  provision. The recovery cannot, because it runs only after a create that already failed.

The caller analysis from the rounds that produced those last three is **overtaken by the
contender-only decision** rather than dropped; it is why the decision is worded as
monotonicity — the owner's failure set may shrink and must never grow.

### The finding

`buildSessionRefs` keys `refs.affine` on `ps.Q2AffGen` **alone**
(`orphan_qos2_sweep.go:884`), and `reconcileOrphanQ2AffClaims` skips every claim of a
referenced generation (`:1765`). (`deleteOrphanQ2AffConsumers` skipped the generation for
the same reason; F-036 course E removed that function, so the reference skip now has one
consumer rather than two — which changes nothing about this finding, since the skip itself
is untouched.) Under a live, matching record the sweep therefore touches **no claim of
that generation at all** — deliberately, because the owner is presumed to be coming back.
The premise that makes that safe is **"a live generation reference makes owner resume a
sufficient reconciler"**, and `recoverQ2AffClaims` is indeed the only thing that ever
revisits such a claim. A session that never resumes — and MQTT permits a session that never
expires — is the counterexample.

**Consequence 1 — a permanently stalled shared group.** This is the consequence the original
roadmap bullet named. Record live and matching, affine consumer authoritatively absent,
owner never resumes:

- every contender's `materializeQ2AffOnBehalf` used to fail at the consumer lookup,
  forever, acknowledging nothing — this is the half that landed;
- `discardAbsentWinnerClaim` defers because the generation matches
  (`orphan_qos2_sweep.go:1855`);
- `reconcileOrphanQ2AffClaims` never reaches the claim at all;
- `Lost` has exactly one writer in the tree, `recoverQ2AffClaims`
  (`qos2_shared_affine.go:2237`), on the owner's own resume.

The shared group consumer sets **no `MaxDeliver`** (`mqtt.go:15925-15937`), so AckWait
re-offers that source message forever while it holds a `MaxAckPending` slot, with nothing
logged. Enough of them and the shared subscription stops delivering for every member.
Reachable through the out-of-band consumer destruction `docs/CONFORMANCE.md` documents, and
through F-036's third manifestation below.

**Consequence 2 — unbounded accumulation of settled records (residual).** Three windows
leave a claim that nothing under a live reference revisits. The reap is the LAST step of
`mqttFinishQ2AffineDelivery` (`:1926-1942`) and is skipped both by a crash and by a copy ack
whose confirmation response is merely lost — recovery's own comment names both.
`confirmSourceAckAndRecord` can likewise commit its ack while the response is lost,
returning before the terminal contender's reap (`:1522-1525`). And W1 can commit with the
copy store then failing, so a later redelivery finds the Message Expiry Interval elapsed and
bare-acks the source **before** consulting or electing any claim (`:1412`, `:1445-1448`),
leaving `claimed`, `SourceAcked == false`, no copy, and the source genuinely gone. `Lost`
records are retained by design and are not this.

**The premise is enforced TWICE, and that is measured rather than assumed.** The reference
skip is not the only gate: `discardAbsentWinnerClaim` independently defers on
`ps.Q2AffGen == e.claim.Gen`. The fixture below was run against each mutation separately and
**passed both times**, failing only when both gates were removed. **A remedy that addresses
one of them changes nothing** — which is the trap this note exists to prevent.

**Why the silent loss is NOT here.** An earlier draft of this audit grouped the
store-without-retention loss under this premise. It does not belong: no owner-independent
reconciler can make consumer existence atomic with the copy store and the source
acknowledgement, so the remedy that closes the stall leaves the loss untouched. That is
F-063, and the remedy test is what separates them.

## F-065

**Title:** A claim reaped on a copy-absence reading taken before its own SourceAcked transition

- Status: **Fixed 2026-08-20**, with the stated guarantee RE-SCOPED (the same shape as
  F-063's). The re-scope is what the tick rests on and is put to the repository owner at
  the commit gate; declining it lands the mechanism and leaves this finding open. Found by
  F-033's consequence audit, 2026-08-18.
- Classification: HA arbitration architecture / liveness
- Release treatment: **Alpha blocker.** A copy of an accepted QoS 2 message is pinned behind
  the fail-closed delivery gate and holds a `MaxAckPending` slot for as long as it lasts.
- Roadmap section: §0.2 (multi-frontend HA blockers)

### The finding

`reapQ2AffClaim` reads the copy state (`qos2_shared_affine.go:1360`), may then advance
`SourceAcked` (`:1367`) — a full JetStream round trip that loads, mutates, CASes and retries
— and evaluates `reapable(copyAbsent)` on **the reading taken before it**. The predicate's
whole purpose is that a copy which still exists keeps its claim; it is handed a copy state
that may be that stale.

**Worked interleaving.**

1. An **on-behalf** contender loads claim `C`, not yet `Discarded`, passes
   `materializeQ2AffCopy`'s guard and reads the copy ABSENT.
2. Concurrently `C` becomes `Discarded` — by teardown, by the sweep, or by the on-behalf
   discard arm — **and** its source side is proven, which the reap predicate requires before
   anything is deletable: either by a terminal contender or by the reaper's own
   unofferable-source inference.
3. The reaper finds the copy absent, observes `SourceAcked`, `reapable(true)` returns true
on
   `Discarded`, and it deletes `C` at its exact sequence.
4. The contender now stores the copy. Its `advanceQ2AffClaim` finds no claim and returns
   `(nil, 0, nil)`, so materialization reports failure — but the copy is already on the
   stream.

**Leftover: source proven, claim absent, copy present.** The delivery gate takes the
`claim == nil` branch and NAKs (`:1816-1822`), never reaching the `Discarded` branch that
would dispose of it, so the copy NAK-loops. That is precisely the state `reapable`'s own
comment says must never exist: deleting the claim there "would strand the copy behind the
delivery gate, NAK'd forever with nothing left able to authorize or dispose of it".

**The stale materializer must be the on-behalf one.** `prepareQ2AffineUnwind` drains every
locally admitted move before discarding, so the owner's own materializer cannot still be in
that window — but a contender materializing on another broker is not inside that session
fence.

**How long it lasts.** On the ordinary teardown route the same teardown next deletes the
affine consumer (`sess_teardown.go:749-753`) and interest retention takes the stranded copy
with it. A strand that **persists across the failure** needs one of two further facts: the
consumer delete fails or the broker dies before it; or the dispose-first packet unwind
discards the claims and its confirming save then fails DEFINITELY, so
`restoreQ2AffineUnwind` puts the live path back under a session that keeps running
(`mqtt.go:7504-7512`) — and that restore leaves `q2AffReady = false` while the delivery
callback never tests readiness, so the copy still NAK-loops on the retained consumer. In the
first construction a later successful orphan pass could once delete the consumer, which
bounded the strand. **F-036 course E removed that pass's consumer delete, so the strand is
now PERMANENT rather than bounded** — it is part of the accepted correlated leak in
`docs/CONFORMANCE.md`, and this is the sharpest instance of what that removal costs.

**Not closed by the existing guard.** `materializeQ2AffCopy`'s `Discarded` check reads the
caller's **already-loaded** claim, which predates the concurrent transition.

### What the review established: THREE routes, not one

Seven plan-review rounds turned one finding into three, and a remedy that closes only
the first leaves the finding open.

- **Route A, the ordering.** The interleaving above: the copy reading predates the
  reaper's own `SourceAcked` transition.
- **Route B, a stale snapshot with no reaper race at all.** A materializer holding a
  `claimed` snapshot stores its copy after the claim has completed and been reaped —
  `materializeQ2AffCopy`'s only phase guard tests the CALLER'S snapshot — and its
  `advanceQ2AffClaim` then finds `next == *cur` and returns **success without writing**,
  so the claim's sequence is unchanged and the reaper's delete lands. Any remedy keyed on
  "my CAS failed" misses this entirely.
- **Route C, a replacement incarnation.** A group delivery callback dispatched before the
  source ack was proven is still running and unconditionally attempts the create-only
  election, so after the reap it wins the subject again for the SAME winner and
  generation. `DeliverNew` on the group durable rules out FUTURE redeliveries but cannot
  cancel a callback already in flight. Before the fix both incarnations derived one copy
  subject, so the replacement adopted the copy a superseded materializer was about to
  withdraw, and that materializer's advance — which asserted only `Winner+Gen` — mutated
  the REPLACEMENT's record, manufacturing F-064's residual-8 stall.

### The remedy as built

1. **The reaper's copy reading is no older than its own last write**, and nothing but the
   delete follows it.
2. **The claim carries an incarnation `Nonce`**, minted at the election and never
   rewritten. It scopes the copy subject and the copy's `Nats-Msg-Id`, and it fences the
   four incarnation-specific transitions — the `claimed → materialized` phase,
   `AffineDone`, `Lost`, and the source-gone `Discarded` — inside `advanceQ2AffClaim`,
   checked after the load and BEFORE the mutate, because deciding afterwards is too late:
   the write has already moved the successor's record. `SourceAcked` (a fact about the
   source message) and the teardown discards (facts about the session) opt out.
3. **A materialization attempt withdraws the exact copy sequence it proved present** once
   it proves, on an authoritative reading, that no claim of that copy's incarnation
   remains — no claim, a different winner/generation/incarnation, or a terminal claim of
   its own incarnation. Every store outcome converges on the presence proof, so a
   landed-but-unreported store is resolved rather than abandoned, and the withdrawn
   sequence comes from that proof rather than from a store response.
4. **The delivery gate disposes a copy whose live claim names a different copy subject.**
   A positive fact about a record just read — unlike the `claim == nil` arm, which still
   fails closed, because an absence is not a fact.
5. **A per-claim operational validator** refuses an unattributable record on the six
   direct decision paths, while `mqttQ2AffClaimIdentityOK` stays exactly as it is: it is
   the account-wide attribution floor, and tightening it would abort an unrelated
   session's teardown and lock a client ID out of its next CONNECT.
6. **The claim record is size-bounded**, in the admission calculation (as another max
   among independently stored forms) and in the runtime backstop before the election,
   over every reachable terminal shape — a QoS 1/2 Will bypasses admission entirely.
7. **`DenyDelete` on `$MQTT_out` is fatal at startup**, since this owner now deletes
   there.
8. **The carriers hold the copy SUBJECT, and three independent checks bind it.** The
   pending entry and the in-flight register are reached holding a packet identifier and
   nothing else, so they carry `(claim subject, copy subject)`. A bare incarnation — the
   first form built here — is not enough: it says which incarnation a claim reference
   belongs to but never which COPY, so a carrier naming another live claim's subject with
   that claim's own valid incarnation is internally consistent and still about a different
   delivery, and crediting it records a completion on a claim that delivered nothing,
   whose copy the gate's terminal branch then disposes of. The three checks are the
   record's own shape (a carried copy yielding no incarnation is refused, because `_EMPTY_`
   is the fence's opt-out); the claim naming THIS copy, in the single helper that turns a
   carried pair back into a claim; and, on recovery, the register's own SOURCE — for an
   affine delivery the source IS the copy, so the pair is installed only where the source
   is LIVE and its stored subject IS the carried copy. Every other state leaves the pair
   unproven and the register is reserved rather than interpreted, with allocation withheld
   and the reservation marked so no terminal path settles the record.

   Two narrowings were forced by holistic review and both are load-bearing. A mismatch is
   a SUPERSEDED INCARNATION — the only mismatch that licenses disposing of a copy — only
   when the carried copy is a copy of THAT assignment under another incarnation; a copy
   of a different assignment is refused, because the claim that owns it may be live. And an
   unproven register joins the DUPLICATE-SOURCE GROUPING as an unowned member rather than
   being set aside, so it poisons every sibling of its source: setting it aside turned a
   two-member group into a one-member one, and the sibling was then installed as an
   authoritative singleton with a `cpending` mapping the source's redelivery follows,
   re-sending the payload under a second identifier.
9. **The superseded-incarnation disposal is ONE predicate, and every other mismatch is
   refused.** Two paths dispose of a copy on a claim/copy mismatch — the retired settlement
   and the delivery gate — and review caught each of them, a round apart, disposing on bare
   subject inequality. The gate is the worse of the two because it takes its claim subject
   from the copy's own HEADER: a copy persisted on assignment A's subject whose header names
   a valid, attributable assignment B passes every other check, and acknowledging it away
   loses A's message and strands A's claim without `AffineDone`. Only a copy of the same
   assignment under another incarnation is unauthorizable by construction, so that is the
   predicate (`q2AffCopyIsSupersededIncarnationOf`), shared by both paths; a
   cross-assignment mismatch is a shape this broker does not write and is refused rather
   than interpreted.
10. **Every refusal on this path is PACED.** A plain NAK is re-offered immediately, so the
   fail-closed refusals the rest of this remedy adds turned a persistent refusal into a
   request storm — measured at 9214 deliveries in ten seconds for one copy under an absent
   claim, each a claim load and a log line. That state is not forgery-only: this document's
   own residuals include three windows in which a copy legitimately outlives its claim, and
   an unavailable backend produces the same shape for as long as it lasts. Refusals now NAK
   with a delay that doubles from 100 ms to a 30-second cap, derived from JetStream's own
   delivery count so that no per-copy state has to be kept, evicted, or survive a restart.
   The first retry stays fast, which the ordinary phase-advance interleaving needs.

### The re-scope, which the tick rests on

The withdrawal is a **compensation, not an arbitration**: the claim and the copy are on
separate streams and JetStream's atomic batches are per-stream, so no claim-side state
can arbitrate a copy-side store. A durable arbitration would additionally need a bound on
how long a materializer may remain in flight, and nothing supplies one — a tombstone that
never expires is unbounded accumulation, and one that expires on a timer is a dedup window
wearing an arbitration hat. The three surviving routes and their three bounds are stated
in `docs/CONFORMANCE.md`.

### Designs refuted in review, so none returns

- **A `storing` phase the reaper must respect.** The actor that would clear the marker is
  the one that died, so a crashed materializer leaves a terminal claim behind an
  unclearable marker — the accumulation defect, manufactured.
- **Disposing any copy whose claim is absent, at the gate.** An absence never licenses
  destroying an accepted message. A missing claim record USED TO be producible by a
  SUPPORTED operation — recreating `$MQTT_qos2assign` alone — and F-066's enforcement,
  landed 2026-09-01, removed that producer entirely rather than narrowing it: recreating
  one stream is refused at startup, and the supported reset takes all nine, so it clears
  the `$MQTT_out` copy along with the claim and cannot leave the absent-claim state this
  arm reasoned about at all.
  **The refutation does not rest on that producer**, which is why it survives its
  removal: the rule is that an absence is not a positive fact, and a claim record is also
  absent to a reader whose load was indeterminate or whose stream reread raced a delete.
  Do not read the removed producer as licence to revive this design.
- **A reaper-side cleanup.** Its evidence is "absent when I looked, present now" — a
  second stale reading that says nothing about which claim may still authorize the copy.
- **Withdrawing on "my CAS reported the claim gone or renamed"**, and the same rule
  extended with "or returned a terminal claim". Refuted on the same seam twice: the first
  misses route B, the second loses route C, because with a shared copy subject a
  replacement adopts exactly the copy the withdrawal is about to delete.
- **Checking the incarnation AFTER `advanceQ2AffClaim` returns.** Too late: that function
  authorized mutation on `Winner+Gen` alone, so the stale actor's advance had already
  written to the replacement's record.
- **Carrying a bare incarnation on the pending entry and the register.** Built first, then
  refuted: an incarnation identifies which incarnation of a claim a reference means, never
  which copy the reference is about, so a carrier naming a foreign claim and that claim's
  own incarnation passes every check and is still about a different delivery. The carriers
  hold the copy subject instead, and the register's own source is what decides when the
  two disagree.
- **Binding the carried pair to its source by CONTRADICTION rather than by proof.** Built
  first, then refuted: refusing only a LIVE source that names a different copy left an
  indeterminate consumer lookup, an indeterminate message read and an authoritatively absent
  consumer all admitting an internally consistent forgery whole. "An absence is not evidence
  of a forgery" is true and beside the point — nothing licenses installing an authoritative
  pair on the strength of a source that was never read. The cost of the positive form is
  that a transient read failure reserves a legitimate register and withholds that session's
  new allocation until a later resume reads cleanly, which is the treatment an unaccountable
  identifier space already gets.
- **Setting an unproven register aside before the duplicate-source grouping.** Also built
  first: it reserved the identifier but removed the record from arbitration, so a genuine
  sibling naming the same delivery became an authoritative singleton with a source mapping,
  and the redelivery re-sent the payload under a second identifier. An unproven pair is not
  proof the identifier was never transmitted, so the record has to poison its group.
- **Disposing of a copy on bare claim/copy subject inequality.** Written twice, in the two
  places that dispose, and refuted both times: inequality does not say the copy is
  unauthorizable, only that this claim does not name it. The cross-assignment case has an
  owner that may be live, so the disposal is a message loss with a stranded claim behind it.
  Named-sites are a sample: the second instance was found only because the first was fixed
  without sweeping the family.
- **Refusing with a plain, immediate NAK.** What every refusal on this path did before
  round 16 measured it. The refusal itself is right; re-offering the copy at once is what
  turns a persistent refusal into a storm against JetStream, and a bound that needed a
  per-copy retry map would have been a new accumulating mechanism. JetStream's own delivery
  count already carries the state.
- **Treating every carried-copy mismatch as a superseded incarnation.** The transition
  refuses either way, so this looked safe — but the retirement the PUBREC leaves behind
  outlives the transition, and on the next redelivery it confirm-acked the copy IN HAND,
  which for a carrier naming a foreign claim is the answering delivery's own copy. The claim
  that owned it never recorded `AffineDone`: its only copy is gone and its message can never
  be delivered. The mismatch must be qualified by assignment identity.

### What this does NOT close

**A rejected delivery whose affine completion cannot be written.** The affine PUBREC arm
is selected by the claim subject alone, so it also sees a NEGATIVE PUBREC — and that path
creates no `pendingPubRel` entry and stores no PUBREL, so there is no PUBCOMP and
therefore no event at which an unlanded durable retirement mark could ever be retried.
The retirement machinery is deliberately gated on a SUCCESSFUL PUBREC for that reason,
and a rejected delivery keeps its pre-F-065 behaviour: complete the affine side if it can
be completed, then release the register. While the `AffineDone` write is failing the copy
stays unacknowledged and AckWait re-offers it, so the client sees the payload again —
which sits in tension with the broker's own rule that a rejected message must not be
redelivered (`mqtt.go`, at the failed-PUBREC ack). That behaviour PREDATES F-065 and
converges as soon as the claim store heals. It is named here rather than restored
silently, and deliberately NOT recorded as a residual of this change, because this change
neither introduced nor widened it.

**The accumulation this change ADDS, and where it is owned.** On the successful-PUBREC
path a failed `AffineDone` leaves the copy unacknowledged and durably retires its
register. A client that completes PUBCOMP, disconnects before AckWait redelivery and keeps
a non-expiring session that never resumes supplies none of the events that would settle it
— another affine-copy callback, recovery on resume, teardown, or operator action — and the
live session record makes orphan reconciliation preserve the claim through
`refs.hasAffine(idHash, ps.Q2AffGen)` and the retired register through the SEPARATE
`refs.hasInbound(idHash, ps.Gen)`. Before F-065 this path already accumulated the
copy-and-claim pair and deleted the register unconditionally, so the delta is one retired
register per such exchange: unbounded in count, bounded in kind. F-064's accumulation half
owns collecting all three, and its roadmap bullet now names the register class and the
different key, so the remedy cannot be written for claims alone. Stated in
`docs/CONFORMANCE.md`.

One precision, because the absolute form of the claim is false: register deletion is
asynchronous and ignores failure (R5), so a crash before the queued delete lands can still
leave a stale register transiently. What this change establishes is the narrower statement
— the affine branch no longer deliberately RETAINS or REWRITES such a register on the
rejected path — not that the state is unreachable.

F-064's residual 8 and its accumulation half, F-036, and F-066 — whose flagship row is
still reproduced by its own fixture, because the collision there is on the CLAIM subject,
which carries no incarnation. The incarnation does narrow one F-066 consequence
incidentally: a stale claim on a reused `$MQTT_msgs` sequence can no longer make a new
message deliver an OLD payload, because the new incarnation's copy subject differs.

## F-066

**Title:** Cross-stream identity coupling exists as three ad hoc instances, never as a mechanism

- Status: **CLOSED 2026-09-01.** Found by F-033's consequence audit, 2026-08-18, during
  holistic review. The decision this finding was blocked on was taken by the repository
  owner on **2026-08-31**: change the supported recovery contract so **all nine
  broker-owned streams become one recreation unit**, rather than generalising the epoch
  coupling. That enforcement is now in the tree — see "What landed, 2026-09-01" below —
  so recreating a proper subset is refused at startup and every validator's recovery
  text names the bundle. The by-construction route stays abandoned.
- Classification: HA arbitration architecture / data loss
- Release treatment: **Alpha blocker, resolved.** Every precondition was a recovery
  procedure this broker's own startup error told the operator to perform, and the
  messages lost or misdelivered were accepted. The procedure that reached every
  precondition is now an unsupported, refused operation.
- Roadmap section: §0.2 (multi-frontend HA blockers)

### The finding

**Falsified premise: "recreating one stream is a self-contained recovery procedure."** When
a broker-owned stream fails its shape validation, startup refuses and instructs the operator
to *"stop ALL brokers for this account and recreate `<that stream>`"*. For two streams that
instruction names a second stream and calls the pair *"one logical unit"*; for the rest it
names one stream alone. But durable records across this broker persist identities **issued
by other streams** — sequences, consumer durable names, and subjects — and recreating a
stream invalidates every reference to it that survives elsewhere.

**This is filed as one finding over the class, not one per member.** The audit reached it
the wrong way round: four consecutive holistic rounds each surfaced a different member, and
each time the instance was patched rather than the family swept. What that repeated failure
finally established is recorded in the next section, and it is not a population.

### What is established, and what this finding deliberately does NOT claim

**This finding makes no claim to have enumerated the population.** Four attempts were made
and review falsified every one: first "the streams F-033 touches" (blind to
`$MQTT_inflight` and `$MQTT_sess`), then "every `json:` struct tag" (blind to identities
carried in headers and `Nats-Msg-Id`s), then a five-carrier sweep whose JSON arm was in fact
a keyword-filtered grep (blind to `ScanFrom`, `Cons[].Durable`, `CancelledRegs[]`), and a
"five carriers exist" claim refuted by `StreamConfig.Metadata` — the carrier the first row
of the table below depends on. Recording a fifth confident enumeration would repeat the only
mistake this finding has consistently made.

**Completeness is therefore explicitly deferred to F-034**, which owns `$MQTT_inflight`.
That deferral was discharged on 2026-08-22 in the shape this section asked for — a
field-by-field enumeration of every persisted record type, every field dispositioned,
cross-stream or not, over a type universe derived by a recursive walk rather than any
filtered sweep. See [the persisted-identity census](persisted-identity-census.md); note
that its interpretation-surface half is a drift guard rather than a closure proof, so
F-066's own completeness deferral is NOT discharged by it. What follows is a **verified
sample with a worked consequence**, not a census.

### The premise, and the members verified so far

**Falsified premise: "recreating one stream is a self-contained recovery procedure."** When
a broker-owned stream fails shape validation, startup refuses and instructs the operator to
*"stop ALL brokers for this account and recreate `<that stream>`"*. For two streams that
message names a second stream and calls the pair *"one logical unit"*; for the rest it names
one. But durable records persist identities **other streams issue** — sequences, consumer
durable names, subjects, and stream metadata — and recreating a stream invalidates every
surviving reference to it.

Identities confirmed to cross a stream boundary, by carrier:

| Holder | Carrier | Identity | Issued by | Coupled? |
|---|---|---|---|---|
| `$MQTT_teardown` | stream `Metadata` + body `MarkerSeq` | epoch + sequence | `$MQTT_sess` | **YES** — `mqttSessEpochMetaKey`, validator refuses a mismatch, paired stop-all |
| `$MQTT_willq` | body `OriginSessionSeq` + `OriginSessEpoch` | sequence + epoch | `$MQTT_sess` | **YES** — compared only when the epochs are equal |
| `$MQTT_willq` | body `ScanFrom` + `ScanEpoch` | sequence + epoch | `$MQTT_sess` | **YES** |
| `$MQTT_qos2assign` | subject, body `SourceSeq`/`GroupDurable`/`ClaimHash` | sequence, consumer | `$MQTT_msgs` | **NO** |
| `$MQTT_qos2assign` | body `CopySubject`, `AffDurable` | subject, consumer | `$MQTT_out` | **NO** |
| `$MQTT_out` affine copy | subject; header `Nmqtt-Q2Claim`; `Nats-Msg-Id` | claim hash, subject, consumer+sequence | `$MQTT_msgs`, `$MQTT_qos2assign` | **NO** |
| `$MQTT_out` stored PUBREL | headers `Nmqtt-Q2SrcDur`/`Nmqtt-Q2SrcSeq`; `Nats-Msg-Id` | consumer + sequence | `$MQTT_msgs` | **NO** |
| `$MQTT_inflight` | body `Dur`, `Seq` | consumer, sequence | `$MQTT_msgs` / `$MQTT_out` | **NO** |
| `$MQTT_inflight` | body `Q2AffClaim` | subject | `$MQTT_qos2assign` | **NO** |
| `$MQTT_inflight` | body `ClaimSeq` | sequence | `$MQTT_sess` | **NO** |
| `$MQTT_sess` | body `Q2Aff.Durable`, `PubRel.Durable`, `Cons[].Durable` | consumers | `$MQTT_out`, `$MQTT_msgs` | **NO** |
| `$MQTT_sess` | body drain durable, `Started[].Seq` | consumer, sequences | `$MQTT_msgs` | **NO** |
| `$MQTT_sess` | body drain `Started[].RegSeq` | sequence | `$MQTT_inflight` | **NO** |
| `$MQTT_sess` | body `CancelledRegs[]` | register nonces | `$MQTT_inflight` | **NO** |

### What the sample shows: a pattern applied three times, never systematised

An earlier draft asserted a rule — *coupled if and only if it references `$MQTT_sess`* — and
its own table falsified it: `$MQTT_inflight`'s `ClaimSeq` is a `$MQTT_sess` sequence and is
uncoupled. The rule is withdrawn rather than rescued. What survives it is a weaker but
defensible observation, and it is the actual finding:

> **Three verified epoch couplings exist, implemented through two mechanisms, all against
> `$MQTT_sess` — and
> the idea was never generalised, not even across `$MQTT_sess` itself.** Every confirmed
> reference to `$MQTT_msgs`, `$MQTT_out`, `$MQTT_qos2assign` and `$MQTT_inflight` is
> uncoupled, and so is one reference to `$MQTT_sess`.

There is no mechanism here to be absent from — only hand-built instances of a pattern nobody
wrote down. That is why the class produced a new member in each of four consecutive review
rounds, and why a **generalisation** rather than a fourth bespoke coupling was the intended
remedy. **That intent is SUPERSEDED**: the by-construction attempt at the generalisation was
abandoned and the remedy is now the all-nine recreation contract — see "The decision,
2026-08-31". This paragraph states what the sample shows about the CODE, which is unchanged;
it no longer states what the remedy is.

### What that costs on the affine path

Four streams are CONFIRMED to be mutually entangled on this path — `$MQTT_msgs`,
`$MQTT_qos2assign`, `$MQTT_out` and `$MQTT_inflight`. That is the verified affected subset,
**not a proven closed universe**: two earlier drafts named three and had to correct the
count. The census deferred to F-034 has since landed and enumerates the population, but it
is a drift guard over the interpretation surface rather than a closure proof, so it does not
settle whether a fifth belongs either. Recreating any
proper subset of the four strands the references the others hold:

| Recreated | Stranded reference | Direction |
|---|---|---|
| `$MQTT_msgs` | every claim's `SourceSeq` and `ClaimHash` | **Loss.** A new message reuses sequence `s`; the surviving claim is terminal, so the mover takes the terminal arm and acknowledges the new source with nothing delivered. A surviving `claimed` claim instead stalls the group. `q2AffSourceGone` also reads the new incarnation's reused sequence as the old claim's source still being present, breaking the source-absence inference the reaper and recovery both depend on. |
| `$MQTT_msgs` + `$MQTT_qos2assign` | retained copies, addressed by a hash derived from `s` | **Wrong payload.** The new claim's copy subject collides with a retained old copy; `materializeQ2AffCopy`'s create-only CAS is rejected, its re-read reports the copy PRESENT, the claim advances to `materialized`, the source is acknowledged, and the gate publishes the OLD payload under the new message's assignment. |
| `$MQTT_qos2assign` | every copy's embedded claim subject | **Stall.** The delivery gate's `claim == nil` arm NAKs, fail-closed by design, so each orphaned copy NAK-loops forever holding a `MaxAckPending` slot. `$MQTT_inflight`'s `Q2AffClaim` is stranded identically. |
| `$MQTT_out` | claims reading `materialized`; every affine consumer; `$MQTT_inflight`'s `Dur`/`Seq`; the session record's `Q2Aff.Durable` and `PubRel.Durable` | **Loss.** Accepted copies die; only an owner resume records `Lost` (F-064). A register's `Seq` then names a sequence the recreated stream will reuse, so recovery can interpret an unrelated new copy through a stale register. |
| `$MQTT_inflight` | every drain's `Started[].RegSeq` in `$MQTT_sess` | **Destruction of another session's record.** `RegSeq` is an EXACT sequence and `deleteInflightRegisterConfirmed` deletes it with no subject or record identity check at all, so a surviving drain releasing its register after the recreation deletes whatever register now occupies that sequence — an unrelated session's. **What that costs the victim is deliberately NOT asserted**: an absent register on the next restart leaves the tracking maps empty and the redelivery takes a freshly allocated identifier, and a live same-process entry can still complete normally. The established defect is the unconditioned destruction; its downstream consequence belongs to that mechanism's owner. |
| any subset excluding one of the four | the excluded stream's inbound references | as above, per row |
| all four together | none KNOWN | **No stale identity among those confirmed survives.** Whether that makes the set a sufficient logical unit depends on the census this finding does not claim to have done. |

**"Clean" here means no stale identity survives to be misapplied — it does not mean
lossless, and an earlier draft of this table wrongly conflated the two.** A copy can be
materialized on `$MQTT_out` and its `$MQTT_msgs` source acknowledged before the client
finishes the exchange, so recreating `$MQTT_out` destroys an accepted message whose source
is already gone and cannot be re-offered. That loss is inherent to recreating a stream that
holds accepted data and is not what coupling fixes; a remedy that wants to avoid it needs
draining or preservation in the procedure itself, which is a stronger requirement than
this finding makes.

### The remedy this repository already has, twice

`$MQTT_teardown` and `$MQTT_willq` both solve this against `$MQTT_sess`, by two variants of
one idea: stamp the referenced stream's incarnation, and refuse to interpret a reference
whose epoch does not match. `$MQTT_teardown` puts the epoch in stream metadata with a
validator and a paired stop-all message naming both streams as *"one logical unit"*
(`stream_prereqs.go:481`, `:561`); `$MQTT_willq` carries it per record and compares the
sequence only when the epochs are equal. Either shape works.

**This section states the remedy that was INTENDED and is now SUPERSEDED**, and it is kept
because the mechanism it describes is the one the decided remedy generalises rather than
discards. The intended remedy was that treatment extended to the four-stream unit above and
carried into **every** artifact derived from a source sequence — the claim subject, the
claim hash and hence the copy subject, the copy's `Nats-Msg-Id`, the claim's own
`SourceSeq`, the sweep's derived-name validator, and `$MQTT_inflight`'s
`Dur`/`Seq`/`Q2AffClaim`. That per-artifact extension is what the by-construction attempt
tried to close by construction and could not. **The decided remedy keeps the stamp-and-
refuse idea and moves it up a level**: one recovery epoch over all nine broker-owned
streams, compared once at startup, so no per-artifact derivation has to carry an epoch at
all. See "The decision, 2026-08-31".

### Scope, and the method gap this exposed

The table above is a **verified sample, not a census** — the opening section states why
no completeness claim is made and why the enumeration was deferred to F-034, which
landed it on 2026-08-22 without discharging this finding's completeness deferral. Its
rows outside the affine path were **named, not audited here**: `$MQTT_inflight`'s own
coupling was F-034's to answer and has been answered, and the session record's
`$MQTT_out` durable references belong to the findings that own those mechanisms. The
drain `RegSeq` row is the exception and is retained here rather than routed away: it is
the only identity in the table whose stale use is itself DESTRUCTIVE — an unconditioned
delete of an absolute sequence reaching another session's record — so it belongs with
this finding's remedy even though the drain machinery is not F-033's. What the victim
then suffers is left to that mechanism's owner; this finding asserts the destruction,
not a downstream consequence it has not established.

**The method gap, stated plainly.** F-033's five route inventories enumerate writers and
destroyers: they ask what the code does, never what an OPERATOR is instructed to do, and
never which identities cross a stream boundary. That is why this class was invisible to the
audit and had to be found by review. Any later per-stream audit should add both questions
—
"which `stopAll` procedures name this stream alone?" and "what does this stream's persisted
state reference that another stream issues?" — as explicit inventories.

**And a second-order lesson, paid for four times.** Every failed enumeration here was a
FILTER masquerading as a sweep: a set of streams already known, a `json:` tag pattern, a
keyword-filtered grep, a hand-listed set of carriers. Each looked exhaustive, none was, and
twice the claim was refuted by an entry in the very table it introduced. A completeness
claim is worth making only when the enumerated thing is closed by construction — every field
of every persisted type — and otherwise the honest artifact is a sample that says so.

### Corrections to the table above

Four rows are stale and one class of row was wrong. Recorded here rather than edited away,
because the table's errors are themselves evidence about the enumeration.

- **The drain `RegSeq` row's destructive consequence is HISTORICAL.** F-034 closed it: the
  drain never reads `RegSeq` to authorize a release, and addresses the register by a subject
  derived from the packet identifier under a record-identity condition
  (`unsub_drain.go`, "regSeq is deliberately NOT read here … a hint … may never authorize
  progress"). The field remains a cross-stream reference; the destruction does not.
- **The `$MQTT_msgs` + `$MQTT_qos2assign` "wrong payload" row is CLOSED by F-065.** The affine
  copy subject carries the claim's incarnation nonce, so a new incarnation's copy subject
  cannot collide with a retained old copy. What remains from that row is the **claim subject**,
  which is the create-only election key and therefore cannot carry the nonce.
- **`$MQTT_origin` is gone** — deleted by the write-lineage audit and now refused at startup.
- **THREE ROWS WERE NOT CROSS-STREAM AT ALL**, and it takes **two distinct unverified
  properties** to account for them, not one. **Two** fail a holder-versus-issuer comparison
  nothing performs: `mqttQoS2RelClaim.Fence` is an absolute sequence in `$MQTT_qos2in`'s own
  space on a record stored in that stream, and `mqttAcceptFact.SessEpoch` is the epoch of
  the very stream the record sits on — each named its issuer **correctly** and was wrong
  only in calling the reference foreign. **The third fails a different property, and that
  comparison would have ACCEPTED it**: `mqttQoS2RelClaim.Gen` declared issuer `$MQTT_sess`
  on a holder of `$MQTT_qos2in`, so the two differ and a foreignness check passes it as
  cross-stream. It is wrong because **no stream issues that value at all** — it is the
  exchange generation, minted
  in process at PUBLISH time and merely stamped onto the hold, not the session incarnation
  generation `$MQTT_sess` issues. The distinction is load-bearing for whoever builds the guard:
  one written for issuer provenance would not have caught `Fence` or `SessEpoch`, and one
  written for holder-versus-issuer foreignness would not have caught `Gen`. All three were
  also wrong in the census manifest and are corrected there — see the
  [persisted-identity census](persisted-identity-census.md).

### Attempted and STOPPED, 2026-08-26: the by-construction route

F-066 was planned as a generalisation — one epoch mechanism, with coverage closed **by
construction** rather than by a fifth enumeration — and **stopped as UNRESOLVED after five
non-converging plan-review rounds, with no implementation**. The box stays open. This section
records why, so the next attempt does not re-derive it.

**The bar.** Omission must have an executable falsifier without a hand-maintained seed,
and no residual may claim unproven coverage. That bar is not incidental: a residual saying
the interpretation surface is unclosed is the exact condition that blocks discharging this
finding,
so it cannot both stand and support ticking the box.

**Four structural failures, each established against the code:**

1. **Issuer provenance needs a hand-authored relation.** The affine claim's source sequence
   enters as a bare `uint64` from `ackReplyInfo`. Typing that integer's consumer does not
   prove the constructor chose the right issuing stream, and recovering the origin needs another
   hand-authored relation — the seed the bar prohibits. `mqttSharedNames` is the sharper case:
   it derives a group durable from group and subject with **no sequence source at all**, so
   supplying the wrong stream's epoch would compile.
2. **A route classifier over raw egress was neither uniquely rooted, disjoint, nor
   syntactically decidable.** A route can have two roots; a multi-parameter function can
   satisfy two class predicates; and "an ack subject, with no broker-stream subject
   reachable" is a dataflow property rather than a syntactic one.
3. **Final-sink discovery is circular.** Naming the connection's publish method is a seed,
   not discovery by kind, and a receiver alias or a differently named publishing method
   defeats a syntactic scan. Banning "an undiscovered sink" presupposes a way to find one.
4. **Verbatim serialization contradicts the access boundary it was introduced to protect.**
   Preserving a foreign reference's exact token is REQUIRED — a record holding one must stay
   writable, since rebuilding it without the reference is migration by omission — but that
   preservation is itself an encoding escape, handing a caller the value with no comparison.
   Decoder discovery keyed on the target type also escapes through `any`, an interface,
   or a generic parameter.

**A pre-registered stop rule fired.** The rule was recorded before the round that fired it:
if the round still returned not-converging with new structural findings inside the property
machinery, the item stops rather than taking a sixth revision. What fired it was that
predicate, not a trend: round 5 was still non-converging and its structural findings sat
inside machinery the previous revision had introduced — the fixes were generating the
findings. For the record the total findings fell steadily (40 → 25 → 20 → 15 → 12) while
the **structural** count fell once and then rose in every round after (4 → 1 → 2 → 3 → 4),
ending where it began.

**Constraints the attempt established, which a future one inherits rather than rediscovers:**

- Cross-stream identities divide into a **reissued value space** (sequences and names
  deterministically derived from one — the dangerous class, because a stale reference matches
  new unrelated state), a **stable logical name rebindable to a new physical instance**, and
  one **not reissued by a recreation**. The middle class is a property of each USE, not of
  the name,
  and its instance-identity check already exists in the orphan sweep as JetStream's own
  `ConsumerInfo.Created` fingerprint.
- `mqttPersistedSession.Drains` is the **only** field rebuilt from operational state that
  carries such a reference: `Cons` excludes shared consumers, and its persisted durables are
  randomly minted. A gated-out drain must therefore be preserved opaquely across a save,
  or it is dropped — migration by omission.
- The shared group durable is **not a persisted reference**; it is recomputed at restore
  from the membership key, so its hazard belongs to derivation, not to a stored field.
- Discarded `json.Marshal` errors are harmless with today's plain record types and become
  **precondition** on any future change that introduces a fallible marshaler.

**What had to be decided before a further attempt**, and it was not a design question:

1. Accept hand-maintained issuer-origin and sink seeds with strong drift guards and an explicit
   residual — under the current definition of done, F-066 then stays open;
2. Redefine what closing F-066 permits, including both seeded coverage and an explicit
   serialization exception to the access boundary, and require a fresh approved plan;
3. Leave it blocked under the current bar pending a genuinely different construction;
4. **Change the supported recovery contract** so the hazard is eliminated rather than
   generalised — make the affected streams one mandatory recreation unit, or withdraw
   independent single-stream recreation. That is a product and operational-contract decision.

Splitting the finding or shipping a partial mechanism was not among them, and is not now.

### The decision, 2026-08-31

*Historical record of the ruling as written on 2026-08-31, when the enforcement was still
owed. It LANDED on 2026-09-01 — see "What landed" above — so every future-tense statement
below has since come true, and the single-stream procedures this section says are "still
the supported ones" are the ones that change retired. Nothing here describes the contract
in force.*

**Option 4 was taken, at ALL-NINE granularity: every broker-owned `$MQTT_*` stream WILL BE
one recreation unit, and independent single-stream recreation WILL BE withdrawn — when the
enforcement lands, which it has not.** The decision is settled; the contract change is
owed, and until it lands the single-stream procedures the validators print are still the
supported ones. Ruled by the
repository owner on the four options and their costs, informed by Codex consult
`task-mthbq7cn-xtxpjw` and an advisor read that both chose this option. The attribution is
recorded because this decision was reserved to the owner: no consult could take it on their
behalf, and the stopped attempt's own record says so.

**Why the fourth option, and why all nine rather than the affine four.** Options 1 and 2
each require overruling the standing definition of done — option 1's residual is verbatim
the sentence that blocks discharging this finding — and option 3 ships nothing. Option 4's
FOUR-STREAM variant inherits the completeness deferral this finding never discharged,
because the table's "all four together" row is qualified by exactly the census not done. The
all-nine variant needs no census at all: four attempts to enumerate the IDENTITIES were each
falsified, but the STREAM SET is a single ordered provisioning table with one loop over it
(`brokerStreamSpecs`), so the enumerable thing is the streams. That completeness is
**demonstrable at this HEAD and GUARDED rather than impossible** — the test's nine-name set
is itself hand-maintained — and saying so is the point, since overclaiming closure is the
mistake this finding has made four times. `$MQTT_origin` is not a tenth member; it is
retired state that stays separately refused.

**What the decision does to this finding's mechanism.** The stamp-and-refuse idea survives
and moves up a level: one recovery epoch over all nine streams, compared once at startup,
instead of an epoch threaded through every artifact derived from a source sequence. The four
structural failures the by-construction attempt established — issuer provenance needing a
hand-authored relation, the route classifier, circular final-sink discovery, and verbatim
preservation being itself the encoding escape — are why that route was abandoned, not an
inheritance for a sixth revision of it. Nothing above them needs re-deriving.

**What the decision does NOT buy, stated so no plan claims it** (written while the
enforcement was still owed; it landed 2026-09-01, see "What landed" above). Once the
enforcement lands, the hazard becomes unreachable through a SUPPORTED operation
rather than eliminated — and until it lands it is reachable exactly as this finding
describes, since the decision alone changes no contract. Every stale-identity path in
the table above will still exist in the code; what changes is only that no supported procedure
will produce the precondition.
Startup validation remains a snapshot, so a live operator
deleting a stream behind initialized managers still reaches them, and the all-brokers-
stopped precondition stays load-bearing. Accepted-data loss when recreating a stream holding
accepted data is unchanged, and this finding already stated that coupling never fixed it.
Recovery will become a complete account-level state reset with no partial recovery. And
the two existing per-pair couplings will state a fact the global epoch also states, so
they become redundancy-deletion candidates rather than part of the remedy.

### What landed, 2026-09-01

**DONE**, after six plan-review rounds and one decision consult on a fired stop rule. The
ruling's enforcement is in the tree; the by-construction generalisation stays abandoned.

**Two mechanisms, neither subsuming the other.** A COUNT rule
(`mqttPreflightBrokerStreamBundle`) over the same spec table the provisioning loop
iterates, taking a new lookup-only interface so it cannot create what it decides about:
zero admits, nine admits, one through eight refuses. Each lookup's `(info, error)` PAIR is
dispositioned together, with UNDECIDED failing startup rather than reading as absence. And
a per-stream EPOCH stamp: `mqtt_sess_epoch` is now on all nine and compared by every
validator through `validateBundleEpochStamp`, into which `validateTeardownStream`'s private
check was deleted. The count catches a member ABSENT at startup, and what it buys is that
the broker no longer silently recreates it; the epoch catches a member present but from
another incarnation. NEITHER catches an operator who recreates a member by hand and copies
the surviving stamp onto it — forgery, and outside what this claims.

**The guarantee that was deliberately NOT built, which is this finding's fifth lesson about
overclaiming.** Three revisions tried to guard "no durable mutation from a table callback
before a later validator"; review refuted each, the last because the table's closures
capture the receiver, so `as.jsa` is reachable whatever the parameter list says. A consult
ruled the guard out: the invariant holds today, the change strengthens it, it was never
this finding's obligation, and the same hypothetical exists at HEAD. It is recorded as
READ NOT GUARDED, and the ordering argument was re-grounded on the initialization order at
one call site so nothing depends on it.

**What did not change.** The hazard is unreachable through a SUPPORTED operation, not
eliminated. Startup validation and the preflight are snapshots; the all-brokers-stopped
precondition stays load-bearing, and the concurrent-deletion interleaving that defeats the
preflight is a stated residual. Accepted-data loss on a reset is unchanged. Completeness is
demonstrable at this HEAD and GUARDED — the nine-entry table is hand-maintained — not
proven.

The residuals, the concurrent-cold-start refusal and the recovery contracts are in
`docs/CONFORMANCE.md`; the mechanism is in `docs/ARCHITECTURE.md`, "The nine streams are
one recreation unit".

## F-084: Prove a successor before claiming a takeover

**DONE 2026-08-27.** The successor predicate was **avoided rather than found**, which is the
resolution and not a shortcut: no persisted shape proves a successor, so the claim was
withdrawn from every route that cannot hold one. `mqttDisposalTakenOver` now has exactly one
reader, reachable only from `discardTakenOverClient` - the in-process same-client-ID
replacement this broker holds both ends of under the ownership gate. Everything
record-derived reports the new fourth vocabulary `mqttDisposalOwnershipUnsettled` (`0x89`,
`mqttReasonStrOwnershipUnsettled`, minting nothing).

**The Will half was five routes, not one**, and four were real losses that three review
rounds uncovered one at a time: the foreign disposal dropped a parked deferred action on the
floor; a detached client's close publishes only an *immediate* Will so an owed-delayed one
vanished; the remote-persist eviction performed **no** Will action at all; and the takeover
discard reached classes that had demonstrated no takeover. The fifth,
`mqttAbortAcceptedConnect` discarding the *arriving* connection's Will, is correct and
unchanged - which is why the rule is scoped to the **displaced** session's Will, since a
pre-CONNACK disposing arm has both in play taking opposite actions.

**What landed.** A two-phase `mqttForeignDisposition`: phase 1 runs the identity check
first, then extracts the stale client and any parked action and chooses one typed
`mqttWillAction`, all in a single `sess.mu` critical section; phase 2 executes it outside
that lock, after the caller's record transaction closes and, at every caller that
holds one, while the ownership gate is still held. The remote-persist eviction is the
exception and its contract is the inverse: it decides under `as.mu` and runs its action
from a tail where no gate is held at all, so a single universal oracle would have had to
be wrong for one route or the other, and there are two. A refused identity check carries
`mqttWillActionNone`. `dischargeDeferredWill`
is extracted from `applyDeferredDisconnect`'s lower half - Will-only, client-free, no
fence - and `applyDeferredDisconnect` calls it too, so the discharge exists once.
`settleBoundRejectionGated`'s rebind predicate gained the discrimination it needed: `sess.c
!= c` is also true when `sess.c` is **nil**, which a resolver or a record-derived disposal
produces, so only a different **non-nil** local client is licensed evidence.

**Refuted and recorded so none returns.** Reusing `applyDeferredDisconnect` for the
discharge: it applies the whole deferred teardown, assumes the session is still resident and
its record resolved as ours, and takes a record fence two callers already hold - a
self-deadlock, not a race. Deleting `mqttRepairSaveVerdict`: its five control outcomes are
load-bearing and it has three direct production callers; what the dossier actually
wanted was the vocabulary carried in the return, which `mqttDisposalForRepairSave`'s
deletion achieved.
Recovering the fork bit from `voc.reason`: both `0x89` vocabularies share the code, so it
would mint a grant for every unsettled record.

**What a future change must not undo.** `mqttDisposalTakenOver` keeps exactly one reader.
The fork bit travels explicitly from the classification to the mint. The Will action is
chosen by the **licence**, never by the vocabulary or the verdict's name. Phase 2 stays
outside `sess.mu` and outside the record transaction, and inside the ownership gate. The
discharge does not re-park, because phase 1 has already deresidented the session.

**Shape:** 5 plan-review rounds (findings 10 -> 7 -> 7 -> 1 -> 0, zero repeats), 2
consults - one selecting the item and settling the five decisions below, one granting a
bounded
revision 5 as a recorded exception to the pre-registered three-round cap. Residuals in
`docs/CONFORMANCE.md`; mechanism in `docs/ARCHITECTURE.md`, "Only a held successor licenses
a takeover claim".

---

### The original filing, kept for its analysis — NOT current behaviour

**Everything from here to the end of this finding is the record AS FILED on 2026-08-26.**
Its present tense describes the broker as it then was: the takeover claim it enumerates has
since been withdrawn from every route it names, and the Will-disposition question it defers
is answered in the resolution above. Read it for the population, the refuted designs and the
reasoning — not for what the broker does.

**Filed 2026-08-26**, split out of `docs/ROADMAP.md`'s pre-CONNACK-signal
item by an escalation consult after three consecutive plan-review rounds cut this same seam
wider each time. That escalation is the finding's own history and belongs here: the
enumeration kept growing because the enumeration was the defect
(`feedback_close_the_class_with_a_property`), and the cut was made at the CLAIM boundary so
the signalling half could ship without shipping a newly visible false claim.

### The finding

The broker asserts `mqttDisposalTakenOver` — "session taken over by a new connection",
DISCONNECT reason `0x8E`, which tells a client to **stop reconnecting** — on evidence that
establishes no successor at all.

`classifyRecordLineage`'s negative verdict, `mqttLineageNotForked`, expressly covers a
legitimate successor, an unrelated lineage, a different incarnation, a record carrying no
identity, and a verbatim REPLAY of our own acknowledged bytes. Its own comment says
"nothing is established". Every one of those concedes to the takeover vocabulary, and the
conclusion the broker then reports is the strongest one it has.

The verdict is safe where it is CONSUMED negatively — a body claiming our parent does not
establish a takeover, so `0x89` "try again" is licensed and truthful. It is unsafe wherever
the *absence* of sibling evidence is read as a takeover.

### The population — a sample, not a census

Three rounds enumerated this and each found more, which is why it is filed as a sample:

- `disposalForDirectRejection`'s **four** conceding branches: the record could not be read,
  was absent, did not decode, or sat at or below our own base.
- `resolveContendedRecordSave`'s **two** gone arms — the in-loop one and the
  post-exhaustion one — plus `movedVerdict`'s below-base fail-safe ("foreign fail-safe
  whatever its identity says") and its above-base non-sibling arm.
- The three mappers that guess a vocabulary from a verdict which has already discarded the
  evidence: `mqttDisposalForRepairSave`, `mqttDisposalForForeignResolution`,
  `mqttDisposalForRejection`.
- `disposalForRemotePersist`.
- **Two hardcoded takeovers that bypass every mapper**, in `mqtt.go` and in
  `sess_rejected_write.go`. These are why fixing only the named mappers was refuted: the
  invariant would be false the moment it was stated.

### Why it is not a wording fix

The remedy needs a predicate for what POSITIVELY proves a successor, and the obvious
candidate is not one. A usable record above our base, whose digest differs from our bytes,
whose `WID` is non-empty and differs from `ackWID`, and whose `PWID` names `ackWID`,
identifies a **child-shaped body** — not proven ownership. `createOrRestoreSession` adopts
`WID` and `PWID` **without validating provenance**, valid-shaped bodies can arrive through
unconditional publication (the same route that makes the fork classifier necessary at all),
and replay policy is unsettled.

So the predicate cannot be stated as a proof yet, and five decisions come first:

1. trusted provenance versus merely valid-shaped lineage;
2. replayed child records;
3. **parentless clean-start successors**;
4. **descent beyond one hop**;
5. for each resulting class, whether it **discards or consumes the Will**.

Decision 5 is behavioural rather than vocabulary, and it is the one that makes this a
second item's work: F-078's live-SUBSCRIBE Foreign arm discards the Will *as a takeover*,
while Forked and Local take ordinary non-foreign handling. An honest "unsettled" class
reaching that arm therefore CHANGES Will disposition, and every bound displacement path
needs the same review.

### Design already sketched and reviewed, so it is not re-derived

- A **fourth disposal vocabulary** carrying `0x89` and reusing
  `mqttReasonStrOwnershipUnsettled`, with its own `logReason` — exactly the
  `mqttDisposalTeardownIncomplete` precedent, which also reuses that code and text because
  the NAME is what is read at the call site. It **mints nothing**: a fork grant is a repair
  anchored to a record proven to claim our parent, and none was.
- A **sixth outcome** rather than folding unsettled into an existing disposing arm. Folding
  it in is what routes it to `mqttGroupOwnershipForeign` and changes the Will action;
  F-078 switches on the outcome, not on the vocabulary.
- The **deletion** of `mqttDisposalForRepairSave` and of `mqttRepairSaveVerdict`, whose
  whole job is to reconstruct a vocabulary from a verdict that discarded the evidence. The
  vocabulary should be returned by the classifier that has the evidence, which is the shape
  `classifyGatedRecordSave` already uses for the two kinds it does distinguish.

### Refuted, so neither returns

- **Documenting it as a residual.** It would ship a newly visible false claim on a signal
  whose entire purpose is to tell the client the truth.
- **Fixing only the named mappers.** The two hardcoded sites bypass them.

### Tests it owes

- Real **direct-10071 and forced-10164** fixtures, separately. A plain record deletion
  reaches the direct arm and credits nothing to the contended one, so a single fixture
  cannot pin both.
- A **no-mint** assertion for the new outcome, through the per-manager store counter that
  the pre-CONNACK-signal item added — the final capsule state cannot reveal a second mint.
- DISCONNECT-side coverage on a **live v5 shared-SUBSCRIBE** path. **Not** the 3.1.1 forced
  leave: `mqttBuildDisconnect` returns nil for 3.1.1, so that route cannot send `0x8E` or
  `0x89` at all.

### Availability consequence to weigh, not to discover later

Moving existing DISCONNECT paths from `0x8E` to `0x89` invites repeated reconnects while
permanently unreadable or unclassifiable state remains. That is an availability cost, not
a
state-safety failure, and it is preferable to telling a client its session was taken over
when nothing established that — but it is a real cost and the fix should state it.

### Cross-link

The **comment-corpus sweep** filed in `docs/ROADMAP.md` §0.5 carries family 2 — verdict
comments that read a rejected CAS, an absent record or a gone record as proof another owner
holds the session. Those are this finding's distinction stated in prose, and they are swept
there rather than here so that F-084 stays scoped to the successor-evidence question and does
not become a documentation item.

### What is already contained

The pre-CONNACK **failure CONNACK never carries the takeover text on any arm**. Its
projection is a separate two-value vocabulary that asserts only what the evidence supports,
and it is exhaustive with no `default` so a new outcome cannot inherit a claim. So the
exposure this finding names is confined to the DISCONNECT and log paths, which is where it
was before, plus the CONNACK's deliberate UNDER-claim in the other direction — both stated
in `docs/CONFORMANCE.md`.

## F-087: Rebuild every in-flight register rewrite from the authoritative reread

**Title:** Register rewrite/claim/recovery consistency: a retargeted CAS that re-sends a
stale body erases what a concurrent writer proved

- Status: **CLOSED 2026-08-30.** Filed 2026-08-29 by the F-072 plan-review loop (eleven
  Codex rounds; that analysis lives in `~/.claude/lanes/f-072/`). Fixed under a plan of
  its own — thirteen review rounds, three escalation consults, one user reopening, 22
  mutations — recorded in `~/.claude/lanes/f-087/`. What landed: the rewrite rule over the
  frozen writer set; the `Retired`-dominates-`Cancelled` precedence at all four readers;
  post-contest routing on the RETURNED body; lineage-aware sequence application; the
  same-delivery takeover's four converging dispositions, including the held quarantine;
  the verdict-reaping delete property; and the send-section identifier reservation.
  **One residual, documented in `docs/CONFORMANCE.md` and filed as its own roadmap item:**
  a quarantined identifier has no live-session release, because both settled shapes the
  register vocabulary offers are consumable by the rival's supported lifecycle. It is
  released by the next SUCCESSFUL resume's reconciliation — STAGED by the walk and
  committed only past every refusal return, since a refused resume preserves the session's
  maps — or by generation retirement.
- Note (2026-10-06): since F-105 7b the CLAIM contest and its foreign-claim quarantine run
  only for the slot-only (experimental affine) class. An ordinary slot's ownership is the
  connection's accepted entitlement, compared inside each owner-dependent authority batch,
  and an occupant naming another delivery is abandoned fail-closed
  (`mqttSlotDispAbandon`), not quarantined by claim (`docs/ARCHITECTURE.md`, "Resume
  restores from the inventory, and ownership is the entitlement's"). Identifier holds of
  the `regForeign` kind still exist on the ordinary path for unreadable, poisoned and
  demoted registers, so the hold itself is not affine-only.
- Classification: HA arbitration — durable-record lifecycle on `$MQTT_inflight`
- Release treatment: Alpha blocker. The `Retired` erasure below re-sends a payload the
  client has already completed, which is a QoS 2 exactly-once violation, not a
  bookkeeping cost.

### The finding

Several writers of `$MQTT_inflight` build a record body once, and when their CAS is
rejected they reread the subject only to learn the **sequence** — then retarget the CAS
with the **original body**. Whatever a concurrent writer put in the record between the
build and the retarget is silently erased. The first instance was found in round 1 of the
F-072 loop and round 2 widened it from that one site to the closed writer set below; per
`feedback_close_the_class_with_a_property`, the fix is one rule over a frozen close set,
not per-site patches.

**The instance that makes this an alpha blocker.** The resume claim contest snapshots the
record once (`claimed := *fr.rec`, `inflight_register.go:3329`) and, on the
`cur.ClaimSeq < claimSeq` arm, retargets with `expectSeq = smsg.Sequence; continue`
(`:3419`) — re-proposing the stale snapshot. A **retirement written concurrently is
erased**: `markInflightRegisterRetired` exists precisely so a restart does not re-send a
payload whose exchange is past PUBREC, and a claim rewrite that overwrites `Retired`
lets exactly that re-send happen after the client's PUBCOMP. Worked end to end in the
F-072 lane, rounds 1 and 2.

### The class, extensionally

The production writers of `$MQTT_inflight` (every `storeInflightCAS` caller), each with
its disposition under the rule:

1. **The resume claim contest** (`:3342`) — the alpha-blocking instance above. Owns
   `Claim`/`ClaimSeq`; must rebuild from `cur` on every retarget.
2. **`storeInflightRegister`'s same-delivery takeover** (`:2964`, `:2981`) — replaces a
   foreign register naming the same `(Dur, Seq)` with this allocation's nonce. It cannot
   rebuild from `cur` (it replaces a foreign body on purpose), so it needs a
   **source-level converging disposition**: preserve or adopt the occupant's transmission
   history, or quarantine the source without letting it take a second identifier. The
   F-072 lane established the constraint set the hard way — an unconditional refusal
   fails because `abandonInflightRegister` (`:3144`) clears the `cpending` mapping, so
   AckWait re-offers allocate a fresh identifier; and a contested-retry reservation was
   reviewed twice and does not settle in every lifecycle state (a stable foreign pair
   retries forever; a cancellation arriving while the retry is parked leaves no store
   callback to settle `regPending`). Whatever converges here must define **settlement**
   for: occupant completes, occupant is stable, cancellation during the contest, session
   discard during the contest.
3. **`markInflightRegisterRetired`** (`:1640`) — owns `Retired`; already rereads, must
   rebuild from `cur` rather than re-propose its constructed body on retry.
4. **`markInflightRegisterCancelled`** (`:1767`) and **`markInflightRegisterCancelledPending`**
   (`:2470`) — **inside the rule, not exceptions.** Both construct sparse bodies (`:1780`,
   `:2483`) and retarget them (`:1832`, `:2515`), so a cancellation racing a claim rewrite
   or a retirement erases `Claim`/`ClaimSeq` or `Retired` exactly as the contest does. The
   F-072 loop's round 3 rejected "cancellation dominates" as licence for that: dominance is
   a statement about the terminal DISPOSITION — recovery honors `Cancelled` over the other
   facts — not about the right to erase what a concurrent writer proved. They rebuild from
   `cur`, set `Cancelled`, and leave every other field as read. Their
   identifier-lifecycle half is **F-088**, not this finding.
5. **The recovery live-entry sequence repair** (`:4086`) — applies a claim rewrite's
   sequence to a live entry unconditionally; a late apply can regress `regSeq` past a
   newer sequence, aiming later exact-sequence deletes at a dead message. Application
   must be **lineage-aware, not merely base-conditioned** — the F-072 loop refuted the
   base-only form twice: an operation starting at S1 can legitimately retarget through S2
   and land S3 while memory has advanced to S2, and a check accepting only the original
   base then rejects S3, leaving `regSeq` aimed at dead S2. An application may advance
   from **any sequence the operation authoritatively traversed**, and refuses unrelated
   state; the traversal chain terminates on `Retired`, `Cancelled`, a foreign claim, a
   different nonce, an unreadable record, or a subject/body mismatch.

Also in scope, because the same rounds proved recovery consumes what these writers
produce: the **post-contest routing**. Recovery sorts records into `live`/`retired`
before the contest runs (`:3706`), so a record that gained `Retired` between the
classification and the contest's own reread comes back retired but sits in `live`
(`:3850`) and is installed as deliverable. Re-sorting on the returned body is necessary
and NOT sufficient — the F-072 loop's round 3 required an explicit routing over **five
result classes**, and a plausible implementation that preserves the `Retired` bit can
still install a completed payload as deliverable without it:

- **owned, live** — install as today;
- **owned, newly retired** — route to the retirement arm; never re-contested, and never
  left in the live duplicate-source grouping;
- **failed/opaque, live** and **failed/opaque, retired** — retain the existing
  poisoned-sibling semantics where the source identity is still trustworthy;
- **closed / displaced** — as today.

Every result converted to opaque is counted **before** `piSpaceIncomplete` is computed,
keyed on what the walk found rather than what the pass installed (`:3988` already makes
that distinction for the existing opaque class).

**Also in scope, assigned here by the F-072 lane's residual accounting: the stale
send-authority race.** An in-flight delivery callback's ownership check precedes its
socket write and is never rechecked, so a teardown, a subscribe denial, a recovery apply
or a session discard can remove its authority — and free its packet identifier — while
the callback still writes its PUBLISH. Two exchanges can then use one identifier, and an
acknowledgement can bind to the wrong message. Pre-existing (today's confirm→send
interval has it identically), surfaced and repeatedly re-verified by the F-072 rounds,
and owned here because the repair is a revalidation inside the same lifecycle this
finding's rule governs. A dossier-satisfying implementation that closes only the CAS
writers has NOT closed F-087.

### The rule

> A retargeted CAS on `$MQTT_inflight` starts from the record it just read and mutates
> only the fields the operation owns. A body assembled before the reread describes the
> record as it was; re-proposing it erases the winner of the round.

Monotonic facts (`Retired`) merge forward; identity facts (`Nonce`) never change under
one exchange; arbitration facts (`Claim`/`ClaimSeq`) belong to the contest alone. F-072's
transmission fact deliberately does NOT enter this rule: its settled design records it on
a sibling marker that is created once and deleted once, never rewritten, precisely so the
register body carries no transmission field for these writers to erase.

### What this finding is not

It does not own the DUP flag (F-072, since CLOSED 2026-09-09), the transmitted-cancellation
identifier lifecycle (F-088), or any migration/legacy machinery — every shape here is
current-format, and an unsupported shape stays refused.

### Acceptance

- A deterministic race test per class member: the contest retarget racing a retirement
  (the payload must stay suppressed **behaviorally** — source re-offered, nothing
  delivered — not just the stored bit intact); a cancellation retarget racing a claim
  rewrite and racing a retirement (the tombstone must land WITHOUT erasing
  `Claim`/`ClaimSeq` or `Retired`); the sequence repair racing a newer apply in both
  orders **and through an intermediate retarget** (S1 → S2 → S3 with memory already at
  S2 — the case a base-only check fails); the post-contest routing; and the stale
  send-authority race (authority removed and identifier freed mid-write, then proving no
  second exchange can bind the identifier) — for the JETSTREAM-BACKED PUBLISH
  transmissions this finding's mechanism describes; the PUBREL transmission (F-089) and
  the retained QoS 1/2 send (F-091) are filed separately.
- The takeover disposition must include a test that drives a real AckWait re-offer and
  proves the source never takes a second identifier, plus settlement tests for the four
  contest lifecycle states named above.
- Mutations: re-propose the stale body at each rebuild site; drop the reclassification;
  apply a sequence on a nonce match alone. Each must fail its paired test
  (`feedback_mutation_checks_must_discriminate`).

## F-089: A PUBREL transmission must hold its packet identifier until the write returns

**Title:** The PUBREL send path has no send-section reservation, so a PUBCOMP for an
earlier copy can free an identifier a duplicate PUBREL write is still using

- Status: **DONE 2026-08-30**, after five plan-review rounds (`~/.claude/lanes/f-089/`).
  Alpha blocker, unconditional. Filed 2026-08-30 by F-087's holistic
  review (round 3). Scope settled by F-087's `DECISIONS.md` D6: the defect is
  pre-existing, the path is byte-identical before and after F-087, and D2's rule is that
  a shared standard found defective OUTSIDE the reviewed scope is repaired uniformly
  under a new finding.
- Classification: MQTT conformance — packet-identifier lifecycle ([MQTT-2.3.1-4])
- Release treatment: Alpha blocker. Identifier reuse while a transmission still holds the
  identifier can bind an acknowledgement to the wrong message, and can destroy a QoS 2
  exchange's completion evidence — the same severity class as F-088.

### The finding

F-087 gave every JETSTREAM-BACKED QoS 1/2 **PUBLISH** transmission a send section: the
session's `sendsInFlight` registry holds the identifier until the write returns, `bumpPI`
always consults it, and removal paths condemn-and-park rather than free. Two
identifier-bearing transmissions have no equivalent — the **PUBREL** one, which is this
finding, and a **retained** QoS 1/2 send, which is F-091.

`mqttDeliverPubRelCb` calls `trackAsPubRel`, releases
`sess.mu`, and performs a synchronous deadline-free socket write through
`mqttEnqueuePubResponse` with nothing reserving the identifier;
`mqttProcessPubComp`'s `untrackPubRel` (`mqtt.go:11784`) removes the `pendingPubRel`
entry — and with it the identifier's reservation — with no send-section check and no
park.

**The execution.** A duplicate PUBREL for identifier X is being written. That is routine,
not exotic: the PUBREL consumer redelivers on AckWait, and `resendPendingRedelivery` NAKs
pending PUBRELs on resume precisely to force it. While that write is blocked, the read
loop receives the client's PUBCOMP for an EARLIER copy of the same PUBREL. `untrackPubRel`
frees X, and `bumpPI` can re-mint it for a new PUBLISH while the PUBREL write still holds
it. Two exchanges then share one identifier on one socket, in order.

**Second-order, and worse.** The client answers the duplicate PUBREL with a second
PUBCOMP for X. If the new exchange on X has by then reached its own PUBREL, that stray
PUBCOMP acknowledges away the NEW exchange's PUBREL — destroying the durable evidence
that the new exchange is past PUBREC, and completing it against an acknowledgement the
client sent for a different message.

**The argument that excluded this is RETRACTED.** F-087's plan (revision 13) put the
PUBREL path out of scope on the grounds that "a PUBCOMP cannot precede its PUBREL's
completed write on one connection". That is true only of a FIRST transmission. For a
redelivery whose earlier copy the client already answered it is false — which is exactly
the hole F-072's round 1 proved for class D on the PUBLISH side, not carried across.

### Acceptance

**The second bullet below was REFUTED by the implementation and is retained only so it
is not re-derived.** A write-return section plus a PUBCOMP park closes the primary
execution and leaves the second-order one — the one this dossier calls worse — wide
open, because `endSend` frees the identifier the instant the write returns and the
duplicate's own PUBCOMP arrives after that. The park is also machinery this path does
not need: a PUBREL has no in-flight register (the identifier's durable authority moved
to the PUBREL record at the PUBREC handoff, R2), so PUBCOMP has nothing to defer, and
acknowledging the record while a duplicate write is out is preferable rather than merely
safe — the client has answered it, and the ack stops further AckWait redeliveries.
What landed instead is one hold with TWO release conditions: the write has returned, AND
every PUBREL transmitted under that identifier has been PUBCOMPed, which is the event
[MQTT-2.3.1-4] names. See `docs/ARCHITECTURE.md`.

- A phase-agnostic send reservation around identifier-bearing PUBREL writes, reusing
  F-087's registry rather than a second mechanism.
- ~~PUBCOMP parks its identifier-freeing continuation while a reservation is active, in
  the shape F-087's class D already uses for PUBACK and a failed PUBREC.~~ **Refuted;
  see above.**
- The drain's confirmed-PUBREL release (I2b — only a CONFIRMED PUBREL releases a drained
  claim) and the `drainedPubRel` do-nothing arm are reconciled with the park, since both
  reach the same identifier by a different route.
- `TestMQTTInflightEveryQoS12SendIsReserved` extends past `mqttEnqueuePublishMsgTo` to
  the PUBREL enqueue — the structural guard missed this site because it searched only the
  PUBLISH one — plus a behavioral test with a blocked duplicate PUBREL, a concurrent
  PUBCOMP for the earlier copy, and an identifier re-mint attempt.

## F-088: A transmitted delivery's cancellation must not free its packet identifier

**Title:** `Cancelled` over a transmitted delivery lets recovery free an identifier the
client may still hold

- Status: **Open. Alpha blocker, unconditional. Ordered after F-087, which CLOSED
  2026-08-30** — persisting this state safely depends on settled rewrite rules, and it now
  has them. F-087 narrowed the blast radius without closing this: a `Retired ∧ Cancelled`
  record no longer frees an identifier and no writer erases `Retired` any more, but a pure
  `Cancelled` written over a transmitted delivery is still freed by recovery.
  **Since 2026-10-08 that last sentence describes the tree before the joint F-097 / F-088
  change**; see "What the 2026-10-08 change landed" below. The box stays open until the
  remaining witnesses run and the change is accepted.
- Classification: MQTT conformance — packet-identifier lifecycle ([MQTT-2.3.1-4])
- Release treatment: Alpha blocker. Identifier reuse while the client holds the
  identifier can pair an acknowledgement with the wrong message.

### The finding

`Cancelled` is written by two functions reached from three situations, and only one of
them proves no transmission happened:

1. **Pre-send withdrawal of a FRESH allocation** — `markInflightRegisterCancelledPending`
   (`inflight_register.go:2470`), and `confirmInflightRegister`'s ladder for an
   allocation cancelled while `regPending`. Where the allocation is this writer's own
   first one, the store was still in flight and nothing was ever sent, so the tombstone's
   "never transmitted" claim is true.

   **It is NOT true for an INHERITED allocation, and that is the consolidation's whole
   point.** `storeInflightRegister`'s same-source replacement arm admits an allocation over
   an occupant that may already have been transmitted; while its store is in flight the
   entry is still `regPending`, so every site keying on that bit reads a false
   never-transmitted proof. **`regPending` alone must never be promoted into proof that an
   inherited exchange was never transmitted.** See the roadmap bullet's "The inherited
   allocation's lifecycle, consolidated here", which is authoritative for the
   cancellation/custody obligations.
2. **Abandonment of a delivery whose register store completed** — `purgeConsumerPendingLocked`
   (`:2292`) collects **non-`regPending`** entries too. A cleared `regPending` proves the
   REGISTER STORE completed, not that anything was transmitted — the confirm→send interval
   means some of these were never sent, a distinction the F-072 lane established and its
   marker design encodes. The population therefore includes genuinely transmitted
   deliveries, for which the tombstone asserts a falsehood, and the
   cleanup ladder that consumes them, `voidRegistersOnStream` (`:1963-1985`), falls back
   to `markInflightRegisterCancelled` when their delete cannot be proven. The tombstone
   then asserts a falsehood.
3. **A cancellation racing a transmission-state write** — **current, not prospective:
   F-072 LANDED 2026-09-09.** Its contract is what a design consumes here: marker present
   is proven-unsent, marker absent is possibly-attempted, and F-072 deliberately left this
   cancellation route alone.

   **The old "between the send and the marker delete" window is RETIRED and must not be
   used as a fixture.** F-072's redesign moved the marker delete to `recordSendIntent`,
   which runs after admission and **before** the socket write (`mqtt.go:20946-20953`),
   precisely closing that ordering. The two interleavings that are actually reachable:

   - **Cancellation races the captured sender BEFORE intent deletion.** The sender holds a
     token captured at `beginSendLocked`, and `recordSendIntent` consumes it **without
     rechecking cancellation** — so a cancellation that has already read the marker as
     present can have it deleted and the frame written underneath it.
   - **Cancellation lands between the CONFIRMED intent delete and the socket write.** The
     durable pair already reads possibly-attempted while nothing has been written; F-072
     accepts this window as conservative DUP=1, and a cancellation arriving inside it must
     not treat the absent marker as its own proof.

   **F-097 is this item's prerequisite**: its completion contract is what lets a tightened
   release still converge.

Recovery frees the identifier for any `Cancelled` register **without distinguishing these
cases** (`:3720`), so case 2 frees an identifier the client is still holding, against
[MQTT-2.3.1-4] and against the in-flight rule the rest of `inflight_register.go`
enforces. The comment corpus compounds it: `storeInflightRegister:2986` and the
`Cancelled` field comment both assert that a cancellation "records a delivery that was
NEVER transmitted", which `purgeConsumerPendingLocked` falsifies.

This is **pre-existing** — reachable today through the teardown paths that VOID a live
in-flight delivery's registration (`purgeConsumerPendingLocked` and the ladder above; a
drain is NOT one of them, since `stageDrain` retains confirmed deliveries in the drain
rather than freeing their identifiers) — and was surfaced by F-072's review rounds, which
needed to know what a cancellation proves. The F-072 lane's consult explicitly rejected
pinning the current behavior as a documented deviation: it is a defect to fix, not a
residual to accept.

### The safe lifecycle

- `Cancelled` **and provably never transmitted** → reap and free, as today.
- `Cancelled` **and transmitted (or unprovable)** → suppress any further delivery, but
  keep the identifier **reserved** until the protocol exchange completes
  (PUBACK/PUBREC/PUBCOMP as applicable) or the session is destroyed. The reservation
  must not be redeliverable through the removed consumer.
- Every cancellation writer records which case it is writing, and recovery honors it.
  How that fact is persisted is **F-087-dependent** (it must survive the rewrite rules),
  which is why this finding is ordered second.

### Acceptance

- A teardown of a transmitted, unacknowledged QoS 1 and QoS 2 delivery followed by
  restart: the identifier is still reserved, no new allocation reuses it, and the
  client's late acknowledgement completes or the session's destruction frees it.
- The pre-send case still frees promptly (the F-072 lane's fixtures cover this shape).
- The corrected comment corpus: no comment claims `Cancelled` proves never-transmitted.
- Mutation: recovery frees on `Cancelled` unconditionally again; the paired restart test
  must fail.

### What the 2026-10-08 change landed

The joint F-097 / F-088 change (lane `~/.claude/lanes/f-097-after-f105/`, plan revision
20) lands the cancellation half below. The sections above are the finding as filed and
are kept as history; where they describe recovery or the door, this list is current.

- **A cancellation is no longer evidence of non-transmission.** Arm 4 of
  `mqttSlotReleaseAuthority` is marker-only: neither the register's `Cancelled` bit nor the
  session record's nonce verdict licenses a never-transmitted release. Recovery classifies
  a cancelled register like any `publish` register and the unsent marker decides: standing,
  the genuine-cancellation disposition; absent, possibly attempted and restored acting;
  unreadable, opaque.
- **The door refuses where it used to tombstone.** `endUnsentBeforeDestroy`'s no-marker
  and missing-exchange rows refuse, `tombstoneBeforeDestroy` is deleted, and a withdrawal
  whose landing is unknown is recorded as owed and retried by the custody dispatcher.
- **A transmitted entry removed unanswered keeps its acknowledgement route.** Every
  scheduling-entry removal states its reason, and an unanswered removal of a transmitted
  entry installs an AckRoute binding in the same critical section, so the client's late
  PUBACK or PUBREC still reaches the exchange's completion owners (F-097's half).
- **Cancellation provenance.** A cancelled register carries `cancel_origin`
  (`pending-create` or `occupant`); the create-only pending fence is disposed of by that
  provenance alone, through one rule that ends no exchange and releases no charge.
- **Split off: F-119.** A paused admission that lands after its pending fence was disposed
  of, with no cancellation evidence surviving, is preserved and follows ordinary recovery,
  so the cancellation can be lost. That is cancellation durability, not this finding's
  custody seam; it is filed as F-119 below and stated as a residual in
  `docs/CONFORMANCE.md`.
- **Witnesses.** The inherited-allocation interleaving (3), a failed store after
  adoption that dropped the source mapping, is W5b's schedule and is closed for its
  witnessed population by F-097's admission continuity. Interleaving (1) is handled by
  the cancelled arm's staged change (nothing destroyed over a landed register-only
  replacement), but W5c, its production producer chain, was not separately witnessed
  (T17 covers the boundary form). PS-1 and PS-2 (a cancelled register, or a covered
  verdict, over a transmitted exchange) stay OPEN: their tests seed the inputs, and no
  teardown was found to hand a transmitted register to the tombstone writer.

## F-098: No cross-broker authority over an incarnation's in-flight identifiers

**Title:** Within one session incarnation there is no enforceable exclusion or handoff over
inherited PUBLISH transmission, terminal completion and PUBREC/PUBREL advancement, so no
observation of durable state stays true long enough to release a packet identifier

- Status: **CLOSED over the supported boundary**, 2026-09-13, by attempt 3. Filed
  2026-09-10 by F-097 unit 2's plan-review loop, after **three** revisions offered three
  different release proofs and all three were dismantled. **Ordered before F-097**, which
  is blocked on it, and therefore before F-088.

  **C-IV is closed WITH A QUALIFICATION, and the qualification is normative.** Release
  decisions are consolidated across supported recovery, allocation and live-release routes,
  including the local identifier and Receive Maximum consequences. The exception is ONE arm
  of the **experimental affine disposer**: `disposeSettledAffineRegister`'s NON-RESIDENT arm
  still decides its own release, spending no authority and minting no release token, because
  asserting proven-unsent for a retired record would be false. Its RESIDENT arm routes
  through the consolidated gated door like every other release — saying "both arms" was
  wrong, and the code is what settles it. `docs/ROADMAP.md` section P owns the remaining
  arm. An unqualified "C-IV closed" is false; the boundary holds in code, not merely in a
  build tag, because the sweep selects only retired affine registers, the affine pair is
  written only with the experimental gate on, and the production command fixes that gate
  off.

  **What attempt 3 delivered**, beyond the consumption attempt 2 left open: a total
  authority-per-disposition table with a refusing default, with the release class narrower
  than the destructive class (replacement installs a successor rather than freeing an
  identifier); an authority carried as a value **bound to the observation that produced
  it**; one extracted post-delete confirmation partitioning the subject four ways; and the
  identifier exclusion described below.

  **The distinction that took eight plan-review rounds to find**, and the one a future
  reader most needs: **preserving identifier EXCLUSION is not the same operation as
  preserving the actionable, capacity-charging ENTRY.** Three revisions conflated them and
  review dismantled each. Retaining the entry breaks the drain's cancellation handoff (the
  cancellation records only a nonce verdict, so a retained entry lets the returning store's
  confirmation admit a delivery the drain does not claim), misses the real release at the
  quarantine commit, and deterministically leaks capacity for registerless retained frames,
  which can never obtain a durable confirmation. (Since F-105 7c a retained QoS 1/2 send
  admits a register and is a custody-ledger exchange, so the registerless shape is
  history; note of 2026-10-06.)

  **The two gaps a mid-run review found in the change itself, and both are closed.**
  **Cold-recovery occupancy**: the retired/source-complete classifier staged a
  `DeleteComplete` no such record can ever obtain authority for — `Retired` is backed by the
  client's own PUBREC, so arm 4 refuses it and `publish` is not a terminal phase — while the
  apply phase installed exclusion only inside the resident-`pendingPublish` branch. A cold
  resume has no such entry, so the refused delete left the durable record standing with its
  identifier allocatable, which is strictly worse than either outcome alone. The repair is
  **not** to restore the delete, which was itself contrary to C-II: the classifier stages
  nothing, the source is still consumed, and the occupancy obligation is carried into apply
  independently of any resident entry. **`endSendWith`'s deferred settlements**: its
  non-empty branch untracked after clearing the send token without handing off exclusion,
  so for a subscribe denial or a recovery drop the untrack was the moment the identifier
  became allocatable over a slot nobody had confirmed gone.

  **One inventory error of mine was corrected by the same review**, and it is worth recording
  because the conclusion was nearly the opposite of the truth: `settleSlotExclusion` had no
  production caller, and I read that as the exclusion having no release edge at all — attempt
  2's failure mode exactly. `liftIdentifierExclusionLocked` has **two** real callers,
  `recoverPublishTerminalSlot` and `finalizeTerminalSlot`, both lifting on a confirmed subject
  absence. The wrapper was redundant, not the mechanism unwired, and it is deleted.

  **Two HEAD defects were found on the way and are closed with it.** **F-103**: the
  evidence loader admitted a STAGED cancellation verdict, handing the release authority a
  proof no restart can see. **F-104**: the verdict was retired after a delete that removed
  nothing — an already-absent sequence reports success — stranding the surviving register
  permanently. F-104 had TWO bypasses; the second was found by review. A
  `forgetCancelledReg` family sweep found six call sites, four of them licensed by a
  tombstone write or lost arbitration and unchanged.

  **Not to be re-attempted, on top of the three refuted proofs above.** A per-site release
  licence in any wording: four consecutive rounds refuted one, each for the same reason the
  original three failed — an observation goes stale between the observing and the act, and
  `acquirePublishAction` can acquire an ordinary PUBLISH record carrying `Cancelled`.
  Literal entry retention as the exclusion carrier. Equating the confirmation's
  "another exchange occupies the subject" with "this identifier is free". And releasing a
  drain's exclusion on a successful drain alone: a drain from `pubrel_ready` proves the
  ACTION drained, never that the client exchange completed.
- Classification: HA arbitration — packet-identifier lifecycle ([MQTT-2.3.1-4],
  [MQTT-4.4.0-1])
- Release treatment: Alpha blocker. F-097 unit 2 cannot be planned without it; F-088 sits
  behind F-097.

### What is missing

A resume **preserves** the incarnation generation (`sess_incarnation.go:254-256`), so two
brokers may legitimately be operating the same subjects at the same time. Every mechanism
this project has for bounding that is either **per-generation** — the retirement watermark,
which says nothing about a live incarnation — or **process-local**. F-036's incarnation
completion lease is explicit about the latter (`sess_lease.go:33-45`):

> All of this is process memory. No persisted shape changes; a crash discards the ledger
> with the process and recovery semantics are exactly what they were.

And the connection check that looks like a fence is not one: `mqttProcessPublishReceived`
tests `sess.c == c` on entry under `sess.mu` (`mqtt.go:19120-19126`) and then **releases the
lock before its durable work**, so a PUBREC handler admitted before a rebind continues
across it. The takeover tail closes the displaced connection asynchronously
(`sess_takeover_redrive.go:228`), and cross-broker displacement rests on the session-record
watch and a periodic backstop (`sess_record_watch.go:25`), not on the predecessor's read
loop finishing. **Pre-CONNACK is therefore not a fenced moment either**: it stops the
*arriving* connection from processing further packets; it joins nothing.

### Why that is a finding rather than a design preference

Releasing a packet identifier — or declining to reserve one, which is the same
authorization, because `bumpPI` then mints it — requires establishing that no client-facing
obligation stands at that identifier. **Every attempt to establish that from durable state
alone goes stale between the observation and the act.** F-097 unit 2's plan loop demonstrated
this three times, each time with a different proof and each time with a concrete schedule:

| Proof attempted | How it fails |
|---|---|
| the register's **absence** | The drain's `releaseInflightRegisterByIdentity` (`unsub_drain.go:1363`) removes a register while the exchange is still `publish`, and PUBREL admission records the consequence in its own words: an absent slot "does not prove the exchange is over — the client may not have answered PUBCOMP at all, and route B may be about to recreate the record" (`pubrel_action.go:1329-1344`) |
| **no stored PUBREL and no persisted past-PUBREC intent** | The scan is not a snapshot. A PUBREC handler stores its PUBREL (`mqtt.go:19411`) after another broker has scanned, and `sess.mu` is process-local so the scanning broker observes neither the placeholder nor the lock. `pubrel_terminal_draining` and the drain's persisted intent (`unsub_drain.go:2354`) are further carriers the scan does not see |
| **authority at reuse** — an occupied register refuses a new allocation | Retained sends allocate an identifier without consulting any register at all (`mqtt.go:11138` → `trackPublishRetained:13406` → `bumpPI:13247` → `beginRetainedSendLocked:5159`). And the harm is not only reuse of that identifier: releasing it frees **Receive Maximum capacity**, so a later allocation of a *different* identifier transmits while the released exchange still stands |

The same staleness defeats a repair at the allocation site: between loading the occupant,
validating it and deleting it at its exact sequence, a PUBREL can be stored without changing
the register, and the exact-sequence delete then succeeds against a record that has acquired
an obligation. That cross-stream window is supported behaviour at HEAD
(`pubrel_action.go:1213-1229`) and JetStream atomic batches are **per stream**, so no
existing primitive closes it.

### The schedule that must be covered, and it involves no non-conforming client

The inherited-lineage population is real. [MQTT-4.3.3-9] requires a receiver that sent a
**failed** PUBREC to treat a subsequent PUBLISH under that identifier as a **new Application
Message**, which it may accept with a successful PUBREC; the client cannot see the broker's
lineage. So:

1. A broker receives a failed PUBREC for allocation N1 under lineage L; its register cleanup
   fails.
2. The source acknowledgement is lost or delayed; the surviving register stays `publish`
   with no unsent marker.
3. A later offer or resume installs N2 through the same-source replacement arm
   (`inflight_register.go:3998`), **inheriting L**, and transmits.
4. The client accepts that PUBLISH and answers with a successful PUBREC, creating a live
   PUBREL obligation **under the inherited lineage**.

Any completion evidence keyed on L now coexists with a live obligation under L. The broker's
retransmission after a failed PUBREC is itself in tension with [MQTT-4.4.0-2]; that is a
broker-side problem and not grounds to call the responding client non-conforming.

### Acceptance

The repair must supply enforceable exclusion or handoff covering **all three** competing
operations, and its acceptance must cover **recovery, allocation and live release** — not
only a live reconciler:

- inherited PUBLISH transmission (including the retained path, which today bypasses the
  register entirely);
- terminal completion and the durable cleanup that follows it;
- PUBREC/PUBREL advancement, including the store-before-phase window and the drain's
  persisted intent.

It must cover the failed-PUBREC/heir schedule above. A further durable-state observation is
**not** a candidate: that is what three refuted proofs have in common. The evidence does not
establish that any one fence design is sufficient, nor that a fence is the only conceivable
shape — only that another observation is insufficient while these producers and
transmissions remain reachable.

### Not to be re-attempted

The three proofs in the table, in any wording. In particular: "the evidence is the proof and
the absence is only a check" is the same inference as releasing on absence, and was refuted
on that basis.

### Where the next attempt starts

**Reviewed, not speculative.** This section replaces the unvalidated starting position
that stood here. It records what a three-round plan-review loop and two design consults
established, and what they failed to establish. The rounds produced **no production
code**; the loop ended on a ruling, not on a clean round.

#### The design course, ruled twice and not to be re-litigated

- **One executable unit, extending the in-flight slot state machine.** A durable,
  non-expiring **session-wide ownership permit** was ruled a *viable* alternative — the
  earlier claim that every durable epoch reduces to a lease is **withdrawn and was
  wrong** — and it is rejected on **cost**, not on impossibility: an owner crashing
  before its confirmed release strands the whole session's identifiers including idle
  ones, where a slot's abandonment cost is confined to one exchange. The direction is
  settled on that ground. Do not re-dismiss it as impossible and do not re-open it as
  novel.
- **The PUBREL intent must be a MONOTONE PHASE, never a holder-exclusive action.** A
  holder-exclusive intent is permanently pinned by a crash between the CAS and the
  store, with no other broker able to advance it.
- **Retained sends need a durable SOURCE-LESS slot**, not a full source-backed register
  and not F-091's reservation, whose `retainedGen` term is local and goes inert once the
  connection stops owning the session (`inflight_register.go:5319`).
- **A second roadmap bullet was refused twice.** A split proposed in round 2 was
  **withdrawn by its own author** on the ground that separately committed halves need
  separate holistic reviews and temporary adapters that the second half deletes, and
  that with no intermediate deliverable those costs buy nothing.

#### Three contracts that were reviewed and ruled to CLOSE their classes

These are the run's actual product. Each was tried against counterexamples and held.

- **C-I — transmission identity.** The record gains `Acq`, a monotone acquisition
  counter incremented by every action-acquisition CAS and **never reset by a drain**,
  overflow refused rather than wrapped. An exchange's authority identity is
  **(slot subject, `Nonce`, `Acq`)**, and every release, deletion or capacity free CASes
  on the whole triple. Nothing in the tree plays this role: `Nonce` is per-ALLOCATION,
  `Lin` per-OBLIGATION, `beginSendLocked`'s send nonce is process-local
  (`inflight_register.go:4960`), `ActionToken` is erased by the drain
  (`pubrel_action.go:476`), and the slot's stream sequence also moves for custody writes
  and retirement marks (`inflight_register.go:5863`) so evidence keyed on it goes stale
  spuriously.

  **The binding rule is what makes it sound, and omitting it reopens the hole:** an
  acknowledgement is stamped with the transmission identity **already bound to that
  handler's exchange**, never by fetching the slot's current `Acq` after the packet
  arrives — otherwise A receives an old acknowledgement, pauses, and stamps it with B's
  newer counter. A retry may refresh the CAS *sequence*, never the *evidence identity*.
  This is a conservative authorization rule and **not** a claim that MQTT
  acknowledgements identify individual retransmissions.

- **C-II — source completion never discharges a client exchange.** One proof family
  with **four enforcement edges**, because the phase gate alone does not reach two of
  them: the disposition gate (`inflight_slot_state.go:615`, where `mqttSlotDeleteComplete`
  needs proven-unsent authority and no live action, not merely a `publish` phase);
  durable terminal advancement (`advanceFinishedSlotToTerminal`, fed by
  `inflight_register.go:6946`); local untracking (`mqtt.go:14119`); and allocation
  (`inflight_register.go:4127-4162`). Recovery's own rule at `:7203` is the correct one
  and is the model: source completion frees an identifier **only** when the exchange is
  proven-unsent, and possibly-attempted is RESERVED. A further source-derived freeing
  route would **violate** this property rather than constitute a new class.

- **C-III — universal custody.** Every slot has custody at creation whatever its QoS and
  whether or not it is source-backed; every acquisition validates it; and every
  supported quiescent slot participates in custody reconciliation. Mandatory custody at
  creation alone is **not** the property — recovery's eligibility classifier also filters
  by `QoS == 2` (`inflight_register.go:5814`) and the validator permits custody-free
  ordinary QoS 1 (`inflight_slot_state.go:398`). A resume withheld by a **living**
  predecessor's action needs a retry continuation once that action drains, reached from
  ordinary PUBLISH delivery and not only from stored-PUBREL delivery
  (`pubrel_action.go:1454`).

#### What attempt 2 implemented, and what it left open

The section below is attempt 1's analysis and it is still the right description of the
PROBLEM, but two of its three open contracts are now closed in code and the third is
partly closed. Attempt 2 froze its plan after one design consult and two plan-review
rounds and then implemented; `docs/ROADMAP.md`'s bullet lists what landed.

- **C-IV DOES CLOSE**, as a TOTAL FUNCTION with a reserving default rather than the
  enumeration that failed three rounds. Its four arms are in `inflight_slot_state.go`.
  Two corrections the design consult made to the candidate are load-bearing and must not
  be lost: **any live action token withholds, INCLUDING the caller's own** (a broker whose
  socket write is still running could otherwise observe terminal evidence, disregard its
  own token and free the identifier), and **`Retired` is not a proof of non-transmission**
  and dominates a cancellation surviving over it. The `CancelledRegs` persisted verdict is
  a second cancellation carrier and is normalized into the evidence rather than consulted
  separately. **Consuming the function is only partly done** — the remaining destructive
  sites still decide for themselves, and that is the open half.
- **L1 IS CLOSED.** The obligation is a durable `pubrel_intended` phase no peer may
  reap,
  enrolled by the transition that creates it, retried with unlimited attempts and a capped
  delay, with custody transfer extended to a quiescent `pubrel_intended` slot — and the
  record is now RECONSTRUCTED from the slot alone (identifier, owner nonce, source durable
  and sequence, affine pair; no application payload is needed). An already-landed record is
  BOUND rather than duplicated, a store happens only after a complete scan finds none, and
  binding goes through the one readback-proven `pubrel_intended -> pubrel_ready` arrow.
  Holistic review round 1 found two gaps and both are closed: recovery now enrols a
  recovered `pubrel_intended` slot after taking custody, and a PUBREC's intent outcome is
  classified rather than inferred from the slot's existence.
- **L4 IS CLOSED.** The positive, identity-bound `Rel` request is persisted with all four
  preservation edges, and — the gap holistic review round 1 found — every retry is now
  BOUND to it: a superseded request retires only itself and never re-aims at the
  successor. A request naming another target is settled against its own identity before
  anything at the current subject is read (round 2), and one that survives a restart is
  enrolled at resume rather than waiting for a source delivery that an expired source or a
  deleted consumer makes impossible (round 3). The refuted alternative stays refuted.

**What attempt 2 added beyond the three contracts**, each forced by a regression the
contracts did not predict: a stored PUBREL refused because a transmission is live now
leaves a per-record WAITER that the transmission's end wakes (both halves: the durable
action token and the local `pubIdent`, which had never had a writer); `publish_terminal`
has its own recovery branch and store-time case, because every EXISTING phase enumeration
was a second definition of the set; a withheld drain completion has a retry chain, because
the client acknowledges once; and a graceful shutdown joins every PUBLISH send section and
finishes every unresolved drain while JetStream still answers, because an ordinary restart
must not strand an action no later start can attribute.

**The open half is C-IV's consolidation.** Every destructive edge applies one admission
rule — phase, the C-I counter, and arm 2's live-action veto — but the release DECISION is
still made at several sites in their own terms rather than by the authority function.

**Two things attempt 2 learned that the next reader needs.** The C-IV threading is
**compiler-driven for caller discovery and EVIDENCE-driven for the semantics** — making
the destructive helpers require an authority value compiles and enumerates the callers,
but a fail-closed placeholder at a site like `releaseInflightRegisterByIdentity` is not
"strictly safer", it strands the identifier on a path the drain depends on. And an
unlimited retry is only correct for work that CAN succeed: scheduling the unimplemented
reconstruction held the shutdown gate's wait group open and timed a full-suite run out at
ten minutes.

#### Three contracts that are NOT established, and exactly what is missing

This is attempt 1's analysis, kept because it is still the right description of the
problem. The section above records which of the three attempt 2 closed.

- **C-IV — one authoritative slot classification — DOES NOT CLOSE.** Recovery, the drain
  and every release each decide what a slot means, and the shared decision must replace
  all three. HEAD's enum at `inflight_register.go:4468` classifies **sources**, including
  poisoned and retired populations, and must not be reused mechanically. The four
  outcomes are not enough: `Reserved` (quiescent, marker absent, no token, no terminal
  evidence — recovery's own row at `:7214`) and `Unknown` (unreadable: withhold) were
  added and it still failed, because **`publish` + `Cancelled` + no marker + no token**
  matches no positive outcome, and HEAD already accepts pure cancellation as a
  never-transmitted proof with `Retired` taking precedence (`:6578`). **The contract has
  to be about admissible PROOFS OF NON-TRANSMISSION with an explicit precedence rule
  among conflicting evidence and live actions — not about marker presence.** Stated as
  marker presence it stays an enumeration and the next round finds the next member.

- **L1 — the materialization continuation is missing.** A slot is *owed* when it is
  `pubrel_intended`, or `pubrel_ready` naming a canonical record **provably** absent —
  and "provably" is load-bearing: a failed lookup, an incomplete scan, an unreadable or
  mismatching record are all insufficient, and HEAD already distinguishes them before
  duplicate disposal (`pubrel_action.go:1410`). Two things were settled: materialization
  needs **custody but not an action**, so a slot whose PUBLISH token was abandoned is
  **not materializable** and that is the residual rather than a recovery path; and an
  initial binding from a tuple-less `pubrel_intended` is a different operation from
  replacing an existing tuple, since the former has no named record to prove absent.

  **What is missing is liveness.** Re-enrolling an exhausted materializer "at the next
  recovery or custody validation" strands it: a custodian can sit connected with healthy
  storage, no source delivery, no PUBREL delivery and no new recovery, and nothing
  invokes another attempt. The action reconciler already recognises this and schedules
  (`pubrel_action.go:434`). L1 needs an equally guaranteed continuation **or** a
  disclosed residual.

- **L4 — durable release authority across the drain's crash — is unsolved, and the
  obvious fix does not work.** The drain releases the slot at `unsub_drain.go:1363` and
  only then opens its transaction at `:1383`, so a discriminated return value dies on a
  crash. **Reversing the two — persisting the claim removal first — does NOT fix it**,
  and this is worth recording because it is the natural next idea: a persisted claim
  *removal* is an **omission**. `startedSeqs` (`:324`) skips every unclaimed sequence, so
  the identifier and exchange metadata vanish with the entry, and
  `finalizeDrainDelivery` (`:1218`) reads a missing entry as already finalized. After the
  crash recovery finds an absent claim and a surviving slot, and inferring authority from
  that is **the first refuted proof**. There is also a peer schedule: A persists the
  removal and pauses, B takes another permitted action on the surviving slot, and A's
  release identity is now stale — C-I correctly refuses it, and a retry cannot bind the
  absent claim to B's current counter. What is needed is a **positive, identity-bound
  pending disposition** with its own recovery and retirement lifecycle.

#### Two more things not to re-attempt, on top of the three refuted proofs

- **A fresher CAS base is not arbitration.** "Re-read and CAS at the newly observed
  sequence, and fail if anything moved" fences two *overlapping* operations and does
  nothing about evidence created before an obligation and applied after it. Its
  counter-schedule: B holds N1's evidence; A installs N2, transmits and drains; B *then*
  reads N2's sequence and deletes at it, with nothing moving in between. C-I is the
  answer; a sequence is not.
- **An enumeration of carriers is not a rule.** Two revisions answered each finding by
  naming the carriers the reviewer had named, and each round found the next one — the
  same shape in five subsystems. A revision that enumerates converges on nothing. State
  the property; let each carrier become a refusal rather than a grep result.

#### Dependency order, and two defects this loop found at HEAD

**F-099 and F-100 first if they are taken on their own; then F-098, then F-097 unit 2,
then F-088.** Both were found by this loop, both are reachable at HEAD with no F-098
machinery, and both are members of C-II's proof family — so an F-098 attempt that
implements C-II repairs them, and they are filed separately because F-098 may not be
attempted soon.

**Both have now been taken on their own**: F-099 at `e7ca4b7`, F-100 after it. Neither
needed any F-098 machinery, which is the prediction above holding. What F-100 did NOT
establish is the part that was always F-098's: its gate reads local cached state, so it
narrows what one broker releases without establishing what a peer consumed. F-100's own
implementation filed two further findings from its census — **F-101** (two routes still
releasing a Receive Maximum charge without client completion) and **F-102** (a recovery
reservation over a Lost source with no release edge) — both pre-existing at HEAD and
neither in F-098's family.

#### What survives of F-097 unit 2's revision 3

Its **scope** survives: unit 2 is one executable unit whose shipment was blocked rather
than narrowed. Its **release proof does not** — all three were dismantled. Its
unresolved round-3 findings stand and must be answered after C-IV, L1 and L4 are
settled, not carried over as settled: the per-arm terminal-disposition ordering, the
crash row in which evidence is still volatile when the source expires, and `Progressed`
discharging a register-cleanup obligation it does not prove was discharged.

## F-099: Recovery can discard a PUBREL the client is owed, from a stale scan

**Title:** Recovery decides a `pubrel_ready` slot is finished using an absence observed
by an EARLIER scan, so a peer that stored and readied that exchange in between has its
record disposed of without a PUBCOMP

- Status: **DONE.** Filed 2026-09-10 by F-098's plan-review loop; repaired the same day.
  The item WIDENED during its plan-review loop: round 2 found a second live defect in the
  same function, and both are arms of one property. Working title as repaired:
  **recovery classification follows adopted slot state and ordered PUBREL evidence.**
- Classification: HA arbitration — outbound QoS 2 completion ([MQTT-4.3.1-1])
- Release treatment: **A lost message, reachable at HEAD**, with no F-098 machinery
  involved. Independent of F-098.

### The finding

Recovery scans `$MQTT_out` for PUBREL records **first** (`inflight_register.go:6178`) and
builds `rels`; `findExactPubRel` closes over that slice, so it answers from the earlier
scan and never re-reads. Then, having **subsequently** loaded the registers,
`inflight_register.go:6943-6947` concludes:

```go
finished := rv.rec.State == mqttInflightPubRelTerminal
if !finished && rv.rec.State == mqttInflightPubRelReady {
    _, exact := findExactPubRel(rv.rec)
    finished = !exact && sourceComplete
}
```

`advanceFinishedSlotToTerminal` (`pubrel_action.go:1092`) then moves the slot to terminal
and terminal cleanup disposes its records (`pubrel_action.go:964`).

**The schedule, with two brokers legitimately serving one incarnation** (a resume
preserves the generation):

1. B scans the PUBREL subject and finds it empty.
2. A's PUBREC handler stores the exchange's PUBREL and phases the slot to `pubrel_ready`.
3. The source is acknowledged, so B reads `sourceComplete`.
4. B loads the slot — now `pubrel_ready` — and evaluates `!exact` from its **step 1**
   scan. The slot is declared finished.
5. B advances it to terminal and terminal cleanup deletes the PUBREL.

The client never receives the PUBREL and never sends PUBCOMP. An exact-sequence CAS does
not help: B observed the slot **after** A's changes, so it is current.

### Why it is not one of the refuted proofs restated

It IS the second refuted proof — *no stored PUBREL therefore finished* — but as a
**live defect at HEAD** rather than as a proposed design. It is filed because F-097 unit
2's loop refuted that inference as a *plan*, and nobody looked for it in the tree.

### The second arm, found by this item's own plan-review loop

`claimRecoveredTransferableInflight` is called with candidates filtered on
`State == mqttInflightPublish`, but its withheld arm covers only action states and terminal
(`inflight_register.go:5929`). A body adopted as `pubrel_ready` therefore returns OWNED
(`:5966`) into the PUBLISH population, where the apply install has no state guard (`:7454`)
and the PRE-contest source verdict can restore the `cpending` mapping (`:7456`). The payload
of an exchange the client already answered with PUBREC is re-sent.

`mqttSplitNewlyRetired` (`:1602`) had stated this rule for the RETIREMENT flag since F-087,
and the comment above it (`:6716`) names the retransmission as "the finding itself". Only
the PHASE half was missing — and at the plain-QoS 2 site the re-judgement was missing
altogether.

### Acceptance, as scoped and delivered

The originally filed acceptance said `finished` must rest on positive terminal authority and
**never** on an absence. That was NARROWED deliberately, and the narrowing is recorded rather
than quietly taken: the literal version strands a SUPPORTED population. `mqtt.go:19832`
sends the PUBREL delivery's JetStream ack unconditionally after `mqttTerminalizeOnPubComp`,
including the `mqttTerminalIndeterminate` branch reached on a failed read or exhausted CAS
retries (`pubrel_action.go:843,867,901`), and `$MQTT_out` is interest-retained — so *source
complete → client PUBCOMP → terminal advancement fails → the ack removes the record → crash
before the process-local reconciliation timer (`pubrel_action.go:689`)* leaves
`pubrel_ready` + no record + complete source with the client having already sent PUBCOMP.
Deleting the inference makes that permanent, and with Receive Maximum 1 the session receives
nothing again.

So F-099 delivers:

1. All recovery PUBREL classification answers from ONE scan begun after every batch of the
   pass's INITIAL register enumeration and load — closing the filed interleaving.
2. Every contest's owned result (contests are slot-only since F-105 7b) is routed by its
   adopted body's phase and suppression flags
   before any PUBLISH grouping, send-state derivation or installation. All five sites have
   behavioral coverage, with targeted mutation kills over the repaired routing cases; a
   static check keeps the direct-call inventory under review and fails closed on an indirect
   reference, but proves no routing property of its own.
3. The supported phantom cut above keeps its recovery and gains the regression it never had.

**Positive terminal authority remains F-098's C-II**, and so do the store-after-scan window
and any exclusion of future stores. A past-PUBREC reservation with no record is reaped only
by a later resume whose own initial classification proves source completion; LOST and
UNKNOWN retain it, which is a held identifier rather than a lost message.

## F-100: Resend reconciliation frees Receive Maximum capacity without client completion

**Title:** A possibly-attempted exchange whose source is gone has its packet identifier
and its Receive Maximum slot released on source absence alone, though the client may hold
the exchange

- Status: **CLOSED**, taken on its own. Filed 2026-09-10 by F-098's plan-review loop.
- Classification: outbound in-flight identifiers — capacity ([MQTT-2.3.1-4], Spec5
  [3.3.2.3.3])
- Release treatment: below F-099. A member of the same proof family; F-098's C-II subsumes
  it and does not contradict it.

### The finding

`resendPendingRedelivery` reconciles the pending snapshot against `$MQTT_msgs`: an entry
whose message is below the stream's first sequence, or confirmed absent by an exact
lookup, is dropped, "releasing its packet identifier and skipping its NAK"
(`mqtt.go:14084-14119`).

Recovery classifies the *same* population the opposite way and is right to: a
**possibly-attempted** exchange whose source is complete or authoritatively absent is
**RESERVED** — identifier held, no source mapping — "because the receiver may hold that
identifier from an attempt whose PUBREC or PUBACK was lost"
(`inflight_register.go:337-343`, `:7203-7218`). A Message Expiry TTL reap is one of the
two `sourceComplete` producers, so this is not a rare cut.

So a source expiring under a transmitted-but-unacknowledged QoS 1/2 PUBLISH frees a
Receive Maximum slot the client is still spending. **Durable slot occupancy does not
protect it**: the harm is the freed capacity, not only the identifier, which is the third
refuted proof's actual point — a later allocation of a *different* identifier transmits.

### What is honest about the current code, and why it is still a finding

The release is rechecked under a final `sess.mu` and the entry is *condemned* rather than
untracked when a send section began in between, and the behaviour has a stated rationale
in Spec5 [3.3.2.3.3] — an entry whose message is gone "can neither be redelivered nor
PUBACK'd" and would otherwise leak a slot for the session's life. That rationale is about
the BROKER's inability to continue; it is not evidence about what the CLIENT holds, and
the two are exactly what recovery's proven-unsent / possibly-attempted split separates.

### Acceptance

Source absence may free capacity only for an exchange proven **unsent** — the unsent
marker present. A possibly-attempted entry stays reserved on this route as it does in
recovery, so the two rules stop contradicting each other about one record.

### How it was closed

The reap decides each candidate under one uninterrupted `sess.mu`, in three arms: an entry
whose `(durable, sequence)` no longer matches what the scan classified is left alone (HEAD
acted on the packet identifier, which is not a delivery identity); a proven-unsent entry is
released; everything else is reserved with its identifier and Receive Maximum slot held and
out of the NAK set. The gate is `provenUnsentReleasableLocked`, making this its fourth
caller rather than a second predicate. The site's `condemnSendLocked` call was deleted —
the predicate refuses an open send section under the same lock, so no continuation can be
owed. The pass's redelivery budget is read under one lock immediately before the arithmetic
rather than at the top of the pass, because a reserved entry leaves the NAK set while still
holding its slot.

**One deliberate departure from recovery**: the reserved entry KEEPS its `cpending` source
mapping. Recovery runs pre-CONNACK with no source interest installed; the reap runs after
restoration, where a callback may already hold the payload, and without the mapping
`trackPublish` would hand it a second identifier for one source. The cost is drain
enrolment of a vanished source, which the claim floor's placement before `recoverDrains`
gives a pruning opportunity — not a guarantee, and a claim staged by a live UNSUBSCRIBE
after the reap gets no opportunity before the next resume.

**Do not re-derive these**: the five `jsAckSubject == _EMPTY_` discriminators in
`pubrel_action.go` and `inflight_register.go:7499` are `pendingPubRel` readers and say
nothing about a PUBLISH entry; recovery's `:7545` skip branch leaves a live entry's mapping
in place, so "recovery already produces this shape" is false for a resident entry; and
`oversizeDisposalAllowed` enforces the same proven-unsent split through the send token, so
this predicate's callers are not the whole rule.

## F-101: Two routes release a Receive Maximum charge without client completion

**Title:** A retained send dropped for having no JetStream backing, and a pending entry
deleted by a source-consumer teardown, each free a slot the client may still be spending

- Status: **CLOSED 2026-09-10.**
- Classification: outbound in-flight identifiers — capacity ([MQTT-2.3.1-4])
- Release treatment: below F-100, and not in its scope.

### The finding

Two sites release a pending entry, and with it a Receive Maximum slot, on evidence that the
BROKER cannot continue rather than that the CLIENT completed — the inference F-100 rejects:

- `dropUnredeliverableRetained` (deleted by the fix; it lived in `mqtt.go` and was called
  on resume) dropped every
  pending retained QoS 1/2 send, identified by having no ack subject, no durable and no
  register. A retained send genuinely cannot be redelivered — it has no JetStream backing
  at all (R6) — but one that was **transmitted** still occupies client state.
- `purgeConsumerPendingLocked` (`inflight_register.go:3117`) deletes pending entries, or
  schedules their removal, during a source-consumer teardown. The teardown's own ladder
  establishes what the teardown owes; it does not establish that a possibly-attempted
  exchange's identifier is free.

Both were first classified as separate populations excluded from F-100's rule, and that was
wrong on review. Neither is reached through `untrackPublish`, which is why an
`untrackPublish`-only census misses them.

### Why it is not F-100's remainder

F-100's acceptance is about exchanges **with a source**: it distinguishes proven-unsent from
possibly-attempted using the unsent marker on a durable register. A retained send has no
register and no source to reason from, so it needs its own evidence rather than this one's.
F-100 does make the teardown site more reachable, by retaining the `cpending` source mapping
its reservation would otherwise drop — which is why a reserved identifier's hold is
documented as **not** unconditional.

### Acceptance

Written at close, because the finding was filed without one.

1. **Neither route releases an identifier on anything but proof of non-transmission.** The
   teardown's purge and the void staging before it both ask
   `provenUnsentReleasableLocked`; a retained send, which the predicate cannot classify
   because it has no durable pair (R6), is released only at its ABANDONMENT, and the
   resume-time drop is deleted rather than merely gated.
2. **A reserved exchange can still complete.** Its durable owner register is left
   uncollected, so a later PUBREC stores a correlated PUBREL that admission accepts
   against the surviving slot, and PUBCOMP frees the identifier. Deleting the slot instead
   leaves a stored PUBREL that `retainedPubRelAdmittedLocked_` refuses forever.
3. **A staged never-transmitted verdict cannot be falsified by a later send.**
   `beginSendLocked` refuses a register whose nonce carries one, and a rollback's
   `unvoidRegNonces` restores admission in the step that withdraws it.
4. **The release is identity-keyed.** A retained frame's disposition names its allocation
   nonce, so one arriving after a `clear()` cannot release whichever exchange now holds the
   identifier.
5. **What is NOT claimed.** No cross-broker authority (F-098's). No repair of the other
   durable `Cancelled`-verdict writers, `storeInflightRegister`'s same-source replacement
   included — those stay F-088's, and `inflight_register.go:1462` and `:4264` are named
   there rather than certified here. No durable cleanup keyed on a `Lost` classification,
   which was F-102's and **has since landed** (2026-09-11). And no prompt reclamation for
   a teardown that overlaps an open send
   section: that window reserves in all four intent outcomes, including the one a later
   definite pre-write refusal proves unsent, and `docs/CONFORMANCE.md` states the cost.

## F-102: A recovery reservation over a Lost source has no release edge

**Title:** A proven-unsent entry released on source absence leaves a durable pair that
recovery restores but never consumes, with no ack subject and no consumer to re-offer it

- Status: **CLOSED 2026-09-11.** Filed 2026-09-10 by F-100's plan-review loop; closed
  after 4 plan-review rounds (8 / 7 / 3 / 2 findings, all folded, none refuted).
- Classification: outbound in-flight identifiers — capacity ([MQTT-2.3.1-4])
- Release treatment: below F-100. **Pre-existing at HEAD**, and HEAD's release is strictly
  wider than F-100's, so this is not a regression F-100 introduced.

### The finding

Releasing a proven-unsent entry leaves its durable pair behind for recovery to consume. That
consumption requires the source to classify **Complete**, and it may not. Both
classifiers — `inflightSourceState` (`inflight_register.go:3707`) and the recovery batch
cache (`:3600`) — return **Lost** when the consumer lookup completes and finds nothing,
because deleting a consumer on an interest-retained stream deletes the messages it held, so
"the message is gone" proves the exchange finished only while that consumer still exists.

The live reap checks message absence and never consults the consumer, so the two genuinely
disagree. A Lost source never enters the `sourceComplete` consume arm, and recovery restores
the entry instead — carrying its marker and its source mapping but **no ack subject**,
because `mqttRecoveredPending` installs none. The reap only ever scans entries that have an
ack subject, so nothing re-examines it; no consumer remains to offer the source; and the
client never received this exchange, so its acknowledgement is not a realistic release
either.

### Acceptance

A reservation whose source classifies Lost needs a durable cleanup keyed on that
classification. F-100 deliberately did not expand into durable cleanup to absorb this, on
the ruling that a scoped live-release correction should not grow a recovery-side mechanism.

### What closed it

Recovery's consume arm routes on **the source can never be re-offered** — Complete or Lost,
through `mqttInflightSourceMayReoffer` — and the **unsent marker** decides the outcome. The
marker is the license, not the classification: a confirmed marker delete strictly precedes
any write, so a present marker proves the PUBLISH never went out. A proven-unsent register
is dropped with `mqttSlotDeleteNeverTransmitted`; a possibly-attempted one is reserved
unchanged.

**The correction that cost two rounds.** `mqttRecoveredInflightReg.sourceComplete` was a
ROUTED flag, not a classification: five exclusions — a pure cancellation, an unproven
affine register, a drain-ambiguous source, a drain-OWNED record, a retirement — each leave
the classify loop *before* it is assigned, and that absence is the only thing keeping them
out of the consume arm. Reading the source cache at the arm instead would have deleted all
five silently, for **Complete** sources as well as Lost ones. The field is replaced by
`mqttRoutedSourceCleanup`, whose zero value means *not routed*, assigned at the same site
and carried across the three claim-contest rebuilds (which run only for the slot-only class
since F-105 7b; for an ordinary register the flag passes through the walk unchanged).
`mqttInflightSourceState` was rejected
as the field type for the same reason: its zero is `Live`, which would make an excluded
record indistinguishable from a classified live one.

**Not closed by this.** Cross-broker release authority: the arm rests on a classified
snapshot, and there is no within-generation fence against a concurrent sender. That window
is equally reachable through `Complete` after a TTL expiry and remains **F-098's**. The
`storeInflightRegister` conflict arm is untouched — it holds no marker proof for the
conflicting delivery — so a stranded pair still refuses that identifier until a recovery
pass deletes it. Residuals in `docs/CONFORMANCE.md`.

## F-097: Terminal-completion evidence and reconciliation across allocation replacement

**Title:** A completed exchange has no durable completion evidence that survives its
register's replacement, and no reconciliation that releases the identifier without another
offer or a resume

- Status: **Open. Alpha blocker. Superseded in part on 2026-10-08**: the status below
  predates F-098's closure and F-105; see "The 2026-10-08 contract" at the end of this
  section. Original status: **Unit 2 is BLOCKED on F-098.** Filed 2026-09-09 by the
  joint F-088 / inherited-allocation planning loop, whose rounds 1 and 2 established that
  the cancellation/custody repair cannot be stated without it. Ordered **before** F-088 and
  **after** F-098. **Unit 1 of two has landed** (the exchange lineage; see "The design shape,
  ruled" below). The defect above is **not fixed**: nothing reads the lineage yet.
- Classification: MQTT conformance — packet-identifier lifecycle ([MQTT-2.3.1-4],
  [MQTT-4.4.0-1])
- Release treatment: Alpha blocker. F-088 consumes this contract; the two cannot be
  planned in the other order.

### Why this is its own finding rather than F-088's remainder

**It has independently reachable defects at HEAD, with no cancellation involved.** F-072
already prevents an ordinary marker-absent, source-complete PUBLISH register from being
consumed by recovery (`inflight_register.go:7110` onward). That is credited and not
re-proved — and it is a protection against **premature release**, not a claim that
cancellation safety has landed.

**The defect is what that protection leaves standing, and the schedule is concrete:**

1. An exchange completes. The acknowledgement handler releases its **live** custody
   correctly: `mqtt.go:19244` untracks the entry before any durable work, and the Receive
   Maximum slot is freed with it. **Nothing is stranded in memory at this point**, and any
   claim that a lost delete immediately stalls the live path is wrong.
2. The register's confirmed delete is **lost, indeterminate, or aimed at a sequence a
   rewrite has moved**, so the durable record survives as residue.
3. A later resume's recovery reads that marker-absent register, cannot prove the exchange
   terminal, and **reserves the identifier**.
4. Nothing releases it. There is no completion evidence to consult, and **a resume is not
a release mechanism** — retaining it is precisely what `:7110` does.

The cost is **outbound** QoS 1/2 capacity for the incarnation's life; at Receive Maximum 1
it stalls the session's outbound QoS 1/2 traffic. It is not an inbound defect: the client
owns its own identifier space.

### The prior representation ruling, and what is NOT settled

F-088's D1 answered the **representation**: an immutable, create-only completion sibling
`$MQTT.inflight.<idHash>.<gen>.<PI>.done.<nonce>` carrying **no `Claim`/`ClaimSeq`**, so a
custody rewrite cannot invalidate an acknowledgement. The refused alternative — terminality
in place on the register — reopens F-087's frozen five-writer set and must not be
re-proposed.

**That ruling settles the representation and nothing else. Exchange identity and lifecycle
integration are UNRESOLVED, and treating them as proved is the mistake this finding exists
to prevent.** Two identity failures are established, and any design must answer both:

1. **Nonce-only lookup loses completion across replacement.** Exchange N1 receives its
   terminal acknowledgement; its register survives because cleanup was delayed or
   ineffective; a same-source replacement installs N2; the landed sibling is `.done.N1`;
   reconciliation asks about N2 and finds nothing. This is a steady-state race between
   identical brokers, not a format problem, so the compatibility boundary does not excuse
   it.
2. **`(PI, Dur, Seq)` wrongly joins DISTINCT exchanges, and it was proposed and refuted.**
   The QoS 1 / failed-PUBREC arm releases the register and then sends a **fire-and-forget**
   source acknowledgement. `mqtt.go:19630`'s own comment: *"The register goes first, then
   the ordinary fire-and-forget ack: a lost ack here costs one duplicate PUBLISH, which QoS
   1 permits and which [MQTT-4.4.0-1] does not bind — it governs UNACKNOWLEDGED messages —
   so a confirmed round trip on the hottest path would buy nothing."* So: E1 completes and
   `.done.N1` lands; the source acknowledgement is lost; the same durable and sequence are
   **re-offered**; after identifier reuse E2 takes the same triple; E1's retained evidence
   matches E2 and authorizes a release of an outstanding exchange. **The premise "a
   completed source is not re-offered" is FALSE at this HEAD.**

   `.done.N1` is not implemented at HEAD, so this is a counterexample against the
   *proposed* design, not a defect of current behavior. The re-offer itself is existing,
   intended behavior.

   **Retained sends are a second collision population.** A retained send has no register
   and no source, so every retained exchange under one identifier shares `(PI, "", 0)`
   within an incarnation.

### The design shape, ruled

A read-only design consult settled this, and it chose **stable exchange lineage across
replacement** over a proved suppression rule. That choice stands, and unit 1's lineage
stands with it.

**But the guarantee originally claimed for it does NOT, and is superseded here.** The
original wording read: "with a lineage that only ever carries forward through the
inheriting replacement arm, old evidence cannot name a genuinely fresh exchange, so a
completed source that is re-offered costs at most the duplicate PUBLISH QoS 1 already
permits." A 2026-09-10 consult refuted that from the specification. **[MQTT-4.3.3-9]**
requires a receiver that sent a **failed** PUBREC to treat a subsequent PUBLISH under
that identifier as a **new Application Message**, which it may accept with a successful
PUBREC — and the client cannot see the broker's lineage. So the failed-PUBREC/heir
schedule in F-098 leaves completion evidence for `L` coexisting with a **live** obligation
under `L`, with no non-conforming client anywhere in it.

**The corrected statement: the lineage is correlation, not proof of one continuing
obligation.** Inheritance alone does not establish that an heir's obligation is the one
the evidence discharged, and it therefore authorizes no release. Whatever supplies that
authority is F-098's, not the lineage's. A re-offer after a failed PUBREC is also in
tension with [MQTT-4.4.0-2]; that is a broker-side problem, and it is not excused by
calling the responding client non-conforming.

The ruling also refused **the nonce as the lineage**. A replacement re-mints the nonce
for a stated reason (fencing the previous writer's cleanup) while inheriting the
client's obligation, so the two identities answer different questions and must be
separate fields.

**Unit 1, LANDED**: the mandatory `Lin` field, its per-attempt write-mode rule, its
refusal of an absent or malformed value, its carriage into memory from the landed
record, and a source-scanning writer census. Mechanism in `docs/ARCHITECTURE.md` under
"An exchange's LINEAGE is carried, where its nonce is deliberately re-minted".

**Unit 2, OPEN and BLOCKED on F-098**: the completion record and its entire lifecycle.
Carry-forward in `~/.claude/lanes/f-097/plan-item2.md`; the blocked plan, at revision 3, in
`~/.claude/lanes/f-097-unit2/plan.md`.

### Why unit 2 is blocked, and what its plan loop established

An attempt on 2026-09-10 ran **three plan-review rounds and three consults** and produced
no code. Rounds 1, 2 and 3 dismantled three successive proofs that a broker which does not
own an exchange may release its identifier, and the second escalation consult ruled that the
missing piece is a **primitive that does not exist at HEAD**. That primitive is now
**F-098**, which carries the three refuted proofs and their schedules; they are not to be
re-attempted here.

**What the loop settled, and what a later attempt may build on rather than re-derive:**

- The **representation and identity** stand: an immutable, create-only
  `$MQTT.inflight.<idHash>.<gen>.<PI>.done.<lin>` sibling with no `Claim`/`ClaimSeq`, keyed
  on unit 1's lineage. Five tokens past the prefix, so neither the register walk (`…<gen>.*`)
  nor the marker walk (`…<gen>.*.unsent`) enumerates it, and `$MQTT_inflight`'s
  `Subjects: ["$MQTT.inflight.>"]` with `MaxMsgsPer: 1` already admits it.
- The **latch**: evidence is owed when this owner's cleanup ends without a **first-attempt**
  confirmed deletion of this exchange's register, and owed-ness is never withdrawn by a later
  observation. A later absence or a different lineage at the subject proves current
  non-occupancy, not discharge of a peer's outstanding reservation.
- **`deleteInflightRegisterFor` cannot be read as "cleanup succeeded".** It returns `true`
  for three non-deleting outcomes (absent, another exchange's, moved), and underneath it
  `deleteInflightRegisterConfirmed` (`inflight_register.go:1405`) converts missing-sequence
  errors into success **and retries internally**, so its `nil` does not even mean the first
  attempt was proven.
- A **create-only rejection proves occupancy, not validity**: the occupant must be loaded
  and validated, and an unsupported one refused rather than counted as the obligation satisfied.
- Two **pre-existing defects** are prerequisites and must be labelled as such:
  `condemnSendLocked`'s unconditional first-wins precedence (`inflight_register.go:5532`),
  which hides whether a parked settle was a terminal acknowledgement; and `endSendWith`
  (`:5470-5518`), which untracks without draining deferred NAKs where the acknowledgement
  handler does (`mqtt.go:19270`).
- The evidence ordering is against the **terminal source disposition**, not `sendAck`
  alone — the failed-affine arm records its disposition inside `mqttFinishQ2AffineDelivery`
  (`qos2_shared_affine.go:3626`) before `releaseRegister`, and the drained arm reaches
  `finalizeDrainSource` (`unsub_drain.go:1801`) as a switch condition, bypassing
  `releaseRegister` entirely.
- The **teardown cannot reach a completion-only generation** as currently shaped:
  `reapIncarnationRegisters` returns early when its register enumeration is empty
  (`inflight_register.go:1182-1184`), so a completion pass appended after the marker pass
  never runs — which is exactly the state a crash after register deletion leaves.
- **`collectInflightCandidates` (`orphan_qos2_sweep.go:1621`) walks `$MQTT.inflight.>`** and
  would log every completion record as "an unattributable in-flight register" on every sweep
  tick. Reader recognition of the class must land with its producer.

### A defect this loop found at HEAD, with no completion record involved

`storeInflightRegister`'s different-source arm (`inflight_register.go:4127-4162`) meets a
register whose source is provably complete and **deletes it**, justified by "a provably
COMPLETE source means the exchange terminated, and identifier continuity protects only
incomplete exchanges". It **never examines the unsent marker**, and
`mqttSlotAdmitsDisposition` with `mqttSlotDeleteComplete` proves only that the slot is in
`publish` (`inflight_slot_state.go:615-618`) — nothing about the receiver.

Recovery states the opposite about the same record: `:7207-7216` reserves it because "the
receiver may hold this identifier", and `:337-343` says "Source disappearance is not receiver
completion". A read-only consult ruled **recovery correct and allocation the defect**, and
supplied the counterexample: a QoS 2 PUBLISH reaches the client, its PUBREC is lost, the
source expires by its own Message Expiry TTL while its consumer still holds interest (which
`inflightSourceState` classifies `Complete`), and a later allocation deletes the surviving
register and reuses the identifier while the client still holds the exchange.

**This is a source-supported counterexample and a required regression, not an observed
failing test** — no fixture for it exists yet. The two rules also interlock, which is why
it has been invisible: recovery's reservation is what stops `bumpPI` from ever offering that
identifier to the allocation that would have reaped it, so the reservation protects the
defect from being reached.

The repair belongs to F-097 unit 2 rather than to a separate finding — it is the same rule
stated at a third call site — and it is blocked with the rest of unit 2, because the
allocation site has the same unfenced scan-to-delete interval F-098 describes.

### A correction to this finding's own text

The claim above that **retained sends are a second collision population sharing `(PI,
"", 0)`** is true only of a design that keys evidence on the source triple. Retained
sends **do** mint an exchange nonce (`mqtt.go`, `trackPublishRetained`) — that part of
the original text was wrong in the other direction — but they have **no register and no
slot** (R6), so a producer restricted to **register-backed** terminal acknowledgements
never writes evidence for one and the population does not arise. The restriction is a
usable slot reference, **not** a non-empty nonce.

### What is known about the lifecycle, so it is not re-derived

- **The acknowledgement handler untracks memory BEFORE its durable release work**
  (`mqtt.go:19244`), which permits a new allocation and a send before the old
  acknowledgement's durable and source work finishes. A source-keyed completion match does
  not by itself suppress that offer.
- **`retainAndReconcileTerminal` (`pubrel_action.go:640`) cannot be imported unchanged.**
  It is a per-terminal-**reservation** retry, not a per-session reconciler: it requires a
  usable slot reference, installs or matches an empty `pendingPubRel` reservation, uses
  that entry's `terminalReconcile` bit, calls `mqttTerminalizeOnPubComp`, and releases that
  terminal reservation when the attempt settles; a live PUBREL entry with an
  acknowledgement subject is explicitly refused. A pending cancellation with no landed slot
  cannot enter it, and must not acquire a fictitious PUBREL reservation or invoke PUBCOMP
  terminalization. **Its scheduling and admission pattern may inform a design — its
  terminal-reservation semantics may not be reused.** Once admitted, its timer is genuinely
  independent of offers and Receive Maximum, which is the property a reconciler needs.
- **Generation scoping works only if every delayed writer and reconciliation task uses the
  CAPTURED generation**, and retirement handles late writes after cleanup.
- The register and marker recovery walks do **not** enumerate `.done.<nonce>` subjects
  today; a design adds that subject class and its retirement cleanup.

### The deliverable contract

Genuine durable completion evidence **survives replacement of the same exchange**, **never
releases a different exchange**, and **drives durable and memory release after a finite
storage failure without requiring another offer or a resume — including at exhausted
Receive Maximum.**

### Residual carried, narrowly

**R-B.** A crash before any durable consequence of the terminal acknowledgement leaves no
recoverable completion proof; no mechanism can do better, and a synchronous append narrows
the window without eliminating it. **R-B does not excuse losing landed evidence, and it
does not excuse omitting production retries.** Evidence retention, cleanup, and the
storage and latency costs of whichever design is chosen are **unresolved** and are not
pre-accepted here.

### Acceptance

Schedules are required for: same-exchange replacement; identifier reuse after a lost source
acknowledgement; retained sends; an **ambiguous** completion write (re-attempted, never
read back); ineffective register cleanup; recovery; and delayed work across generation
retirement. Each needs a **discriminating mutation**, a **route witness** for every negative
assertion, and a **positive control**, so that a permanently stalled or
refuse-everything implementation cannot pass.

### Prior art

Planning lane `~/.claude/lanes/replacing-allocation-lifecycle/` — `review-ledger.md`
(rounds 1-2 and the convergence verdict), `DECISIONS.md` (C1's joint-scope ruling and C2's
split), `plan.md` revision 2. **Revision 2 is NOT an approved starting design**; the next
attempt begins with a design consult on lineage versus suppression, before any plan review.

### The 2026-10-08 contract

After F-105 and F-106 were accepted, consult C1 of the joint F-097 / F-088 run (lane
`~/.claude/lanes/f-097-after-f105/`, `DECISIONS.md`) re-scoped this finding. F-105's
lineage-keyed exchange records are the identity; the `.done.<lin>` completion sibling and
the dependency on F-098 are retired; the register is the durable carrier, preserved. What
landed on that contract:

- **A late acknowledgement is bound before it arrives.** A transmitted entry removed
  unanswered installs an AckRoute binding in the same critical section, built from the
  entry (allocation, nonce, lineage, acquisition), so the client's PUBACK or PUBREC is
  routed to the existing completion owners without reading an occupant by identifier. An
  AckRoute is refused for a lineage the custody ledger has already seen end. A successful
  PUBREC through an AckRoute over a source a drain still claims records the drain's PUBREL
  intent first, as the live-entry path does; T29 found the PUBREL withheld for ever and
  the drain left claimed without it.
- **An autonomous custody dispatcher** replaces the hint-only observer: a failed exchange
  read is retried on its own cadence and, while a withholding gate's demand stands, a
  peer's later completion is rediscovered without traffic, from bounded units on a
  tracked timer.
- **R-B stays narrow:** a crash between receiving an acknowledgement and its durable
  advance leaves nothing to complete from.

- **Admission continuity, witness first.** W5a (a definite failure) found no defect. W5b
  REACHED one: an admission that landed with every readback failing was abandoned with
  its source mapping, and the re-offer was admitted again under another identifier, two
  un-ended exchanges for one source delivery. A continuity record now withholds that
  source until the custody dispatcher decides the first admission; for the witnessed
  population (landed, never sent) it is withdrawn and the source re-offered once. The
  rows no witness reached were not built, and the refusal stands there.
- **D4, witness first.** W6b's ack-during-write order REACHED it: a drain-owned deny
  parked first kept the send section's end, and the client's already-consumed PUBACK
  completed nothing. The client's answer now replaces a parked non-answer in
  `condemnSendLocked`, never the reverse. W6a (a recovery drop over a live entry) was not
  run.

The box stays open until the holistic review of the whole change has run. Mechanism in
`docs/ARCHITECTURE.md` ("A late acknowledgement is bound before it arrives", "An admission
that may have landed keeps its source"); the full status, matrix and witness list are in
the ROADMAP bullet.

## F-119: Cancellation must survive pending-fence disposal

**Title:** A cancellation whose pending fence is disposed of can be lost, and a delivery
cancelled before its admission landed can later be served

- Status: **Closed 2026-10-12 for the supported classes; affine remainder in chapter P.**
  Allocated 2026-10-08 by consult C6 of the joint F-097 / F-088 run, which split it off
  rather than close it there. An ordinary pending fence of the serving generation is now
  deleted only behind a confirmed, entitlement-fenced `fence-pending-disposal` authority
  advance and only once no admission ladder of its nonce is running
  (`pending_fence_disposal.go`); see the ROADMAP bullet's "What landed".
- Classification: delivery correctness (cancellation durability). Not an identifier or
  custody defect: the late landing frees no identifier, destroys no custody or completion
  route and admits no conflicting delivery, so F-119 does not reopen F-088's custody seam.
- Release treatment: Alpha blocker.

**The ROADMAP bullet is the authoritative home** (section 0.3, right after F-088): it
carries the HEAD schedule, both populations, the HEAD defect P-14#1, the candidate design
and the six review findings against it, the amended acceptance and what landed. The
remaining affine behaviour is stated as a residual in `docs/CONFORMANCE.md` ("A cancelled
experimental affine delivery can still be served after its pending fence is disposed of").
The real-server witnesses are `internal/mqtt/f119_fence_disposal_test.go` and the
converted row 17 of `TestF088PendingFenceDisposalPreservationMatrix`, which before F-119
injected the late landing with a fresh authority read (`landAdmission`) and so recorded it
restored ACTING without ever witnessing a previously prepared batch. Prior analysis: lane
`~/.claude/lanes/f-097-after-f105/` (`DECISIONS.md` C5 and C6, `review-ledger.md` P-12 to
P-15, `witnesses.md`).
