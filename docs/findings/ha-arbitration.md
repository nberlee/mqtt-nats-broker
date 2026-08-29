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
`readd` closure on `rollbackSubscribePacket`'s save-failure path restores the packet's
`cons` entries without restoring their `sharedSubs` membership. The main rollback
snapshot restores both maps correctly. No JetStream proposal fence can reconstruct the
missing entry in the `readd` arm.

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

**1. Claim transitions (ten, all inheriting W2).** All ten CAS at a sequence loaded in
the same attempt and preserve proven facts through `mergeProven`, so a transition can
only ever ADD to what is known. Seven carry an explicit `wantWinner`/`wantGen` identity
assertion and mutate nothing on a mismatch: `qos2_shared_affine.go:1727` (materialize),
`:1809` (the on-behalf discard arm), `:2156` (`discardQ2AffClaimsFor`), `:2237` (`Lost`),
`:2251` (recovery's `Discarded + SourceAcked`), `orphan_qos2_sweep.go:1871`
(`discardAbsentWinnerClaim`) and `:1975` (the sweep's pre-delete discard). Three pass no
identity because the fact they record is owner-independent — `SourceAcked` is a statement
about the group delivery and `AffineDone` about a copy the gate has already attributed:
`qos2_shared_affine.go:1367` (the reaper's unofferable-source inference), `:1423`
(`confirmSourceAckAndRecord`) and `:2036` (`mqttFinishQ2AffineDelivery`). Of those three
only the two `SourceAcked` writes rest on the per-subject CAS alone; F-065 fenced
`AffineDone` on the claim's INCARNATION and on the COPY the delivery carried, so an
owner-independent fact about one incarnation's copy cannot be written onto another's
record. None is
unconditional and none takes a stale base. The two sweep transitions are the ones whose
EVIDENCE is weaker than a CAS can express; that is F-064, not a lineage gap.

**2. Claim reaps (twelve, all inheriting D1).** Every one re-reads the claim and
re-derives both proofs before deleting, and deletes only the exact sequence it loaded:
`qos2_shared_affine.go:1574`, `:1593`, `:1655`, `:1816`, `:1855`, `:2047`, `:2163`,
`:2215`, `:2258`, `orphan_qos2_sweep.go:1796`, `:1879`, `:2068`. The re-read is what
makes reaping from every proof-writer correct rather than redundant: the two proofs are
produced by unordered paths, so whichever commits second observes the first. All twelve
inherit the same internal TOCTOU on the copy-absence reading, which is **F-065** — one
defect in the primitive, not twelve.

**3. Affine consumer deletes (four).** `orphan_qos2_sweep.go:2058` (the sweep, the only
one running under no session claim, and a check-then-act whose delete is BY NAME),
`qos2_shared_affine.go:1137` (provisioning rollback, `minted` only — nothing durable ever
referenced that generation), `:2474` (`finishQ2AffineUnwind`, under the session's own
fence) and `sess_teardown.go:750` (teardown, under a won `claimTeardown`, and after
`discardQ2AffClaimsFor` has marked every assignment of the generation). Only the sweep's
acts on evidence weaker than its consequence; that is F-036's third manifestation below.
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

**A fourth consequence sorts onto F-036, and that sort was CORRECTED during review.** The
sweep's affine consumer delete is by name, and a session resuming inside its final window
ADOPTS the consumer rather than recreating it — provisioning is a create-or-UPDATE — so
step (5)'s `Created` fingerprint does not discriminate it. The audit first filed this as a
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

**Also benign, and stated so rather than filed.** The sweep's consumer delete committing
while its response is lost skips that pass's reaps (`orphan_qos2_sweep.go:2058-2070`), but
the next pass independently revisits orphan `Discarded` claims and calls the reaper
(`:1783-1793`). The end state is a bounded delay in reaping an otherwise-reapable subset,
not durable accumulation.

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

- Status: Open — NARROWED, not resolved, and **ATTEMPTED BY CONSTRUCTION AND STOPPED
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
  no-watermark arm, because `$MQTT_teardown` recreated alone admits EMPTY and loses every
  open fence, so absence is not proof; and the different-generation arm, where proceeding
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

### A third manifestation added by F-033's audit (2026-08-18): the adoption blind spot

`deleteOrphanQ2AffConsumers` deletes an orphaned affine consumer **by name**, after two
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
`TestQ2AffResumeAdoptsConsumerKeepingItsCreated` pins the adoption mechanism.

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

**Scope: one stream incarnation.** Deleting and recreating `$MQTT_qos2in` restarts the
sequence space, and *every* sequence identity in this protocol breaks with it — the
per-subject CAS expectations, scope rule 2's exact-sequence claim deletes,
`releaseQoS2Record`, the sweep's `qos2ClaimUnchanged`, and now this fence. That
precondition is pre-existing and repository-wide, not introduced here; recreation is an
all-brokers-stopped operation under `docs/ARCHITECTURE.md`'s compatibility boundary. The
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

- Status: **Partly remedied, 2026-08-20. Still open.** Found by F-033's consequence audit,
  2026-08-18.
- Classification: HA arbitration architecture / liveness
- Release treatment: **Alpha blocker** for the stall. The accumulation is a resource leak
  and is stated here as a residual of the same premise rather than filed separately.
- Roadmap section: §0.2 (multi-frontend HA blockers)

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
referenced generation (`:1765`); `deleteOrphanQ2AffConsumers` skips the generation for
the same reason. Under a live, matching record the sweep therefore touches **no claim of
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
first construction a later successful orphan pass can still delete the consumer, so the
strand is bounded by that pass rather than permanent.

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
- **Disposing any copy whose claim is absent, at the gate.** A missing claim record is
  producible by a supported operation (F-066's stream recreation), so an absence never
  licenses destroying an accepted message.
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

- Status: **Open.** Found by F-033's consequence audit, 2026-08-18, during holistic review.
- Classification: HA arbitration architecture / data loss
- Release treatment: **Alpha blocker.** Every precondition is a recovery procedure this
  broker's own startup error tells the operator to perform, and the messages lost or
  misdelivered were accepted.
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
rounds, and why the remedy is a **generalisation** rather than a fourth bespoke coupling.

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
sequence only when the epochs are equal. Either shape works. What the affine path needs is
that treatment extended to the four-stream unit above and carried into **every** artifact
derived from a source sequence — the claim subject, the claim hash and hence the copy
subject, the copy's `Nats-Msg-Id`, the claim's own `SourceSeq`, the sweep's derived-name
validator, and `$MQTT_inflight`'s `Dur`/`Seq`/`Q2AffClaim`.

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

**What must be decided before a further attempt**, and it is not a design question:

1. Accept hand-maintained issuer-origin and sink seeds with strong drift guards and an explicit
   residual — under the current definition of done, F-066 then stays open;
2. Redefine what closing F-066 permits, including both seeded coverage and an explicit
   serialization exception to the access boundary, and require a fresh approved plan;
3. Leave it blocked under the current bar pending a genuinely different construction;
4. **Change the supported recovery contract** so the hazard is eliminated rather than
   generalised — make the affected streams one mandatory recreation unit, or withdraw
   independent single-stream recreation. That is a product and operational-contract decision.

Splitting the finding or shipping a partial mechanism is not among them.

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
  `classifyPreConnackSave` already uses for the two kinds it does distinguish.

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

- Status: **Open. Alpha blocker, unconditional.** Filed 2026-08-29 by the F-072
  plan-review loop (eleven Codex rounds; the analysis lives in
  `~/.claude/lanes/f-072/`, `review-ledger.md` and `plan.md` revision 11).
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

It does not own the DUP flag (F-072, blocked behind this), the transmitted-cancellation
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
  second exchange can bind the identifier).
- The takeover disposition must include a test that drives a real AckWait re-offer and
  proves the source never takes a second identifier, plus settlement tests for the four
  contest lifecycle states named above.
- Mutations: re-propose the stale body at each rebuild site; drop the reclassification;
  apply a sequence on a nonce match alone. Each must fail its paired test
  (`feedback_mutation_checks_must_discriminate`).

## F-088: A transmitted delivery's cancellation must not free its packet identifier

**Title:** `Cancelled` over a transmitted delivery lets recovery free an identifier the
client may still hold

- Status: **Open. Alpha blocker, unconditional. Ordered after F-087** — persisting this
  state safely depends on settled rewrite rules.
- Classification: MQTT conformance — packet-identifier lifecycle ([MQTT-2.3.1-4])
- Release treatment: Alpha blocker. Identifier reuse while the client holds the
  identifier can pair an acknowledgement with the wrong message.

### The finding

`Cancelled` is written by two functions reached from three situations, and only one of
them proves no transmission happened:

1. **Pre-send withdrawal** — `markInflightRegisterCancelledPending`
   (`inflight_register.go:2470`), and `confirmInflightRegister`'s ladder for an
   allocation cancelled while `regPending`. The store was still in flight; nothing was
   ever sent. The tombstone's "never transmitted" claim is true here.
2. **Abandonment of a delivery whose register store completed** — `purgeConsumerPendingLocked`
   (`:2292`) collects **non-`regPending`** entries too. A cleared `regPending` proves the
   REGISTER STORE completed, not that anything was transmitted — the confirm→send interval
   means some of these were never sent, a distinction the F-072 lane established and its
   marker design encodes. The population therefore includes genuinely transmitted
   deliveries, for which the tombstone asserts a falsehood, and the
   cleanup ladder that consumes them, `voidRegistersOnStream` (`:1963-1985`), falls back
   to `markInflightRegisterCancelled` when their delete cannot be proven. The tombstone
   then asserts a falsehood.
3. **A cancellation racing a transmission-state write** — prospective, not current: once
   F-072's marker exists, a cancellation can land between the send and the marker delete,
   and the tombstone dominates. Named now so the lifecycle is designed with it in view.

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
