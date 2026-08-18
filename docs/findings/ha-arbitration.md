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
none closes on this evidence alone. **F-033 and F-034 still owe their audits.** F-032's ran
on 2026-08-17: it inherited the contract without re-proving it, needed no lineage, and
found a local defect that JetStream's guarantees never spoke to — **F-062**, filed below
and open.

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
It gains one required field, `rwid`, naming the RECORD its deletion removes — the record
the teardown claimed, never the marker that claimed it — and a fourth reader, the
record-absent create, which inherits it as the parent of its first write immediately
after the proof that the subject is empty. So a create becomes a child of the record it
replaces: two creates in one interval are siblings of each other, a create in a later
interval is a descendant, and ordering survives deletions with no new comparison
anywhere. `classifyRecordLineage` is untouched; the whole change to the seven consults
is where a parent token comes from.

**That fourth reader no longer exists.** The write-lineage audit deleted it, along with
the other three sources; `rwid` stays on the watermark with no reader, and a create is
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
What survives on the record is `rwid`, the only durable statement of *"this specific
record was retired"* — now with **no reader at all**, since the create's valid arm no
longer consults it and the register that was once offered as its replacement does not
exist. It is retained because F-036's remaining shape plausibly needs exactly that
predicate: a bound, silent Keep Alive 0 client whose record another broker deleted needs
a disposal that does not wait for client traffic, and record-absence is ambiguous where
`ackWID == rwid` is positive proof. **If neither F-036 nor "give a vanished record its own
disposal vocabulary" needs it, whoever closes the later of the two should CONSIDER
removing `rwid`.** Both roadmap bullets carry the note; the consideration is what is
prescribed, not the removal.

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
  the 2.14.5 floor). Two mutations discriminate it: removing the comparison, and
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
  open until it is audited on its own terms. **F-033 and F-034 are still open on those
  terms.** F-032's audit has since run and closed, producing F-062, which is open.
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
batches. At the pinned v2.14.5 no Raft-level field preserves "these entries came from one
`ProposeMulti`" — entries are pushed individually, popped as whatever prefix exists,
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
purge, which is F-062.

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
one. Its two caller classes are arbitrated differently, and neither arbitration makes it
exchange-bound. That is **F-062** (the teardown caller) and **F-036** (the orphan sweep
caller, plus the post-enumeration gap).

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

- Status: Open. The shared cross-term premise is closed
  (see the note at the top of this file and F-030); what remains for this finding is its
  OWN consequence audit against that
  route inventory, plus a fixture and a discriminating mutation for anything the
  audit finds still reachable.
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 33

### Original finding record

- [ ] `$MQTT_qos2assign` carries no write lineage. An affine claim erased by a sibling
  leaves contenders waiting on a `claimed` record whose owner never materialises, which
  stalls the shared-group delivery of that message indefinitely — the same stall the
  generation-scoped naming was introduced to make impossible.

## F-034

**Title:** Add write lineage to outbound in-flight registers

- Status: Open. The shared cross-term premise is closed
  (see the note at the top of this file and F-030); what remains for this finding is its
  OWN consequence audit against that
  route inventory, plus a fixture and a discriminating mutation for anything the
  audit finds still reachable.
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 34

### Original finding record

- [ ] `$MQTT_inflight` carries no write lineage. A register erased by a sibling frees a
  packet identifier that is still in flight (so a later delivery can collide with it) or,
  in the other direction, leaves a register whose deletion was proven, reserving an
  identifier and a Receive Maximum slot for the life of the session.

## F-036

**Title:** Revalidate ownership for otherwise quiet resident sessions

- Status: Open — NARROWED, not resolved. Slice A landed on 2026-08-11 (the
  session-record watch plus its committed-read backstop,
  `internal/mqtt/sess_record_watch.go`; `docs/ARCHITECTURE.md`, "The session-record
  watch"): ownership changes only by a record write, so the watch observes every one
  of them, a quiet session still costs no write per interval, and every MOVED record
  now converges — bound sessions included, through the eviction worker itself. What
  remains is the DELETED record on a BOUND session: the backstop arms the ownership
  marker, and the record-rejection settlement that answers it is packet-driven for a
  bound connection, so a Keep Alive 0 client that goes silent still holds a session
  another broker tore down. That is this finding's own failure shape, so the finding
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
  subject the snapshot DID contain is reached by name and destroyed by the pending purge —
  F-062 mechanism 1, not this. The boundary is which side of the snapshot the SUBJECT
  falls on, not when the hold was written.) If the peer has not yet observed the ownership
  marker, it wins an ordinary `releasing` claim, passes its authoritative re-read and
  **delivers onward after the session was discarded**. Note the direction: this is a
  delivery that should not have happened, not a loss. A purge cutoff (F-062's mechanism-1
  candidate) does not address it, and must not be described as though it did: a cutoff at
  `Sequence: E+1` excludes never-enumerated holds and bounds purging to sequences at or
  below `E`, but it does **not** make teardown own exactly the enumerated set — `LastSeq`
  and `Subjects` are not one snapshot, so it may also spare a subject that WAS enumerated
  whose record landed above `E` (see F-062's stated limit). So this manifestation needs
  its own remedy, and a fix here must account for an enumerated-but-unfenced hold that is
  still serviceable after the teardown, not only for a never-enumerated one.
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

## F-062

**Title:** Teardown reuse hazards on an inbound QoS 2 hold subject after ownership moved

- Status: **Open.** Found by F-032's consequence audit, 2026-08-17.
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
is reached from four places, and mechanism 1 is a property of the SHARED purge rather
than of any one caller:

| Arm | Route | What A holds when it purges |
|---|---|---|
| A1 | `discardQoS2Hold` wins a fresh `discarded` claim (`qos2_release_claim.go:689`) | the teardown claim, and a release claim for an occupancy that had already ended — the subject was empty when it created the marker |
| A2 | `discardQoS2Hold` observes an existing `discarded` marker, its own from an earlier attempt or a peer's (`:718`) | the teardown claim, and a marker that may predate the current occupant entirely |
| A3 | `awaitQoS2Release` observes the claim subject EMPTY (`:750`) | the teardown claim, and **no release claim at all** — it LOST that one to E1, which is why it was awaiting |
| A4 | the orphan sweep (`orphan_qos2_sweep.go:1393`) | no teardown claim either; fenced by the reference walk instead — that arm is **F-036**, not this |

**Distinguish the two claims, because A3 is where conflating them misleads.** The
**session teardown claim** is what licenses the walk; the per-subject **release claim** is
what arbitrates one exchange. In A3, A holds the first and never acquires the second, so
E2 can win its own `releasing` claim and pass `verifyQoS2HoldForRelease` — returning
`Held` — and that newly won claim cannot cancel A's already-committed purge. In A1 and A2,
A does hold a marker, but one written for an occupancy that is over. **In none of the three
is the purge fenced against the subject's CURRENT occupant.**

**The cutoff closes A1 and A3, and NOT A2.** For A1 and A3 the marker or the awaited claim
belongs to *this* walk, so any hold accepted after this walk's enumeration sits above `E`
and is spared — which is why the cutoff belongs **inside `purgeQoS2Hold`**, at the shared
boundary, rather than in one caller.

A2 defeats it, and this is the arm most likely to be missed by a remedy that looks
complete. The marker there is from an **earlier, partial teardown**, so the fence it
represents is older than the current walk's watermark. Interleaving: an earlier walk leaves
a `discarded` marker without completing the purge; B then accepts E2 and answers PUBREC;
a repeated walk enumerates, and **its `LastSeq` now includes E2**, so `E` is above E2's
sequence; `discardQoS2Hold` observes the pre-existing marker and purges with `Sequence:
E+1`, which deletes E2. The current watermark is simply the wrong fence for a marker that
predates it.

So A2 needs the marker's **own** fence — the value the marker itself records — which is the
same evidence mechanism 2's remedy has to supply. **A1/A3 and A2/mechanism 2 therefore share
two remedies, not one**, and a fix that implements only the cutoff still loses acknowledged
payloads through A2. The fixture must fail if only the cutoff is implemented.

**Mechanism 2 — a persistent `discarded` claim poisons a later exchange.** A creates the
`discarded` marker and the teardown proceeds. B's PUBREL for E1 observes it, releases E1's
hold and answers 0x92. B's client then reuses the identifier: E2's create-only hold succeeds
on the emptied subject and PUBREC is returned. E2's PUBREL reads the **same, still present**
marker and is answered 0x92 with E2's hold released. **Direction: loss**, established by
exhausting every arm that reads a claim state — the two delivering arms require
`State == releasing`, which a `discarded` marker never satisfies; the repeated-teardown arm
purges the hold; and nothing reaps a `discarded` marker except the aged orphan sweep,
hold-first. Successful cleanup does send PUBCOMP (0x92 on MQTT 5) and frees the identifier,
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

### Fixtures and discriminating mutations

- **`TestQoS2TeardownPurgeSparesAPostEnumerationHold`** — mechanism 1, and it must
  discriminate **every arm in the table above, not one branch**, because the arms do not
  share one remedy. Structure it at the shared boundary — assert `purgeQoS2Hold` never
  removes a record the teardown was not entitled to — with one subtest per arm:
  1. *A1, fresh `discarded` win.* E2 accepted between this walk's enumeration and the
     purge. Closed by the cutoff: E2 sits above this walk's `E`.
  2. *A3, `releasing` claim disappears.* Same staging through `awaitQoS2Release`.
     Closed by the cutoff for the same reason.
  3. *A2, marker from an EARLIER partial teardown.* Stage it in that order: earlier walk
     leaves the marker without purging, **then** E2 is accepted and answered PUBREC,
     **then** a repeated walk enumerates — so E2 is BELOW this walk's watermark. **This
     subtest must fail against a cutoff-only implementation**, since the cutoff cannot
     fence a marker older than the watermark; it passes only once the marker carries its
     own fence (mechanism 2's remedy).

  Assert in every subtest that E2's hold survives and its PUBREL completes.
  **Mutations, one per remedy so neither can be dropped:** remove the purge's `Sequence`
  bound — A1 and A3 must fail; and revert the marker-borne fence — A2 must fail. A
  remedy implementing only the cutoff leaves A2 red, which is the point of splitting
  them.
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
  staging only ordering 1 would bless a broken remedy. **Mutation:** revert the stale-claim
  handling; must fail on **both** subtests, so a partial remedy cannot score the kill.

### Remedy candidates

**Mechanism 1 — a purge cutoff (leading).** `JSApiStreamPurgeRequest.Sequence` is a
subject-combinable cutoff excluding the cutoff sequence, and `StreamState.LastSeq` is already
in the `STREAM.INFO` response `streamSubjects` currently discards. Capture `LastSeq = E` at
enumeration and purge with `Sequence: E+1`. No payload read, no extra round trip, and no
per-hold sequence — which is what makes it viable, since the arbitration has no cheap
per-hold sequence source (`STREAM.INFO`'s subject map carries per-subject *counts*).
`MaxMsgsPer: 1` replacements receive a later stream sequence, so a replacement is above the
cutoff.

**Its stated limit:** `LastSeq` and `Subjects` are **not one snapshot** — they are taken under
independent store locks — so a hold may appear in the enumerated map yet sit above the cutoff.
The claim is "the purge is bounded to sequences at or below `E`", **not** "the teardown owns
exactly what it enumerated". Direction of that gap: such a hold **survives** and is left to
the orphan sweep; it is never destroyed.

**Mechanism 2 — open, with a constraint that rules out the naive fix.** The remedy must not
**unconditionally** reap the marker: its persistence is load-bearing, because it is the only
durable proof that a late releaser of the *retired* incarnation must lose. A **conditional
exact-sequence** reap is safe once there is durable evidence the marker does not fence the
loaded hold. Two directions, qualified:

- **Scoping the claim subject by exchange generation** is not directly viable under the
  current layout: the releaser knows the exchange generation but the **payload-free discard
  enumeration does not**. Making it viable needs a changed hold-subject layout, an
  index, or a metadata lookup — none of which may reintroduce a payload read on the
  discard path.
- **Recording the fence in the marker** is viable provided the recorded value is the
  enumeration cutoff `E` or an equivalent durable fence identity, **not** an exact per-hold
  sequence, which is unavailable. A later exchange can then prove its hold is above the fence,
  exact-delete the stale marker, and retry.

Existing claim-versus-hold stream ordering identifies only marker-before-hold cases, so it
cannot resolve ordering 2 on its own.
