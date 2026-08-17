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
across elections. The mechanisms are stream-independent, so F-032 through F-034 inherit
the server contract and the canary — but each owns a consequence of its own, and none
closes on this evidence alone.

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
  stream-independent, but F-032 through F-034 each own a consequence of their own and
  stay open until each is audited on its own terms.
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

- Status: Open. The shared cross-term premise is closed
  (see the note at the top of this file and F-030); what remains for this finding is its
  OWN consequence audit against that
  route inventory, plus a fixture and a discriminating mutation for anything the
  audit finds still reachable.
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 32

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
